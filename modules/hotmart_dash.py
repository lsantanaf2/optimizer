"""
Dashboard de Faturamento por Funil — vendas Hotmart via Google Sheets.

Fonte: planilha alimentada por webhook Hotmart (aba 'VENDAS HOTMART').
Leitura primária via Service Account (mesma credencial das planilhas de MQLs);
fallback para o export CSV público enquanto a planilha não for compartilhada
com o SA (sklucas@dash-teste-458004.iam.gserviceaccount.com).

Rotas:
  GET /dash/faturamento              — página (aberta, sem token — decisão do produto)
  GET /api/dash/faturamento/data     — JSON agregado (?since=&until=&refresh=1)

Regras de negócio (ver docstring de _aggregate):
  - 5 produtos TOPO FUNIL definem os funis (EMA em 4 idiomas + SSM)
  - Bumps/upsells herdam o funil da transação mãe (coluna E, cadeia recursiva)
  - Sem mãe/DE-PARA → bucket "Sem atribuição" (exibido para auditoria)
  - Valores SEMPRE da coluna 'Comissão BRL'
  - Bruto = APPROVED | Reembolsos = REFUNDED+CHARGEBACK | Líquido = diferença
"""

import csv
import io
import logging
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Blueprint, jsonify, render_template, request

logger = logging.getLogger(__name__)
_BR_TZ = ZoneInfo('America/Sao_Paulo')

hotmart_dash_bp = Blueprint('hotmart_dash', __name__)

SHEET_ID = '1X4vizNmoOCPDIrB8Gle9xWkj53Ep_igIRy5lPz6mihM'
SHEET_TAB = 'VENDAS HOTMART'
CACHE_TTL = 600  # 10 min

# DE-PARA: produto TOPO FUNIL → funil (confirmado com o cliente em 28/07/2026)
FUNIS = {
    '2301254': 'EMA-PT',   # Emissões Avançadas
    '5096685': 'EMA-ES',   # Emisiones Avanzadas
    '7084722': 'EMA-EN',   # EMA Assistant
    '7527383': 'EMA-FR',   # EMA Assistant. (com ponto)
    '8126548': 'SSM',      # Segundo Salário com Milhas
}
FUNIL_ORDER = ['EMA-PT', 'EMA-ES', 'EMA-EN', 'EMA-FR', 'SSM']
EMA_KEYS = ['EMA-PT', 'EMA-ES', 'EMA-EN', 'EMA-FR']
TIPOS = ['TOPO FUNIL', 'ORDER BUMP', 'UPSELL']


# ── Leitura da planilha ───────────────────────────────────────────────────────

def _rows_from_values(values):
    """Converte a matriz da Sheets API em lista de dicts pelo cabeçalho."""
    if not values:
        return []
    headers = [h.strip() for h in values[0]]
    rows = []
    for row in values[1:]:
        padded = row + [''] * (len(headers) - len(row))
        rows.append(dict(zip(headers, padded)))
    return rows


def _fetch_via_service_account():
    """Leitura primária: Sheets API v4 com o Service Account existente.
    Ignora a coluna S (Payload) — só A:R interessa e economiza banda."""
    from modules.cruzamento import _get_google_token
    token = _get_google_token()
    url = (f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}"
           f"/values/'{SHEET_TAB}'!A:R")
    resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=30)
    resp.raise_for_status()
    return _rows_from_values(resp.json().get('values', []))


def _fetch_via_public_csv():
    """Fallback: export CSV público (funciona enquanto o link estiver aberto)."""
    url = (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
           f"/gviz/tq?tqx=out:csv&sheet={SHEET_TAB.replace(' ', '%20')}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    if resp.text.lstrip().startswith('<'):
        raise RuntimeError('Planilha não é pública (retornou HTML de login)')
    return list(csv.DictReader(io.StringIO(resp.text)))


def _fetch_sheet_rows():
    """SA primeiro; fallback CSV público. Levanta com mensagem amigável se ambos falham."""
    try:
        return _fetch_via_service_account()
    except Exception as e:
        logger.warning(f'[faturamento] Service Account falhou ({e}) — tentando CSV público')
    try:
        return _fetch_via_public_csv()
    except Exception as e:
        raise RuntimeError(
            'Não foi possível ler a planilha de vendas. Verifique se ela está '
            'compartilhada com o Service Account ou se o link público está ativo. '
            f'Detalhe: {e}'
        )


# ── Parsing ───────────────────────────────────────────────────────────────────

def _parse_brl(s):
    """'1.234,56' ou '126,43' → float. Vazio/inválido → 0.0."""
    if not s:
        return 0.0
    s = str(s).strip()
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_date(s):
    """'17/07/2026 20:15:22' → date. Inválido → None."""
    try:
        return datetime.strptime(str(s).strip()[:10], '%d/%m/%Y').date()
    except (ValueError, TypeError):
        return None


# ── Agregação ─────────────────────────────────────────────────────────────────

def _new_bucket():
    return {
        'bruto': 0.0, 'reembolsos': 0.0,
        'por_tipo': {t: {'valor': 0.0, 'qtd': 0} for t in TIPOS},
        'qtd_vendas': 0, 'qtd_reembolsos': 0,
    }


def _aggregate(rows, since_d=None, until_d=None):
    """Agrega vendas por funil no período.

    IMPORTANTE: a resolução de âncoras (transação topo → funil) e a cadeia de
    parents usam a planilha INTEIRA, não só o período — um upsell de hoje pode
    ter a venda mãe do mês passado. Só o SOMATÓRIO respeita o filtro de data.
    """
    # Passada 1 (planilha inteira): âncoras, cadeia de parents e datas das vendas
    anchors = {}      # transaction → funil
    parents = {}      # transaction → parent transaction
    approved_date = {}  # transaction → date da venda APPROVED (p/ datar reembolsos)
    for r in rows:
        tx = (r.get('Transaction') or '').strip()
        if not tx:
            continue
        pid = (r.get('Product ID') or '').strip()
        if pid in FUNIS:
            anchors[tx] = FUNIS[pid]
        par = (r.get('Parent Transaction') or '').strip()
        if par:
            parents[tx] = par
        if (r.get('Status') or '').strip().upper() == 'APPROVED':
            d = _parse_date(r.get('Recebido em'))
            if d:
                approved_date[tx] = d

    def resolve_funil(tx):
        """Segue a cadeia de parents até uma âncora (máx 6 saltos, anti-ciclo)."""
        seen = set()
        cur = tx
        for _ in range(6):
            if cur in anchors:
                return anchors[cur]
            seen.add(cur)
            cur = parents.get(cur)
            if not cur or cur in seen:
                return None
        return None

    # Passada 2 (com filtro de data): somatório
    funis = {k: _new_bucket() for k in FUNIL_ORDER}
    sem_atrib = _new_bucket()
    sem_atrib_produtos = {}  # (pid, nome) → {'bruto', 'reembolsos', 'qtd'}
    reembolsos_sem_data_fora = 0  # excluídos do período por não terem data

    filtro_ativo = bool(since_d or until_d)

    for r in rows:
        status_raw = (r.get('Status') or '').strip().upper()
        d = _parse_date(r.get('Recebido em'))
        # Reembolsos chegam SEM data na coluna A (webhook da Hotmart) —
        # herdam a data da venda APPROVED original quando ela está na planilha.
        if d is None and status_raw in ('REFUNDED', 'CHARGEBACK'):
            d = approved_date.get((r.get('Transaction') or '').strip())
        if d is None:
            if status_raw in ('REFUNDED', 'CHARGEBACK'):
                if filtro_ativo:
                    # sem data possível → só entra na visão "Tudo"
                    reembolsos_sem_data_fora += 1
                    continue
                # visão "Tudo": inclui mesmo sem data (d fica None, sem filtro)
            else:
                continue  # linha não-reembolso sem data: ignora (malformada)
        if d is not None:
            if since_d and d < since_d:
                continue
            if until_d and d > until_d:
                continue

        valor = _parse_brl(r.get('Comissão BRL'))
        status = (r.get('Status') or '').strip().upper()
        tipo = (r.get('Tipo de Compra') or '').strip().upper()
        if tipo not in TIPOS:
            tipo = 'TOPO FUNIL'  # defensivo: tipo desconhecido trata como topo
        pid = (r.get('Product ID') or '').strip()
        tx = (r.get('Transaction') or '').strip()

        funil_key = FUNIS.get(pid) or resolve_funil(tx)
        bucket = funis[funil_key] if funil_key else sem_atrib

        if status == 'APPROVED':
            bucket['bruto'] += valor
            bucket['por_tipo'][tipo]['valor'] += valor
            bucket['por_tipo'][tipo]['qtd'] += 1
            bucket['qtd_vendas'] += 1
        elif status in ('REFUNDED', 'CHARGEBACK'):
            bucket['reembolsos'] += valor
            bucket['qtd_reembolsos'] += 1

        if not funil_key:
            key = (pid, (r.get('Produto') or '').strip())
            p = sem_atrib_produtos.setdefault(key, {'bruto': 0.0, 'reembolsos': 0.0, 'qtd': 0})
            if status == 'APPROVED':
                p['bruto'] += valor
                p['qtd'] += 1
            elif status in ('REFUNDED', 'CHARGEBACK'):
                p['reembolsos'] += valor

    def _finalize(b):
        out = {
            'bruto': round(b['bruto'], 2),
            'reembolsos': round(b['reembolsos'], 2),
            'liquido': round(b['bruto'] - b['reembolsos'], 2),
            'qtd_vendas': b['qtd_vendas'],
            'qtd_reembolsos': b['qtd_reembolsos'],
            'por_tipo': {t: {'valor': round(v['valor'], 2), 'qtd': v['qtd']}
                         for t, v in b['por_tipo'].items()},
        }
        return out

    # EMA Global = soma dos 4 idiomas
    ema = _new_bucket()
    for k in EMA_KEYS:
        b = funis[k]
        ema['bruto'] += b['bruto']
        ema['reembolsos'] += b['reembolsos']
        ema['qtd_vendas'] += b['qtd_vendas']
        ema['qtd_reembolsos'] += b['qtd_reembolsos']
        for t in TIPOS:
            ema['por_tipo'][t]['valor'] += b['por_tipo'][t]['valor']
            ema['por_tipo'][t]['qtd'] += b['por_tipo'][t]['qtd']

    # Totais gerais (funis + sem atribuição — bate com a planilha inteira)
    tot = _new_bucket()
    for b in list(funis.values()) + [sem_atrib]:
        tot['bruto'] += b['bruto']
        tot['reembolsos'] += b['reembolsos']
        tot['qtd_vendas'] += b['qtd_vendas']
        tot['qtd_reembolsos'] += b['qtd_reembolsos']

    return {
        'totals': _finalize(tot),
        'reembolsos_sem_data_fora': reembolsos_sem_data_fora,
        'ema_global': _finalize(ema),
        'funis': [{'key': k, **_finalize(funis[k])} for k in FUNIL_ORDER],
        'sem_atribuicao': {
            **_finalize(sem_atrib),
            'produtos': [
                {'product_id': pid, 'produto': nome,
                 'bruto': round(v['bruto'], 2), 'reembolsos': round(v['reembolsos'], 2),
                 'qtd': v['qtd']}
                for (pid, nome), v in sorted(sem_atrib_produtos.items(),
                                             key=lambda x: -x[1]['bruto'])
            ],
        },
    }


# ── Rotas ─────────────────────────────────────────────────────────────────────

@hotmart_dash_bp.route('/dash/faturamento')
def faturamento_page():
    from modules.rate_limiter import check_rate_limit
    check_rate_limit('faturamento-page')
    return render_template('dash_faturamento.html')


@hotmart_dash_bp.route('/api/dash/faturamento/data')
def faturamento_data():
    from modules.rate_limiter import check_rate_limit
    check_rate_limit('faturamento-api')

    from modules.meta_cache import get_or_fetch, invalidate

    if request.args.get('refresh') == '1':
        invalidate('hotmart_rows')

    try:
        rows = get_or_fetch(('hotmart_rows',), CACHE_TTL, _fetch_sheet_rows)
    except Exception as e:
        logger.error(f'[faturamento] leitura falhou: {e}')
        return jsonify({'success': False, 'error': str(e)}), 502

    since_d = until_d = None
    try:
        if request.args.get('since'):
            since_d = datetime.strptime(request.args['since'], '%Y-%m-%d').date()
        if request.args.get('until'):
            until_d = datetime.strptime(request.args['until'], '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'success': False, 'error': 'Datas inválidas (use YYYY-MM-DD)'}), 400

    result = _aggregate(rows, since_d, until_d)
    result['success'] = True
    result['meta'] = {
        'linhas_planilha': len(rows),
        'periodo': {'since': str(since_d) if since_d else None,
                    'until': str(until_d) if until_d else None},
        'gerado_em': datetime.now(_BR_TZ).isoformat(),
    }
    return jsonify(result)
