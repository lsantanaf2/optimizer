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
  - A baia é o IDIOMA do funil (PT/ES/EN/FR/IT/DE), não o produto
  - Quem define o funil é o produto de ENTRADA (ver PRODUTO_IDIOMA)
  - Bumps/upsells herdam o funil da transação mãe (coluna E, cadeia recursiva)
  - Sem mãe/DE-PARA → bucket "Sem atribuição" (exibido para auditoria)
  - Valores SEMPRE da coluna 'Comissão BRL'
  - Bruto = APPROVED | Reembolsos = REFUNDED+CHARGEBACK | Líquido = diferença
"""

import csv
import io
import json
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

# DE-PARA: produto → IDIOMA do funil.
#
# A baia é o idioma, não o produto: os funis trocaram de produto de entrada
# duas vezes (08-09/10/2026 e 20-23/10/2026) e o que interessa é o mercado.
# Desde 20/10 os produtos novos trazem a bandeira no nome, o que torna o
# idioma legível direto na planilha.
#
# Quem define o funil é o produto de ENTRADA (venda sem transação mãe). Todo
# o resto da cadeia — order bump, upsell, downsell — herda pela mãe. É por
# isso que ancorar sempre pelo ID estaria errado: o mesmo produto é vendido
# como upsell dentro de funis de outros idiomas, e ancorar levaria essa
# receita para a baia errada.
PRODUTO_IDIOMA = {
    # ── Português ──
    '2301254': 'PT',   # Emissões Avançadas (front até 08/10/2026)
    '4956764': 'PT',   # Alerta da Milhas Lucrativas → Secret Flight Deals (09/10)
    '5818264': 'PT',   # Assistente EMA
    '4839597': 'PT',   # Manual dos cartões lucrativos
    '6018059': 'PT',   # Alerta de Boletos Secretos
    '8662943': 'PT',   # Comunidade AML
    # ── Espanhol ──
    '5096685': 'ES',   # Emisiones Avanzadas
    '8772302': 'ES',   # SFD Radar 🇪🇸 (front desde 20/10/2026)
    '8772565': 'ES',   # Asistente IA 🇪🇸
    # ── Inglês ──
    '7084722': 'EN',   # EMA Assistant 🇺🇸
    '8773526': 'EN',   # SFD Radar 🇺🇸 (front desde 21/10/2026)
    '8773580': 'EN',   # AI Assistant 🇺🇸
    '7763423': 'EN',   # Secret Flight Deals → SFD Radar (08/10). Sem bandeira
                       # no nome e vendido como upsell em TODOS os idiomas —
                       # quando tem mãe, quem manda é ela; o idioma aqui só
                       # vale para as vendas de entrada (cliente, 26/10/2026).
    # ── Francês ──
    '7527383': 'FR',   # EMA Assistant 🇫🇷
    '8776426': 'FR',   # SFD Radar 🇫🇷 (front desde 21/10/2026)
    '8776434': 'FR',   # Assistant IA 🇫🇷
    # ── Italiano ──
    '8298713': 'IT',   # EMA Assistant 🇮🇹
    '8778600': 'IT',   # SFD Radar 🇮🇹 (front desde 22/10/2026)
    '8778629': 'IT',   # Assistente IA 🇮🇹
    # ── Alemão ──
    '8299099': 'DE',   # EMA Assistant 🇩🇪
    '8780971': 'DE',   # SFD Radar 🇩🇪 (front desde 23/10/2026)
    '8781014': 'DE',   # KI-Assistent 🇩🇪
    # ── Linha à parte, não é EMA ──
    '8126548': 'SSM',  # Segundo Salário com Milhas
    #
    # FORA DE PROPÓSITO:
    #   7086112  'Exclusive Flight Alerts'. Nome em inglês mas sem bandeira e
    #            sem funil identificável. Fica em "sem atribuição" até o
    #            cliente dizer de qual idioma é.
}
FUNIL_ORDER = ['PT', 'ES', 'EN', 'FR', 'IT', 'DE', 'SSM']
FUNIL_LABEL = {'PT': 'Português', 'ES': 'Espanhol', 'EN': 'Inglês',
               'FR': 'Francês', 'IT': 'Italiano', 'DE': 'Alemão',
               'SSM': 'Segundo Salário com Milhas'}
EMA_KEYS = ['PT', 'ES', 'EN', 'FR', 'IT', 'DE']
TIPOS = ['TOPO FUNIL', 'ORDER BUMP', 'UPSELL']


# ── Leitura da planilha ───────────────────────────────────────────────────────

def _rows_from_values(values):
    """Converte a matriz da Sheets API em lista de dicts pelo cabeçalho.

    Guarda o número da linha na planilha ('_linha') para poder buscar depois a
    coluna Payload só das linhas que interessarem — ela sozinha é 85% do
    volume e não vale baixar inteira.
    """
    if not values:
        return []
    headers = [h.strip() for h in values[0]]
    rows = []
    for i, row in enumerate(values[1:]):
        padded = row + [''] * (len(headers) - len(row))
        d = dict(zip(headers, padded))
        d['_linha'] = i + 2   # 1-based, pulando o cabeçalho
        rows.append(d)
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
    linhas = list(csv.DictReader(io.StringIO(resp.text)))
    for i, d in enumerate(linhas):
        d['_linha'] = i + 2
    return linhas


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


# ── Renovações de assinatura ──────────────────────────────────────────────────

MAX_LINHAS_PAYLOAD = 400   # teto de segurança para o batchGet


def _fetch_recorrencias(linhas):
    """Lê a coluna Payload SÓ das linhas indicadas e devolve {transaction: n}.

    `n` é o recurrence_number da Hotmart: 1 é a primeira cobrança da assinatura
    e acima disso é renovação. A coluna Payload tem ~2 KB por linha (85% da
    planilha), então ela é buscada por faixas individuais, nunca inteira.
    """
    if not linhas:
        return {}
    from modules.cruzamento import _get_google_token
    token = _get_google_token()

    out = {}
    linhas = linhas[:MAX_LINHAS_PAYLOAD]
    for bloco in range(0, len(linhas), 100):
        pedaco = linhas[bloco:bloco + 100]
        params = [('ranges', f"'{SHEET_TAB}'!S{n}") for n, _ in pedaco]
        resp = requests.get(
            f'https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values:batchGet',
            headers={'Authorization': f'Bearer {token}'}, params=params, timeout=30)
        resp.raise_for_status()
        for (n, tx), faixa in zip(pedaco, resp.json().get('valueRanges', [])):
            vals = faixa.get('values') or []
            if not vals or not vals[0]:
                continue
            try:
                p = json.loads(vals[0][0])
            except (ValueError, TypeError):
                continue
            rec = ((p.get('data') or {}).get('purchase') or {}).get('recurrence_number')
            if isinstance(rec, int):
                out[tx] = rec
    return out


def renovacoes_de(rows, resolve_funil):
    """Transações de RENOVAÇÃO entre as vendas que não caem em nenhum funil.

    Renovação não passa por funil: é cobrança automática de uma assinatura que
    pode ter nascido antes da planilha existir. Por isso ela chega sem venda
    mãe e sem rastreamento, e ficava no balde "sem atribuição".
    """
    alvo = []
    vistos = set()
    for r in rows:
        if (r.get('Status') or '').strip().upper() not in ('APPROVED', 'REFUNDED', 'CHARGEBACK'):
            continue
        tx = (r.get('Transaction') or '').strip()
        n = r.get('_linha')
        if not tx or not n or tx in vistos:
            continue
        if resolve_funil(tx):
            continue
        vistos.add(tx)
        alvo.append((n, tx))

    try:
        recs = _fetch_recorrencias(alvo)
    except Exception as e:
        # Sem o Payload seguimos como antes: tudo fica em "sem atribuição"
        logger.warning(f'[faturamento] recorrências não lidas ({e}) — sem card de renovações')
        return set()
    return {tx for tx, n in recs.items() if n > 1}


# ── Agregação ─────────────────────────────────────────────────────────────────

def _resolver(rows):
    """Passada 1 (planilha inteira): âncoras e cadeia de transações mãe.

    Devolve (resolve_funil, approved_date). Fica separado da agregação porque a
    detecção de renovações precisa saber, antes de somar, quais vendas não caem
    em funil nenhum.
    """
    anchors = {}      # transaction → funil
    parents = {}      # transaction → parent transaction
    approved_date = {}  # transaction → date da venda APPROVED (p/ datar reembolsos)
    candidatas_entrada = {}  # transaction → funil, se for venda de entrada (ver abaixo)
    aprovadas = set()  # transactions com linha de compra aprovada
    for r in rows:
        tx = (r.get('Transaction') or '').strip()
        if not tx:
            continue
        pid = (r.get('Product ID') or '').strip()
        par = (r.get('Parent Transaction') or '').strip()
        # Transação que aponta para si mesma como mãe é ruído do webhook
        if par and par != tx:
            parents[tx] = par
        idioma = PRODUTO_IDIOMA.get(pid)
        if idioma:
            candidatas_entrada[tx] = idioma
        if (r.get('Status') or '').strip().upper() == 'APPROVED':
            aprovadas.add(tx)
            d = _parse_date(r.get('Recebido em'))
            if d:
                approved_date[tx] = d

    # Produto de entrada só vira âncora depois de ver a planilha inteira, com
    # duas condições:
    #
    # 1. Sem mãe em NENHUMA linha. A mesma transação pode ter uma linha com mãe
    #    e outra sem — em HP3816891780 a linha de reembolso veio sem mãe e como
    #    TOPO FUNIL, e sozinha tiraria a venda do funil de EMA a que pertence.
    # 2. Com linha de compra aprovada. Há 20 reembolsos de SFD cuja venda não
    #    está na planilha: sem ela não dá para saber de que funil vieram, e
    #    chutar encheria o funil novo de reembolsos alheios. Seguem em "Sem
    #    atribuição", que existe para esse tipo de auditoria.
    for tx, funil in candidatas_entrada.items():
        if tx not in parents and tx not in anchors and tx in aprovadas:
            anchors[tx] = funil

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

    return resolve_funil, approved_date


def _new_bucket():
    return {
        'bruto': 0.0, 'reembolsos': 0.0,
        'por_tipo': {t: {'valor': 0.0, 'qtd': 0} for t in TIPOS},
        'qtd_vendas': 0, 'qtd_reembolsos': 0,
    }


def _aggregate(rows, since_d=None, until_d=None, renovacoes=None):
    """Agrega vendas por funil no período.

    IMPORTANTE: a resolução de âncoras (transação topo → funil) e a cadeia de
    parents usam a planilha INTEIRA, não só o período — um upsell de hoje pode
    ter a venda mãe do mês passado. Só o SOMATÓRIO respeita o filtro de data.

    `renovacoes`: transações de renovação de assinatura (ver renovacoes_de).
    Vão para um bucket próprio em vez de "sem atribuição" — não são venda nova
    de funil, são cobrança recorrente da base.
    """
    renovacoes = renovacoes or set()
    resolve_funil, approved_date = _resolver(rows)

    # Passada 2 (com filtro de data): somatório
    funis = {k: _new_bucket() for k in FUNIL_ORDER}
    sem_atrib = _new_bucket()
    renov = _new_bucket()
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

        # 1) O funil vem da venda de ENTRADA, seguindo a cadeia de mães: é
        #    assim que um upsell vendido dentro do funil de outro idioma fica
        #    no idioma certo. 2) Se a cadeia morre — acontece com upsell cuja
        #    venda mãe é anterior à planilha —, cai no idioma do próprio
        #    produto, que para os produtos com bandeira no nome é certeiro.
        funil_key = resolve_funil(tx) or PRODUTO_IDIOMA.get(pid)
        if funil_key:
            bucket = funis[funil_key]
        elif tx in renovacoes:
            bucket = renov
        else:
            bucket = sem_atrib

        if status == 'APPROVED':
            bucket['bruto'] += valor
            bucket['por_tipo'][tipo]['valor'] += valor
            bucket['por_tipo'][tipo]['qtd'] += 1
            bucket['qtd_vendas'] += 1
        elif status in ('REFUNDED', 'CHARGEBACK'):
            bucket['reembolsos'] += valor
            bucket['qtd_reembolsos'] += 1

        if not funil_key and bucket is sem_atrib:
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

    # EMA Global = soma de todos os idiomas (EMA_KEYS)
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

    # Totais gerais (funis + renovações + sem atribuição — bate com a planilha)
    tot = _new_bucket()
    for b in list(funis.values()) + [renov, sem_atrib]:
        tot['bruto'] += b['bruto']
        tot['reembolsos'] += b['reembolsos']
        tot['qtd_vendas'] += b['qtd_vendas']
        tot['qtd_reembolsos'] += b['qtd_reembolsos']

    return {
        'totals': _finalize(tot),
        'reembolsos_sem_data_fora': reembolsos_sem_data_fora,
        'ema_global': _finalize(ema),
        'renovacoes': _finalize(renov),
        'funis': [{'key': k, 'label': FUNIL_LABEL.get(k, k), **_finalize(funis[k])}
                  for k in FUNIL_ORDER],
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

    # Renovações dependem do Payload (coluna S), lido só das linhas sem funil.
    # Cacheado à parte: o conjunto não muda com o filtro de data.
    try:
        resolve_funil, _ = _resolver(rows)
        renovacoes = get_or_fetch(('hotmart_renovacoes', len(rows)), CACHE_TTL,
                                  lambda: renovacoes_de(rows, resolve_funil))
    except Exception as e:
        logger.warning(f'[faturamento] renovações indisponíveis: {e}')
        renovacoes = set()

    result = _aggregate(rows, since_d, until_d, renovacoes)
    result['success'] = True
    result['meta'] = {
        'linhas_planilha': len(rows),
        'periodo': {'since': str(since_d) if since_d else None,
                    'until': str(until_d) if until_d else None},
        'gerado_em': datetime.now(_BR_TZ).isoformat(),
    }
    return jsonify(result)
