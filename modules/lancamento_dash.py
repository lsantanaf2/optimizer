"""
Dashboard de Lançamento Pago — rotas e agregação de dados.

Fontes (brief seção 2, em ordem de prioridade):
  1. Meta Ads via meta_client (throttle/backoff/imposto já centralizados)
  2. Planilha da plataforma de pagamento (fonte da verdade financeira)

A camada de cálculo vive em modules/lancamento_metrics.py (pura, sem UI).
Este módulo só busca, junta e serve.

Rotas:
  GET /dash/lancamento/<slug>              — página (pública, sem token)
  GET /api/dash/lancamento/<slug>/data     — JSON completo (?since=&until=&refresh=1)
"""

import csv
import io
import json
import logging
from datetime import datetime, date

import requests
from flask import Blueprint, jsonify, render_template, request

from modules.meta_client import GRAPH_BASE, META_TAX_RATE
from modules.lancamento_metrics import (
    normalize_rows, filter_by_patterns, compute_metrics, serie_diaria,
    totais_serie, fase_atual, calcular_meta, merge_config,
)

logger = logging.getLogger(__name__)
lancamento_bp = Blueprint('lancamento', __name__)

CACHE_TTL = 600  # 10 min

# ── Configuração dos lançamentos (brief seção 1 — nada hardcoded no cálculo) ──
LANCAMENTOS = {
    'lp11': {
        'nome': 'Aulão de Balanceamento',
        'expert': 'Edu — Sorveteiro Raiz',
        'edicao': 'LP11 · AGO26',
        'ad_account_id': '741348911043132',
        'campaign_patterns': ['[LP11] [AGO26]'],
        'datas': {
            'inicio_venda_ingresso': '2026-08-17',
            'evento': None,                       # não informado
            'abertura_carrinho': None,
            'fechamento_carrinho': '2026-09-14',
        },
        'precos': {'ingresso': 67.00, 'principal': None},
        # Budget de R$ 10.000 JÁ COM IMPOSTO → teto bruto = 10000 / 1,1215
        'metas': {
            'investimento_real': 10000.00,
            'investimento': round(10000.00 / (1 + META_TAX_RATE), 2),
            'faturamento': 10000.00,     # zero a zero contra o custo real
            'ingressos': None,           # derivada do ticket médio (ver metas_derivadas)
        },
        'alvos': {'roas': 1.0, 'cpa_ingresso': None},   # ROAS sobre CUSTO REAL
        'receita_campo': 'COMISSÃO',     # decisão do cliente
        'vendas': {
            'spreadsheet_id': '1ADWoRJg4MSTgf7mWZfRybFRvwNwwv9aNobjLHd_OqTY',
            'gid': '0',
            'produtos': [
                'AULÃO DE BALANCEAMENTO - com Edu Sorveteiro Raiz',
                'COMBO: TODOS OS PRODUTOS JUNTOS',
                'GRAVAÇÃO - AULÃO DE BALANCEAMENTO com Edu Sorveteiro Raiz',
                'Combo: 4 Ebooks (Ciência do sabor - Boas práticas na prátca - '
                'Comprar produto pronto ou produzir o seu - Sorvete dentro da lei)',
            ],
            'produto_ingresso': 'AULÃO DE BALANCEAMENTO',
        },
        'benchmarks': {
            'roas':         {'alvo': 1.0,  'atencao': 0.15, 'critico': 0.30, 'direcao': 'maior_melhor'},
            'ctr':          {'alvo': 3.5,  'atencao': 0.20, 'critico': 0.40, 'direcao': 'maior_melhor'},
            'cpm':          {'alvo': 35.0, 'atencao': 0.25, 'critico': 0.50, 'direcao': 'menor_melhor'},
            'connect_rate': {'alvo': 75.0, 'atencao': 0.15, 'critico': 0.30, 'direcao': 'maior_melhor'},
        },
    },
}


def _cfg(slug):
    cfg = LANCAMENTOS.get(slug)
    if not cfg:
        return None
    return merge_config(cfg)


# ── Meta Ads ─────────────────────────────────────────────────────────────────

def _fetch_meta(cfg, since, until):
    """Insights diários por anúncio das campanhas do lançamento."""
    from modules.meta_client import meta_get_insights_rows
    acct = cfg['ad_account_id']
    if not acct.startswith('act_'):
        acct = f'act_{acct}'

    from app import obter_token
    token = obter_token()
    if not token:
        raise RuntimeError('Sistema não autenticado na Meta. Contate o administrador.')

    params = {
        'access_token':   token,
        'level':          'ad',
        'fields':         ('campaign_id,campaign_name,adset_name,ad_id,ad_name,'
                           'spend,impressions,reach,frequency,inline_link_clicks,'
                           'actions,action_values'),
        'limit':          500,
        'time_increment': 1,
        'time_range':     json.dumps({'since': since, 'until': until}, separators=(',', ':')),
    }
    raw = meta_get_insights_rows(f'{GRAPH_BASE}/{acct}/insights', params, timeout=60)

    # Extrai as ações do funil que a API devolve aninhadas
    def _act(actions, *tipos):
        total = 0
        for a in (actions or []):
            if a.get('action_type') in tipos:
                total += int(float(a.get('value', 0) or 0))
        return total

    linhas = []
    for r in raw:
        actions = r.get('actions') or []
        linhas.append({
            'date_start':   r.get('date_start'),
            'name':         r.get('campaign_name', ''),
            'campaign_id':  r.get('campaign_id', ''),
            'adset_name':   r.get('adset_name', ''),
            'ad_id':        r.get('ad_id', ''),
            'ad_name':      r.get('ad_name', ''),
            # spend já vem COM imposto (apply_meta_tax no meta_client)? Não:
            # aqui usamos o valor cru da API e o imposto é aplicado na camada
            # de métricas, que expõe bruto e real separadamente.
            'spend':               r.get('spend', 0),
            'impressions':         r.get('impressions', 0),
            'reach':               r.get('reach', 0),
            'inline_link_clicks':  r.get('inline_link_clicks', 0),
            'landing_page_view':   _act(actions, 'landing_page_view',
                                        'omni_landing_page_view'),
            'initiate_checkout':   _act(actions, 'initiate_checkout',
                                        'offsite_conversion.fb_pixel_initiate_checkout',
                                        'omni_initiated_checkout'),
            'compras_pixel':       _act(actions, 'offsite_conversion.fb_pixel_purchase',
                                        'purchase', 'omni_purchase'),
        })
    return filter_by_patterns(normalize_rows(linhas), cfg.get('campaign_patterns'))


# ── Planilha de vendas ───────────────────────────────────────────────────────

def _parse_brl(s):
    s = str(s or '').replace('R$', '').replace('\xa0', '').strip()
    if not s:
        return 0.0
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0


def _fetch_vendas(cfg, since, until):
    """Lê a planilha da plataforma e agrega por dia, só os produtos do lançamento.

    Retorna (vendas_por_dia, resumo).
    """
    vcfg = cfg.get('vendas') or {}
    sid, gid = vcfg.get('spreadsheet_id'), vcfg.get('gid', '0')
    if not sid:
        return {}, {'erro': 'Planilha de vendas não configurada'}

    rows = None
    # 1) CSV público — primário aqui: a planilha é compartilhada por link e o
    #    export CSV responde em segundos. (O values/A:Z da Sheets API trava
    #    nesta planilha, que tem centenas de colunas vazias.)
    try:
        url = (f'https://docs.google.com/spreadsheets/d/{sid}'
               f'/gviz/tq?tqx=out:csv&gid={gid}')
        resp = requests.get(url, timeout=25)
        resp.raise_for_status()
        if resp.text.lstrip().startswith('<'):
            raise RuntimeError('retornou HTML (planilha não é pública)')
        rows = list(csv.DictReader(io.StringIO(resp.text)))
    except Exception as e:
        logger.warning(f'[lancamento] CSV público falhou ({e}) — tentando Service Account')

    # 2) Service Account com range ENXUTO (A:I = colunas úteis)
    if rows is None:
        from modules.cruzamento import _get_google_token
        token = _get_google_token()
        url = f'https://sheets.googleapis.com/v4/spreadsheets/{sid}/values/A1:I20000'
        resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=30)
        resp.raise_for_status()
        vals = resp.json().get('values', [])
        if not vals:
            raise RuntimeError('Planilha de vendas vazia ou inacessível')
        hdr = [h.strip() for h in vals[0]]
        rows = [dict(zip(hdr, r + [''] * (len(hdr) - len(r)))) for r in vals[1:]]

    produtos = [p.lower()[:40] for p in (vcfg.get('produtos') or [])]
    ingresso_key = (vcfg.get('produto_ingresso') or '').lower()
    campo_receita = cfg.get('receita_campo', 'COMISSÃO')

    since_d = datetime.strptime(since, '%Y-%m-%d').date()
    until_d = datetime.strptime(until, '%Y-%m-%d').date()

    por_dia, total, ingressos, reembolsos = {}, {'vendas': 0, 'faturamento': 0.0}, 0, 0
    por_produto = {}
    for r in rows:
        if not (r.get('TRANSAÇÃO') or '').strip():
            continue
        nome = (r.get('PRODUTO') or '').strip()
        if produtos and not any(p in nome.lower() for p in produtos):
            continue
        try:
            d = datetime.strptime((r.get('DATA') or '').strip()[:10], '%d/%m/%Y').date()
        except ValueError:
            continue
        if d < since_d or d > until_d:
            continue

        evento = (r.get('EVENTO') or '').strip().upper()
        valor = _parse_brl(r.get(campo_receita))
        if evento == 'PURCHASE_REFUNDED' or evento == 'PURCHASE_CHARGEBACK':
            reembolsos += 1
            continue
        if evento and evento != 'PURCHASE_APPROVED':
            continue

        k = d.isoformat()
        e = por_dia.setdefault(k, {'vendas': 0, 'faturamento': 0.0})
        e['vendas'] += 1
        e['faturamento'] += valor
        total['vendas'] += 1
        total['faturamento'] += valor
        # startswith, não "contains": "GRAVAÇÃO - AULÃO DE BALANCEAMENTO" contém
        # o nome do ingresso mas é outro produto (order bump).
        if ingresso_key and nome.lower().startswith(ingresso_key):
            ingressos += 1
        p = por_produto.setdefault(nome, {'vendas': 0, 'faturamento': 0.0})
        p['vendas'] += 1
        p['faturamento'] += valor

    for e in por_dia.values():
        e['faturamento'] = round(e['faturamento'], 2)

    resumo = {
        'vendas': total['vendas'],
        'faturamento': round(total['faturamento'], 2),
        'ingressos': ingressos,
        'reembolsos': reembolsos,
        'campo_receita': campo_receita,
        'por_produto': [
            {'produto': k, 'vendas': v['vendas'], 'faturamento': round(v['faturamento'], 2)}
            for k, v in sorted(por_produto.items(), key=lambda x: -x[1]['faturamento'])
        ],
    }
    return por_dia, resumo


# ── Agregações auxiliares ────────────────────────────────────────────────────

def _tabela_entidades(rows_raw, chave):
    """Agrega por campanha / conjunto / anúncio (bloco G do brief)."""
    por = {}
    for r in rows_raw:
        k = r.get(chave) or '—'
        e = por.setdefault(k, {'nome': k, 'investimento': 0.0, 'impressoes': 0,
                               'cliques_link': 0, 'lpv': 0, 'checkouts': 0,
                               'compras_pixel': 0})
        e['investimento'] += r.get('investimento', 0) or 0
        e['impressoes'] += r.get('impressoes', 0) or 0
        e['cliques_link'] += r.get('cliques_link', 0) or 0
        e['lpv'] += r.get('lpv', 0) or 0
        e['checkouts'] += r.get('checkouts', 0) or 0
        e['compras_pixel'] += r.get('compras_pixel', 0) or 0

    saida = []
    for e in por.values():
        inv, imp = e['investimento'], e['impressoes']
        saida.append({
            **e,
            'investimento': round(inv, 2),
            'custo_real':   round(inv * (1 + META_TAX_RATE), 2),
            'cpm': round(inv / imp * 1000, 2) if imp else None,
            'ctr': round(e['cliques_link'] / imp * 100, 2) if imp else None,
            'cpc': round(inv / e['cliques_link'], 2) if e['cliques_link'] else None,
            'cpa': round(inv / e['compras_pixel'], 2) if e['compras_pixel'] else None,
            'connect_rate': round(e['lpv'] / e['cliques_link'] * 100, 2) if e['cliques_link'] else None,
        })
    saida.sort(key=lambda x: -x['investimento'])
    return saida


def _metas_derivadas(cfg, resumo, custo_real):
    """Traduz 'zero a zero' em números acionáveis do dia."""
    metas = cfg.get('metas') or {}
    alvo_fat = metas.get('faturamento')
    fat = resumo.get('faturamento') or 0
    vendas = resumo.get('vendas') or 0
    ticket = (fat / vendas) if vendas else None

    return {
        'budget_real':        metas.get('investimento_real'),
        'budget_bruto':       metas.get('investimento'),
        'faturamento_alvo':   alvo_fat,
        'faturamento_atual':  round(fat, 2),
        'pct_faturamento':    round(fat / alvo_fat * 100, 1) if alvo_fat else None,
        'pct_budget':         round(custo_real / metas['investimento_real'] * 100, 1)
                              if metas.get('investimento_real') else None,
        'ticket_medio':       round(ticket, 2) if ticket else None,
        # Quantas vendas ainda faltam para o zero a zero, no ticket atual
        'vendas_para_break_even': (int(round((alvo_fat - fat) / ticket))
                                   if (alvo_fat and ticket and fat < alvo_fat) else 0),
        'break_even_atingido': bool(alvo_fat and fat >= alvo_fat),
        # Resultado do lançamento até agora
        'resultado': round(fat - custo_real, 2),
    }


# ── Rotas ────────────────────────────────────────────────────────────────────

@lancamento_bp.route('/dash/lancamento/<slug>')
def lancamento_page(slug):
    from modules.rate_limiter import check_rate_limit
    check_rate_limit(f'lancamento-page:{slug}')
    cfg = _cfg(slug)
    if not cfg:
        return render_template('dash_error.html',
                               message='Lançamento não encontrado.'), 404
    return render_template('dash_lancamento.html', slug=slug,
                           nome=cfg['nome'], expert=cfg['expert'], edicao=cfg['edicao'])


@lancamento_bp.route('/api/dash/lancamento/<slug>/data')
def lancamento_data(slug):
    from modules.rate_limiter import check_rate_limit
    check_rate_limit(f'lancamento-api:{slug}')

    cfg = _cfg(slug)
    if not cfg:
        return jsonify({'success': False, 'error': 'Lançamento não encontrado'}), 404

    dts = cfg['datas']
    since = request.args.get('since') or dts.get('inicio_venda_ingresso')
    until = request.args.get('until') or dts.get('fechamento_carrinho') \
        or date.today().isoformat()
    # Nunca consultar além de hoje (Meta devolve erro em datas futuras)
    hoje = date.today().isoformat()
    if until > hoje:
        until = hoje

    from modules.meta_cache import get_or_fetch, invalidate
    if request.args.get('refresh') == '1':
        invalidate(f'lancamento:{slug}')

    try:
        rows = get_or_fetch((f'lancamento:{slug}', 'meta', since, until), CACHE_TTL,
                            lambda: _fetch_meta(cfg, since, until))
    except Exception as e:
        logger.error(f'[lancamento:{slug}] Meta falhou: {e}')
        return jsonify({'success': False, 'error': f'Meta Ads: {e}'}), 502

    try:
        vendas_por_dia, resumo_vendas = get_or_fetch(
            (f'lancamento:{slug}', 'vendas', since, until), CACHE_TTL,
            lambda: _fetch_vendas(cfg, since, until))
    except Exception as e:
        logger.warning(f'[lancamento:{slug}] planilha falhou: {e}')
        vendas_por_dia, resumo_vendas = {}, {'erro': str(e), 'vendas': None}

    tem_vendas = bool(resumo_vendas.get('vendas') is not None)
    metricas = compute_metrics(
        rows,
        vendas_plataforma=({'ingressos': resumo_vendas.get('vendas'),
                            'faturamento_ingresso': resumo_vendas.get('faturamento')}
                           if tem_vendas else None),
        config=cfg,
    )
    serie = serie_diaria(rows, cfg, vendas_por_dia=(vendas_por_dia if tem_vendas else None))
    totais = totais_serie(serie, cfg)
    fase = fase_atual(cfg)
    meta = calcular_meta(rows, cfg, campo='compras_pixel',
                         realizado=resumo_vendas.get('vendas'))
    derivadas = _metas_derivadas(cfg, resumo_vendas,
                                 metricas['custos']['custo_real_midia'])

    # ROAS sobre o CUSTO REAL — é o que define o "zero a zero", já que o
    # budget de referência do cliente inclui o imposto.
    custo_real = metricas['custos']['custo_real_midia']
    fat = resumo_vendas.get('faturamento') or metricas['totais']['valor_pixel']
    metricas['financeiro']['roas_real'] = (round(fat / custo_real, 2)
                                           if custo_real else None)

    return jsonify({
        'success': True,
        'config': {
            'nome': cfg['nome'], 'expert': cfg['expert'], 'edicao': cfg['edicao'],
            'datas': cfg['datas'], 'metas': cfg['metas'], 'alvos': cfg['alvos'],
            'imposto': META_TAX_RATE, 'receita_campo': cfg.get('receita_campo'),
        },
        'periodo': {'since': since, 'until': until},
        'fase': fase,
        'metricas': metricas,
        'meta': meta,
        'derivadas': derivadas,
        'serie_diaria': serie,
        'totais_diarios': totais,
        'vendas': resumo_vendas,
        'campanhas': _tabela_entidades(rows, 'campanha'),
        'gerado_em': datetime.now().isoformat(),
    })
