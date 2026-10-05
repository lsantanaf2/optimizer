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

import concurrent.futures
import csv
import io
import json
import logging
from datetime import datetime, date, timedelta

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
    # Lançamento em duas fases: aquecimento (distribuição de conteúdo) e
    # captura de leads. A fase sai da tag no nome da campanha.
    'black2026': {
        'nome': 'Black 2026',
        'expert': 'Edu — Sorveteiro Raiz',
        'edicao': 'BLACK 2026',
        'ad_account_id': '741348911043132',
        'campaign_patterns': ['[BLACK 2026]'],
        'duas_fases': True,
        'fases': {
            'aquecimento': {
                'label': 'Aquecimento',
                'patterns': ['[AQUECIMENTO]'],
            },
            'captura': {
                'label': 'Captura',
                'patterns': ['[CAPTURA]'],
                # Preenchido quando a planilha de leads existir. Sem isso a aba
                # mostra o investimento e avisa que a fonte de leads falta.
                'leads': {
                    'spreadsheet_id': None,
                    'gid': '0',
                    'coluna_data': None,   # ex: 'Carimbo de data/hora'
                },
            },
        },
        'datas': {
            'inicio_aquecimento': '2026-09-24',
            'inicio_captura': None,
        },
    },
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
                'GRAVAÇÃO - AULÃO DE BALANCEAMENTO com Edu Sorveteiro Raiz',
                'Combo: 4 Ebooks (Ciência do sabor - Boas práticas na prátca - '
                'Comprar produto pronto ou produzir o seu - Sorvete dentro da lei)',
                'Tira Dúvidas de 1 hora em Grupo com Edu Sorveteiro Raiz!',
                'COMBO: TODOS OS PRODUTOS JUNTOS',
            ],
            'produto_ingresso': 'AULÃO DE BALANCEAMENTO',
        },
        # Produto principal vendido depois do aulão. Mesma planilha de vendas,
        # mas o produto tem vendas de edições anteriores: a janela abaixo é o
        # que separa as vendas DESTE lançamento.
        'principal': {
            'produto': 'Profissão Sorveteiro',
            'inicio_vendas': '2026-09-15',
            'fim_vendas': '2026-09-22',          # ~7 dias de carrinho
        },
        # Formulário de perfil respondido pelos inscritos. Nome, e-mail e
        # telefone são descartados no servidor: esta dash é link público.
        'pesquisa': {
            'spreadsheet_id': '1RDnXj3Lr1tGbxbtDekml86r7lBDpefiJc6l-r25D7GM',
            'gid': '0',
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

    # Extrai ações do funil por PRIORIDADE, nunca somando.
    # Os action_types se sobrepõem: 'omni_landing_page_view' já agrega
    # 'landing_page_view'. Somar os dois dobra o volume e produz taxas
    # impossíveis (>100%). Mesmo padrão de meta_api._extract_conversions.
    def _act(actions, *tipos_por_prioridade):
        mapa = {}
        for a in (actions or []):
            t = a.get('action_type')
            if t:
                mapa[t] = mapa.get(t, 0) + int(float(a.get('value', 0) or 0))
        for t in tipos_por_prioridade:
            if mapa.get(t):
                return mapa[t]
        return 0

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
        e = por_dia.setdefault(k, {'vendas': 0, 'ingressos': 0, 'faturamento': 0.0})
        e['vendas'] += 1
        e['faturamento'] += valor
        total['vendas'] += 1
        total['faturamento'] += valor
        # startswith, não "contains": "GRAVAÇÃO - AULÃO DE BALANCEAMENTO" contém
        # o nome do ingresso mas é outro produto (order bump).
        if ingresso_key and nome.lower().startswith(ingresso_key):
            ingressos += 1
            e['ingressos'] += 1
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
        # Custos unitários sobre o CUSTO REAL (com imposto), igual ao resto da dash
        real = round(inv * (1 + META_TAX_RATE), 2)
        saida.append({
            **e,
            'investimento': round(inv, 2),
            'custo_real':   real,
            'cpm': round(real / imp * 1000, 2) if imp else None,
            'ctr': round(e['cliques_link'] / imp * 100, 2) if imp else None,
            'cpc': round(real / e['cliques_link'], 2) if e['cliques_link'] else None,
            'cpa': round(real / e['compras_pixel'], 2) if e['compras_pixel'] else None,
            'connect_rate': round(e['lpv'] / e['cliques_link'] * 100, 2) if e['cliques_link'] else None,
        })
    saida.sort(key=lambda x: -x['investimento'])
    return saida


def _metas_derivadas(cfg, resumo, custo_real):
    """Traduz 'zero a zero' em números acionáveis do dia."""
    metas = cfg.get('metas') or {}
    alvo_fat = metas.get('faturamento')
    fat = resumo.get('faturamento') or 0
    # Ticket médio = receita ÷ ingressos (definição do lançamento). É a base
    # correta para projetar quantos INGRESSOS faltam para o zero a zero.
    ingressos = resumo.get('ingressos') or 0
    ticket = (fat / ingressos) if ingressos else None

    return {
        'budget_real':        metas.get('investimento_real'),
        'budget_bruto':       metas.get('investimento'),
        'faturamento_alvo':   alvo_fat,
        'faturamento_atual':  round(fat, 2),
        'pct_faturamento':    round(fat / alvo_fat * 100, 1) if alvo_fat else None,
        'pct_budget':         round(custo_real / metas['investimento_real'] * 100, 1)
                              if metas.get('investimento_real') else None,
        'ticket_medio':       round(ticket, 2) if ticket else None,
        'ticket_base':        'comissão ÷ ingressos',
        # Quantos INGRESSOS ainda faltam para o zero a zero, no ticket atual
        'vendas_para_break_even': (int(round((alvo_fat - fat) / ticket))
                                   if (alvo_fat and ticket and fat < alvo_fat) else 0),
        'break_even_atingido': bool(alvo_fat and fat >= alvo_fat),
        # Resultado do lançamento até agora
        'resultado': round(fat - custo_real, 2),
    }


# ── Pesquisa de perfil dos inscritos ─────────────────────────────────────────

# Nome, e-mail e telefone não aparecem em nenhuma lista abaixo — é assim que
# ficam fora da resposta da API, já que só as colunas mapeadas são lidas.
# Campos que viram gráfico. 'multi' = resposta de múltipla escolha, em que uma
# pessoa conta para vários valores.
PESQUISA_CAMPOS = [
    {'key': 'situacao',     'label': 'Situação atual',        'tipo': 'single',
     'coluna': 'Qual a sua situação atual?'},
    {'key': 'faturamento',  'label': 'Faturamento mensal',    'tipo': 'single',
     'coluna': 'Quanto a sua empresa vende por mês'},
    {'key': 'dificuldade',  'label': 'Maior dificuldade',     'tipo': 'multi',
     'coluna': 'Qual a sua maior dificuldade hoje?'},
    {'key': 'produtos',     'label': 'Produtos que trabalha', 'tipo': 'multi',
     'coluna': 'Quais produtos você trabalha?'},
    {'key': 'tempo',        'label': 'Tempo no ramo',         'tipo': 'single',
     'coluna': 'Quanto tempo você está no ramo de sorvete?'},
    {'key': 'idade',        'label': 'Faixa etária',          'tipo': 'single',
     'coluna': 'Qual sua faixa etária'},
    {'key': 'sexo',         'label': 'Sexo',                  'tipo': 'single',
     'coluna': 'Qual seu sexo?'},
    {'key': 'escolaridade', 'label': 'Escolaridade',          'tipo': 'single',
     'coluna': 'Qual o seu nível de escolaridade?'},
    {'key': 'estado',       'label': 'Estado',                'tipo': 'single',
     'coluna': 'Qual é o seu estado?'},
    {'key': 'origem',       'label': 'Como conheceu',         'tipo': 'single',
     'coluna': 'Como você conheceu meu trabalho?'},
]

# Texto livre: não vira gráfico, vira lista (filtrável pelos gráficos acima).
PESQUISA_TEXTOS = [
    {'key': 'objetivo',   'label': 'Objetivos',
     'coluna': 'Agora vamos ao que interessa! Me diga com detalhes quais são os '
               'seus objetivos. O que você quer melhorar?'},
    {'key': 'pergunta',   'label': 'O que perguntaria ao Edu',
     'coluna': 'Eu e você frente a frente, o que você me perguntaria?'},
    {'key': 'expectativa', 'label': 'Expectativa',
     'coluna': 'Qual a sua expectativa para o AULÃO DE BALANCEAMENTO com Edu Sorveteiro Raiz?'},
    {'key': 'maquinario',  'label': 'Maquinário',
     'coluna': 'Já possui maquinário? Se sim, quais?'},
    {'key': 'capacidade',  'label': 'Capacidade diária',
     'coluna': 'Qual a sua capacidade produtiva diária?'},
]

_UF = {
    'ac': 'Acre', 'al': 'Alagoas', 'ap': 'Amapá', 'am': 'Amazonas', 'ba': 'Bahia',
    'ce': 'Ceará', 'df': 'Distrito Federal', 'es': 'Espírito Santo', 'go': 'Goiás',
    'ma': 'Maranhão', 'mt': 'Mato Grosso', 'ms': 'Mato Grosso do Sul',
    'mg': 'Minas Gerais', 'pa': 'Pará', 'pb': 'Paraíba', 'pr': 'Paraná',
    'pe': 'Pernambuco', 'pi': 'Piauí', 'rj': 'Rio de Janeiro',
    'rn': 'Rio Grande do Norte', 'rs': 'Rio Grande do Sul', 'ro': 'Rondônia',
    'rr': 'Roraima', 'sc': 'Santa Catarina', 'sp': 'São Paulo',
    'se': 'Sergipe', 'to': 'Tocantins',
}
_CONECTIVOS = ('de', 'do', 'da', 'dos', 'das', 'e')


def _split_multi(valor):
    """Quebra resposta de múltipla escolha em valores.

    O separador é a vírgula, mas duas opções do formulário têm vírgula dentro:
    "Sorvete de palito (picolé, paleta, ...)" e "Geladinho - ... - Sacolé, etc".
    Por isso ignora vírgula entre parênteses e recola o fragmento "etc".
    """
    if not valor:
        return []
    partes, atual, prof = [], '', 0
    for ch in valor:
        if ch == '(':
            prof += 1
        elif ch == ')':
            prof = max(0, prof - 1)
        if ch == ',' and prof == 0:
            partes.append(atual)
            atual = ''
        else:
            atual += ch
    partes.append(atual)

    out = []
    for p in partes:
        p = p.strip().rstrip('.').strip()
        if not p:
            continue
        if p.lower() in ('etc', 'etc.') and out:
            out[-1] += ', etc'
            continue
        out.append(p)
    return out


def _norm_estado(valor):
    v = (valor or '').strip()
    if not v:
        return 'Não informado'
    v = v.split(',')[0].strip()          # "São Paulo, Caçapava" → "São Paulo"
    if len(v) == 2 and v.lower() in _UF:
        return _UF[v.lower()]
    palavras = [w.capitalize() if w.lower() not in _CONECTIVOS else w.lower()
                for w in v.split()]
    if palavras:
        palavras[0] = palavras[0].capitalize()
    return ' '.join(palavras)


def _bucket_faturamento(valor):
    """Normaliza o faturamento: algumas respostas vêm como número livre."""
    v = (valor or '').strip()
    if not v:
        return 'Não informado'
    low = v.lower()
    if 'não tenho' in low or 'nao tenho' in low:
        return 'Não tem loja/fábrica'
    if 'mil' in low:                      # já é uma das faixas do formulário
        return v
    num = _parse_brl(v)
    if num:
        if num < 25000:
            return 'Abaixo de R$ 25 mil'
        if num < 50000:
            return 'R$25 mil a R$50 mil'
        if num < 100000:
            return 'R$50 mil a R$100 mil'
        return 'Acima de R$ 100 mil'
    return 'Não informado'


def _fetch_pesquisa(cfg):
    """Lê o formulário de perfil e devolve as respostas SEM dado pessoal."""
    pcfg = cfg.get('pesquisa') or {}
    sid, gid = pcfg.get('spreadsheet_id'), pcfg.get('gid', '0')
    if not sid:
        return []

    url = (f'https://docs.google.com/spreadsheets/d/{sid}'
           f'/gviz/tq?tqx=out:csv&gid={gid}')
    resp = requests.get(url, timeout=25)
    resp.raise_for_status()
    if resp.text.lstrip().startswith('<'):
        raise RuntimeError('retornou HTML (planilha não é pública)')
    rows = list(csv.DictReader(io.StringIO(resp.text)))

    # Mapeia header real → coluna esperada, tolerando espaço/caixa diferentes
    def _achar(header, alvo):
        alvo_n = alvo.strip().lower()
        for h in header:
            if (h or '').strip().lower() == alvo_n:
                return h
        return None

    header = list(rows[0].keys()) if rows else []
    mapa_campos = {c['key']: _achar(header, c['coluna']) for c in PESQUISA_CAMPOS}
    mapa_textos = {t['key']: _achar(header, t['coluna']) for t in PESQUISA_TEXTOS}

    out = []
    for r in rows:
        # Descarta linha vazia (planilha de formulário costuma ter sobra)
        if not any((v or '').strip() for v in r.values()):
            continue

        reg = {}
        for campo in PESQUISA_CAMPOS:
            col = mapa_campos.get(campo['key'])
            bruto = (r.get(col) or '').strip() if col else ''
            if campo['key'] == 'estado':
                reg[campo['key']] = _norm_estado(bruto)
            elif campo['key'] == 'faturamento':
                reg[campo['key']] = _bucket_faturamento(bruto)
            elif campo['tipo'] == 'multi':
                reg[campo['key']] = _split_multi(bruto) or ['Não informado']
            else:
                reg[campo['key']] = bruto or 'Não informado'

        for texto in PESQUISA_TEXTOS:
            col = mapa_textos.get(texto['key'])
            reg[texto['key']] = (r.get(col) or '').strip() if col else ''

        out.append(reg)

    logger.info(f'[lancamento] pesquisa: {len(out)} respostas lidas (sem PII)')
    return out


# ── Lançamento em duas fases (aquecimento + captura) ─────────────────────────

def _fase_de(nome_campanha, cfg):
    """Qual fase a campanha pertence, pela tag no nome."""
    alvo = (nome_campanha or '').lower()
    for chave, fase in (cfg.get('fases') or {}).items():
        for p in fase.get('patterns') or []:
            if p.lower() in alvo:
                return chave
    return None


def _fetch_por_anuncio(cfg, since, until):
    """Insights do período agregados por anúncio, com métricas de vídeo.

    Hook rate e custo por VV precisam de video_play_actions (views de 3s) e dos
    marcos de 75% e 95%, que não vêm na busca diária do lançamento.
    """
    from modules.meta_client import meta_get_insights_rows
    acct = cfg['ad_account_id']
    if not acct.startswith('act_'):
        acct = f'act_{acct}'

    from app import obter_token
    token = obter_token()
    if not token:
        raise RuntimeError('Sistema não autenticado na Meta. Contate o administrador.')

    params = {
        'access_token': token,
        'level':        'ad',
        'fields':       ('campaign_name,adset_name,ad_id,ad_name,'
                         'spend,impressions,reach,frequency,inline_link_clicks,'
                         'video_continuous_2_sec_watched_actions,'
                         'video_p75_watched_actions,'
                         'video_p95_watched_actions,actions'),
        'limit':        500,
        'time_range':   json.dumps({'since': since, 'until': until}, separators=(',', ':')),
    }
    raw = meta_get_insights_rows(f'{GRAPH_BASE}/{acct}/insights', params, timeout=60)

    def _vv(campo):
        """Soma o marco de vídeo (a Meta devolve como lista de actions)."""
        return sum(int(float(a.get('value', 0) or 0)) for a in (campo or [])
                   if a.get('action_type') == 'video_view')

    padroes = [p.lower() for p in (cfg.get('campaign_patterns') or [])]
    saida = []
    for r in raw:
        nome_camp = r.get('campaign_name', '')
        if padroes and not any(p in nome_camp.lower() for p in padroes):
            continue
        gasto_bruto = float(r.get('spend', 0) or 0)
        imp   = int(r.get('impressions', 0) or 0)
        # Hook: view CONTÍNUO de 2s. O antigo video_3_sec_watched_actions saiu
        # da API, e video_play_actions conta reprodução iniciada — com autoplay
        # isso dá ~97% das impressões e não mede retenção nenhuma.
        v2s   = _vv(r.get('video_continuous_2_sec_watched_actions'))
        p75   = _vv(r.get('video_p75_watched_actions'))
        p95   = _vv(r.get('video_p95_watched_actions'))
        custo = round(gasto_bruto * (1 + META_TAX_RATE), 2)   # custo real
        saida.append({
            'fase':        _fase_de(nome_camp, cfg),
            'campanha':    nome_camp,
            'adset':       r.get('adset_name', ''),
            'ad_id':       r.get('ad_id', ''),
            'ad':          r.get('ad_name', ''),
            'custo':       custo,
            'impressoes':  imp,
            'alcance':     int(r.get('reach', 0) or 0),
            'frequencia':  round(float(r.get('frequency', 0) or 0), 2),
            'cliques':     int(r.get('inline_link_clicks', 0) or 0),
            'video_2s':    v2s,
            'video_p75':   p75,
            'video_p95':   p95,
            # Hook rate: quem parou nos 3 primeiros segundos, sobre quem viu
            'hook_rate':   round(v2s / imp * 100, 2) if imp else None,
            'retencao_75': round(p75 / v2s * 100, 2) if v2s else None,
            'custo_vv75':  round(custo / p75, 2) if p75 else None,
            'custo_vv95':  round(custo / p95, 2) if p95 else None,
            'cpm':         round(custo / imp * 1000, 2) if imp else None,
        })
    saida.sort(key=lambda x: x['custo'], reverse=True)
    return saida


def _fetch_insights_nivel(cfg, since, until, nivel, campos):
    """Insights do período num nível (adset/campaign), filtrados pelo lançamento."""
    from modules.meta_client import meta_get_insights_rows
    acct = cfg['ad_account_id']
    if not acct.startswith('act_'):
        acct = f'act_{acct}'
    from app import obter_token
    token = obter_token()
    if not token:
        raise RuntimeError('Sistema não autenticado na Meta.')

    raw = meta_get_insights_rows(f'{GRAPH_BASE}/{acct}/insights', {
        'access_token': token, 'level': nivel, 'fields': campos, 'limit': 500,
        'time_range': json.dumps({'since': since, 'until': until}, separators=(',', ':')),
    }, timeout=60)
    padroes = [p.lower() for p in (cfg.get('campaign_patterns') or [])]
    return [r for r in raw
            if not padroes or any(p in (r.get('campaign_name') or '').lower() for p in padroes)]


def _fetch_publicos(cfg, since, until):
    """Alcance e frequência por CONJUNTO — é onde o público é definido."""
    linhas = _fetch_insights_nivel(
        cfg, since, until, 'adset',
        'campaign_name,adset_id,adset_name,spend,impressions,reach,frequency')
    saida = []
    for r in linhas:
        imp = int(r.get('impressions', 0) or 0)
        alc = int(r.get('reach', 0) or 0)
        saida.append({
            'fase':       _fase_de(r.get('campaign_name', ''), cfg),
            'conjunto':   r.get('adset_name', ''),
            'custo':      round(float(r.get('spend', 0) or 0) * (1 + META_TAX_RATE), 2),
            'impressoes': imp,
            'alcance':    alc,
            # Vem pronta da Meta e é deduplicada dentro do conjunto
            'frequencia': round(float(r.get('frequency', 0) or 0), 2),
        })
    saida.sort(key=lambda x: x['alcance'], reverse=True)
    return saida


def _fetch_alcance_campanha(cfg, since, until):
    """Alcance por CAMPANHA.

    Não dá para somar o alcance dos anúncios: quem viu três criativos entraria
    três vezes, inflando o alcance e afundando a frequência. A Meta deduplica
    dentro da campanha, então a frequência geral sai daqui.
    """
    linhas = _fetch_insights_nivel(
        cfg, since, until, 'campaign',
        'campaign_name,impressions,reach,frequency,spend')
    por_fase = {}
    for r in linhas:
        fase = _fase_de(r.get('campaign_name', ''), cfg)
        if not fase:
            continue
        e = por_fase.setdefault(fase, {'alcance': 0, 'impressoes': 0, 'campanhas': 0})
        e['alcance'] += int(r.get('reach', 0) or 0)
        e['impressoes'] += int(r.get('impressions', 0) or 0)
        e['campanhas'] += 1
    for e in por_fase.values():
        e['frequencia'] = round(e['impressoes'] / e['alcance'], 2) if e['alcance'] else None
        # Com mais de uma campanha a soma volta a contar gente repetida
        e['exato'] = e['campanhas'] <= 1
    return por_fase


def _totais_fase(ads):
    """Soma os anúncios de uma fase, recalculando as taxas sobre o total."""
    t = {k: sum(a[k] or 0 for a in ads) for k in
         ('custo', 'impressoes', 'alcance', 'cliques', 'video_2s', 'video_p75', 'video_p95')}
    t['custo'] = round(t['custo'], 2)
    t['ads'] = len(ads)
    t['hook_rate']  = round(t['video_2s'] / t['impressoes'] * 100, 2) if t['impressoes'] else None
    t['retencao_75'] = round(t['video_p75'] / t['video_2s'] * 100, 2) if t['video_2s'] else None
    t['custo_vv75'] = round(t['custo'] / t['video_p75'], 2) if t['video_p75'] else None
    t['custo_vv95'] = round(t['custo'] / t['video_p95'], 2) if t['video_p95'] else None
    t['cpm']        = round(t['custo'] / t['impressoes'] * 1000, 2) if t['impressoes'] else None
    t['frequencia'] = round(t['impressoes'] / t['alcance'], 2) if t['alcance'] else None
    return t


def _fetch_leads(fase_cfg, since, until):
    """Leads por dia da planilha de captura. Sem planilha configurada: None."""
    lcfg = (fase_cfg or {}).get('leads') or {}
    sid, gid = lcfg.get('spreadsheet_id'), lcfg.get('gid', '0')
    if not sid:
        return None

    url = (f'https://docs.google.com/spreadsheets/d/{sid}'
           f'/gviz/tq?tqx=out:csv&gid={gid}')
    resp = requests.get(url, timeout=25)
    resp.raise_for_status()
    if resp.text.lstrip().startswith('<'):
        raise RuntimeError('planilha de leads não é pública')
    linhas = list(csv.DictReader(io.StringIO(resp.text)))
    if not linhas:
        return {}

    col = lcfg.get('coluna_data') or list(linhas[0].keys())[0]
    since_d = datetime.strptime(since, '%Y-%m-%d').date()
    until_d = datetime.strptime(until, '%Y-%m-%d').date()
    por_dia = {}
    for r in linhas:
        bruto = (r.get(col) or '').strip()[:10]
        for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
            try:
                d = datetime.strptime(bruto, fmt).date()
                break
            except ValueError:
                d = None
        if not d or d < since_d or d > until_d:
            continue
        por_dia[d.isoformat()] = por_dia.get(d.isoformat(), 0) + 1
    return por_dia


# ── Rotas ────────────────────────────────────────────────────────────────────

@lancamento_bp.route('/dash/lancamento/<slug>')
def lancamento_page(slug):
    from modules.rate_limiter import check_rate_limit
    check_rate_limit(f'lancamento-page:{slug}')
    cfg = _cfg(slug)
    if not cfg:
        return render_template('dash_error.html',
                               message='Ocorreu um erro ao carregar o dashboard. Avise o desenvolvedor.', code='DSH-104'), 404
    template = 'dash_fases.html' if cfg.get('duas_fases') else 'dash_lancamento.html'
    return render_template(template, slug=slug,
                           nome=cfg['nome'], expert=cfg['expert'], edicao=cfg['edicao'])


@lancamento_bp.route('/api/dash/lancamento/<slug>/instagram')
def lancamento_instagram(slug):
    """Aba Instagram: posts impulsionados + crescimento do perfil + orgânico."""
    from modules.rate_limiter import check_rate_limit
    check_rate_limit(f'lancamento-ig:{slug}')

    cfg = _cfg(slug)
    if not cfg:
        return jsonify({'success': False, 'error': 'Lançamento não encontrado'}), 404

    from app import obter_token
    token = obter_token()
    if not token:
        return jsonify({'success': False, 'error': 'Sistema não autenticado na Meta.'}), 503

    # Período: por padrão os últimos 30 dias (limite do follower_count do IG),
    # que é uma janela diferente da do lançamento — o perfil é contínuo.
    hoje = date.today()
    since = request.args.get('since') or (hoje - timedelta(days=29)).isoformat()
    until = request.args.get('until') or hoje.isoformat()

    from modules.meta_cache import get_or_fetch, invalidate
    from modules.instagram_insights import (
        fetch_boosted_posts, totais_boosted, fetch_ig_account,
        fetch_follower_growth, fetch_organic_posts,
    )
    if request.args.get('refresh') == '1':
        invalidate(f'lancamento-ig:{slug}')

    acct = cfg['ad_account_id']
    avisos = []

    # 1. Posts impulsionados (sempre funciona com os escopos atuais)
    try:
        posts = get_or_fetch((f'lancamento-ig:{slug}', 'boost', since, until), CACHE_TTL,
                             lambda: fetch_boosted_posts(acct, token, since, until))
    except Exception as e:
        logger.error(f'[instagram:{slug}] impulsionados: {e}')
        return jsonify({'success': False, 'error': f'Meta Ads: {e}'}), 502

    # 2 e 3. Dependem de instagram_manage_insights — degradam sem quebrar
    ig = get_or_fetch((f'lancamento-ig:{slug}', 'acct'), CACHE_TTL,
                      lambda: fetch_ig_account(acct, token))
    crescimento, err_growth = [], None
    organicos, err_org = [], None
    if ig and ig.get('id'):
        crescimento, err_growth = get_or_fetch(
            (f'lancamento-ig:{slug}', 'growth', since, until), CACHE_TTL,
            lambda: fetch_follower_growth(ig['id'], token, since, until))
        organicos, err_org = get_or_fetch(
            (f'lancamento-ig:{slug}', 'organic'), CACHE_TTL,
            lambda: fetch_organic_posts(ig['id'], token))
    else:
        avisos.append('Nenhuma conta do Instagram vinculada a esta conta de anúncios.')
    for e in (err_growth, err_org):
        if e and e not in avisos:
            avisos.append(e)

    tot = totais_boosted(posts)
    # Quanto do crescimento veio de anúncio (quando temos as duas séries)
    novos_total = sum(d['novos_seguidores'] for d in crescimento) if crescimento else None
    pct_pago = (round(tot['seguidores'] / novos_total * 100, 1)
                if (novos_total and tot['seguidores']) else None)

    return jsonify({
        'success': True,
        'periodo': {'since': since, 'until': until},
        'conta_ig': ig,
        'impulsionados': posts,
        'totais': tot,
        'crescimento': crescimento,
        'organicos': organicos,
        'resumo_perfil': {
            'novos_seguidores': novos_total,
            'seguidores_via_ads': tot['seguidores'],
            'pct_via_ads': pct_pago,
            'custo_seguidor': tot['custo_seguidor'],
        },
        'avisos': avisos,
        'gerado_em': datetime.now().isoformat(),
    })


@lancamento_bp.route('/api/dash/lancamento/<slug>/criativo/<ad_id>')
def lancamento_criativo(slug, ad_id):
    """Miniatura e textos de um anúncio, para o modal da dash.

    Devolve só campos seguros. O endpoint de preview da Meta retorna um iframe
    com o access token na URL — como esta dash é link público, ele não pode ser
    repassado ao navegador.
    """
    from modules.rate_limiter import check_rate_limit
    check_rate_limit(f'lancamento-criativo:{slug}')

    if not ad_id.isdigit():
        return jsonify({'success': False, 'error': 'id inválido'}), 400
    if not _cfg(slug):
        return jsonify({'success': False, 'error': 'Lançamento não encontrado'}), 404

    from app import obter_token
    from modules.meta_client import meta_get
    token = obter_token()
    if not token:
        return jsonify({'success': False, 'error': 'Sem token Meta'}), 503

    try:
        d = meta_get(f'{GRAPH_BASE}/{ad_id}', {
            'access_token': token,
            'fields': 'name,creative{thumbnail_url,image_url,title,body,object_story_spec}',
        })
    except Exception as e:
        logger.warning(f'[lancamento:{slug}] criativo {ad_id} falhou: {e}')
        return jsonify({'success': False, 'error': str(e)}), 502

    c = d.get('creative') or {}
    spec = c.get('object_story_spec') or {}
    video = spec.get('video_data') or {}
    link = (spec.get('link_data') or {})
    return jsonify({
        'success': True,
        'ad': d.get('name', ''),
        'thumb': c.get('thumbnail_url') or c.get('image_url') or video.get('image_url'),
        'titulo': c.get('title') or link.get('name') or video.get('title') or '',
        'texto': c.get('body') or link.get('message') or video.get('message') or '',
    })


@lancamento_bp.route('/api/dash/lancamento/<slug>/fases')
def lancamento_fases(slug):
    """Lançamento em duas fases: aquecimento (conteúdo) e captura (leads)."""
    from modules.rate_limiter import check_rate_limit
    check_rate_limit(f'lancamento-fases:{slug}')

    cfg = _cfg(slug)
    if not cfg or not cfg.get('duas_fases'):
        return jsonify({'success': False, 'error': 'Lançamento não encontrado'}), 404

    hoje = date.today().isoformat()
    dts = cfg.get('datas') or {}
    since = request.args.get('since') or dts.get('inicio_aquecimento') or hoje
    until = min(request.args.get('until') or hoje, hoje)

    from modules.meta_cache import get_or_fetch, invalidate
    if request.args.get('refresh') == '1':
        invalidate(f'lancamento:{slug}')

    try:
        ads = get_or_fetch((f'lancamento:{slug}', 'por_anuncio', since, until), CACHE_TTL,
                           lambda: _fetch_por_anuncio(cfg, since, until))
    except Exception as e:
        logger.error(f'[lancamento:{slug}] fases/Meta falhou: {e}')
        return jsonify({'success': False, 'error': f'Meta Ads: {e}'}), 502

    # Alcance por conjunto e por campanha: a Meta deduplica em cada nível, e é
    # a única forma de ter frequência real.
    publicos, alcance_fase = [], {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f_pub = ex.submit(get_or_fetch, (f'lancamento:{slug}', 'publicos', since, until),
                          CACHE_TTL, lambda: _fetch_publicos(cfg, since, until))
        f_alc = ex.submit(get_or_fetch, (f'lancamento:{slug}', 'alcance', since, until),
                          CACHE_TTL, lambda: _fetch_alcance_campanha(cfg, since, until))
        try:    publicos = f_pub.result()
        except Exception as e:
            logger.warning(f'[lancamento:{slug}] públicos falharam: {e}')
        try:    alcance_fase = f_alc.result()
        except Exception as e:
            logger.warning(f'[lancamento:{slug}] alcance por campanha falhou: {e}')

    fases_cfg = cfg.get('fases') or {}
    saida = {}
    for chave, fcfg in fases_cfg.items():
        desta = [a for a in ads if a['fase'] == chave]
        totais = _totais_fase(desta)
        real = alcance_fase.get(chave)
        if real:
            # Substitui a soma por anúncio, que conta a mesma pessoa várias vezes
            totais['alcance'] = real['alcance']
            totais['frequencia'] = real['frequencia']
            totais['alcance_exato'] = real['exato']
        saida[chave] = {
            'label':    fcfg.get('label', chave.title()),
            'ads':      desta,
            'publicos': [p for p in publicos if p['fase'] == chave],
            'totais':   totais,
        }

    # Leads da fase de captura (planilha), quando houver fonte configurada
    cap = fases_cfg.get('captura') or {}
    leads_por_dia, leads_erro = None, None
    try:
        leads_por_dia = get_or_fetch((f'lancamento:{slug}', 'leads', since, until), CACHE_TTL,
                                     lambda: _fetch_leads(cap, since, until))
    except Exception as e:
        leads_erro = str(e)
        logger.warning(f'[lancamento:{slug}] leads falharam: {e}')

    if 'captura' in saida:
        total_leads = sum((leads_por_dia or {}).values()) if leads_por_dia else None
        custo_cap = saida['captura']['totais']['custo']
        saida['captura']['leads'] = {
            'configurada': bool((cap.get('leads') or {}).get('spreadsheet_id')),
            'erro':        leads_erro,
            'por_dia':     [{'data': d, 'leads': n} for d, n in sorted((leads_por_dia or {}).items())],
            'total':       total_leads,
            'cpl':         round(custo_cap / total_leads, 2) if total_leads else None,
        }

    # Fora das duas fases (campanha do lançamento sem tag de fase)
    sem_fase = [a for a in ads if not a['fase']]

    return jsonify({
        'success': True,
        'config': {'nome': cfg['nome'], 'expert': cfg['expert'], 'edicao': cfg['edicao'],
                   'imposto': META_TAX_RATE, 'datas': dts},
        'periodo': {'since': since, 'until': until},
        'fases':   saida,
        'sem_fase': {'ads': sem_fase, 'totais': _totais_fase(sem_fase)},
        'gerado_em': datetime.now().isoformat(),
    })


@lancamento_bp.route('/api/dash/lancamento/<slug>/resumo')
def lancamento_resumo(slug):
    """Aba Resumo: ROI geral do lançamento (ingressos + produto principal)."""
    from modules.rate_limiter import check_rate_limit
    check_rate_limit(f'lancamento-resumo:{slug}')

    cfg = _cfg(slug)
    if not cfg:
        return jsonify({'success': False, 'error': 'Lançamento não encontrado'}), 404

    pcfg = cfg.get('principal') or {}
    hoje = date.today().isoformat()
    since = cfg['datas']['inicio_venda_ingresso']
    # Até o fim do carrinho do principal (ou hoje): o gasto depois do
    # fechamento do ingresso também é custo deste lançamento.
    until = min(pcfg.get('fim_vendas') or hoje, hoje)

    from modules.meta_cache import get_or_fetch, invalidate
    if request.args.get('refresh') == '1':
        invalidate(f'lancamento:{slug}')

    try:
        rows = get_or_fetch((f'lancamento:{slug}', 'meta', since, until), CACHE_TTL,
                            lambda: _fetch_meta(cfg, since, until))
    except Exception as e:
        logger.error(f'[lancamento:{slug}] resumo/Meta falhou: {e}')
        return jsonify({'success': False, 'error': f'Meta Ads: {e}'}), 502
    # compute_metrics aplica o imposto de 12,15% sobre o gasto bruto
    custos = compute_metrics(rows, None, cfg)['custos']

    try:
        _, front = get_or_fetch((f'lancamento:{slug}', 'vendas', since, until), CACHE_TTL,
                                lambda: _fetch_vendas(cfg, since, until))
    except Exception as e:
        logger.error(f'[lancamento:{slug}] resumo/vendas falhou: {e}')
        return jsonify({'success': False, 'error': f'Planilha de vendas: {e}'}), 502

    ingresso_key = (cfg['vendas'].get('produto_ingresso') or '').lower()
    com_ingresso, bumps = 0.0, []
    for p in front.get('por_produto') or []:
        if ingresso_key and p['produto'].lower().startswith(ingresso_key):
            com_ingresso += p['faturamento']
        else:
            bumps.append(p)
    com_bumps = sum(p['faturamento'] for p in bumps)

    # Produto principal: reaproveita _fetch_vendas trocando a lista de
    # produtos, com a janela própria do carrinho.
    principal = {'produto': pcfg.get('produto'), 'vendas': 0, 'comissao': 0.0,
                 'reembolsos': 0, 'por_dia': [],
                 'periodo': {'since': pcfg.get('inicio_vendas'), 'until': pcfg.get('fim_vendas')},
                 'iniciado': bool(pcfg.get('inicio_vendas')) and hoje >= pcfg['inicio_vendas']}
    if pcfg.get('produto') and principal['iniciado']:
        p_since, p_until = pcfg['inicio_vendas'], min(pcfg.get('fim_vendas') or hoje, hoje)
        cfg_p = {**cfg, 'vendas': {**cfg['vendas'], 'produtos': [pcfg['produto']],
                                   'produto_ingresso': ''}}
        try:
            por_dia, resumo_p = get_or_fetch(
                (f'lancamento:{slug}', 'principal', p_since, p_until), CACHE_TTL,
                lambda: _fetch_vendas(cfg_p, p_since, p_until))
            principal.update({
                'vendas': resumo_p.get('vendas') or 0,
                'comissao': resumo_p.get('faturamento') or 0.0,
                'reembolsos': resumo_p.get('reembolsos') or 0,
                'por_dia': [{'data': d, 'vendas': v['vendas'], 'comissao': v['faturamento']}
                            for d, v in sorted(por_dia.items())],
            })
        except Exception as e:
            logger.error(f'[lancamento:{slug}] resumo/principal falhou: {e}')
            return jsonify({'success': False, 'error': f'Vendas do principal: {e}'}), 502

    custo = custos['custo_real_midia']
    receita = round(com_ingresso + com_bumps + principal['comissao'], 2)
    lucro = round(receita - custo, 2)

    return jsonify({
        'success': True,
        'periodo': {'since': since, 'until': until},
        'custos': {
            'bruto': custos['investimento_bruto'],
            'imposto': custos['imposto_valor'],
            'aliquota': custos['imposto_aliquota'],
            'real': custo,
        },
        'ingressos': {
            'quantidade': front.get('ingressos') or 0,
            'comissao': round(com_ingresso, 2),
            'reembolsos': front.get('reembolsos') or 0,
            'custo_por_ingresso': (round(custo / front['ingressos'], 2)
                                   if front.get('ingressos') else None),
        },
        'bumps': {'comissao': round(com_bumps, 2), 'produtos': bumps},
        'principal': principal,
        'geral': {
            'receita': receita,
            'lucro': lucro,
            'roi': round(lucro / custo * 100, 1) if custo else None,
            'roas': round(receita / custo, 2) if custo else None,
        },
        'campo_receita': cfg.get('receita_campo'),
        'gerado_em': datetime.now().isoformat(),
    })


@lancamento_bp.route('/api/dash/lancamento/<slug>/pesquisa')
def lancamento_pesquisa(slug):
    """Aba Pesquisa: perfil dos inscritos, agregável e sem dado pessoal."""
    from modules.rate_limiter import check_rate_limit
    check_rate_limit(f'lancamento-pesquisa:{slug}')

    cfg = _cfg(slug)
    if not cfg:
        return jsonify({'success': False, 'error': 'Lançamento não encontrado'}), 404
    if not (cfg.get('pesquisa') or {}).get('spreadsheet_id'):
        return jsonify({'success': False, 'error': 'Pesquisa não configurada'}), 404

    from modules.meta_cache import get_or_fetch, invalidate
    if request.args.get('refresh') == '1':
        invalidate(f'lancamento:{slug}')

    try:
        respostas = get_or_fetch((f'lancamento:{slug}', 'pesquisa'), CACHE_TTL,
                                 lambda: _fetch_pesquisa(cfg))
    except Exception as e:
        logger.error(f'[lancamento:{slug}] pesquisa falhou: {e}')
        return jsonify({'success': False, 'error': f'Planilha da pesquisa: {e}'}), 502

    # Ingressos vendidos: base do percentual de resposta. Mesma janela da aba
    # principal, para o número bater com o card de Ingressos.
    dts = cfg['datas']
    since = dts.get('inicio_venda_ingresso')
    until = dts.get('fechamento_carrinho') or date.today().isoformat()
    hoje = date.today().isoformat()
    if until > hoje:
        until = hoje

    ingressos = None
    try:
        _, resumo_vendas = get_or_fetch(
            (f'lancamento:{slug}', 'vendas', since, until), CACHE_TTL,
            lambda: _fetch_vendas(cfg, since, until))
        ingressos = resumo_vendas.get('ingressos')
    except Exception as e:
        logger.warning(f'[lancamento:{slug}] ingressos p/ taxa de resposta: {e}')

    return jsonify({
        'success':   True,
        'respostas': respostas,
        'total':     len(respostas),
        'ingressos': ingressos,
        'campos':    [{'key': c['key'], 'label': c['label'], 'tipo': c['tipo']}
                      for c in PESQUISA_CAMPOS],
        'textos':    [{'key': t['key'], 'label': t['label']} for t in PESQUISA_TEXTOS],
    })


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
        # Funil e CPA usam INGRESSOS; receita e ticket usam todas as vendas
        vendas_plataforma=({'ingressos': resumo_vendas.get('ingressos'),
                            'vendas_totais': resumo_vendas.get('vendas'),
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
