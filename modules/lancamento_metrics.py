"""
Camada de métricas do Dashboard de Lançamento Pago — PURA, sem UI.

Segue o brief "Dashboard de Lançamento Pago (SK Marketing)":
  - Seção 1: parâmetros configuráveis (nunca hardcoded)
  - Seção 3: fases do lançamento
  - Seção 5: dicionário de métricas (fórmulas exatas)
  - Seção 6: semáforo e benchmarks (thresholds vêm da config)

Regras que este módulo respeita:
  - Nenhuma dependência de UI nem de Flask → testável e reaproveitável no PDF
  - Investimento BRUTO = o que o Gerenciador mostra
    Custo REAL de mídia = bruto × 1,1215 (imposto 12,15% sobre veiculação)
  - KPI financeiro do cliente usa a PLATAFORMA de pagamento; KPI de campanha
    usa o PIXEL, sempre com rótulo explícito (ver reconciliar())
  - Amostra insuficiente não recebe cor de semáforo (evita decisão em ruído)
"""

from datetime import date, datetime, timedelta

# ── Imposto sobre mídia (mesma fonte de verdade do resto do sistema) ──────────
try:
    from modules.meta_client import META_TAX_RATE
except ImportError:  # uso standalone (testes/PDF)
    META_TAX_RATE = 0.1215

MEDIA_TAX_MULTIPLIER = 1.0 + META_TAX_RATE


# ── Config padrão (todos os valores são sobrescrevíveis) ──────────────────────

DEFAULT_CONFIG = {
    'nome': 'Lançamento',
    'expert': '',
    'edicao': '',
    'ad_account_id': '',
    'campaign_patterns': [],      # ex: ['[LP11] [AGO26]'] — só campanhas que casam
    'datas': {
        'inicio_venda_ingresso': None,   # 'YYYY-MM-DD'
        'evento': None,
        'abertura_carrinho': None,
        'fechamento_carrinho': None,
    },
    'precos': {'ingresso': None, 'principal': None},
    'metas': {'ingressos': None, 'investimento': None, 'faturamento': None},
    'alvos': {'cpa_ingresso': None, 'roas': None},
    'taxa_comparecimento_esperada': None,
    'imposto_midia': META_TAX_RATE,
    'timezone': 'America/Sao_Paulo',
    # Seção 6 — thresholds por métrica, editáveis
    'benchmarks': {
        'cpa_ingresso':   {'alvo': None, 'atencao': 0.15, 'critico': 0.30, 'direcao': 'menor_melhor'},
        'roas':           {'alvo': None, 'atencao': 0.15, 'critico': 0.30, 'direcao': 'maior_melhor'},
        'ctr':            {'alvo': None, 'atencao': 0.20, 'critico': 0.40, 'direcao': 'maior_melhor'},
        'cpm':            {'alvo': None, 'atencao': 0.25, 'critico': 0.50, 'direcao': 'menor_melhor'},
        'taxa_checkout':  {'alvo': None, 'atencao': 0.20, 'critico': 0.40, 'direcao': 'maior_melhor'},
        'taxa_compra':    {'alvo': None, 'atencao': 0.20, 'critico': 0.40, 'direcao': 'maior_melhor'},
    },
    # Amostra mínima para colorir semáforo (brief seção 6)
    'amostra_minima': {'impressoes': 500, 'conversoes': 3},
}


def merge_config(user_config=None):
    """Mescla a config do usuário sobre os defaults (deep merge de 1 nível)."""
    cfg = {k: (dict(v) if isinstance(v, dict) else v) for k, v in DEFAULT_CONFIG.items()}
    for k, v in (user_config or {}).items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            cfg[k] = {**cfg[k], **v}
        else:
            cfg[k] = v
    return cfg


# ── Helpers ──────────────────────────────────────────────────────────────────

def _d(value):
    """'YYYY-MM-DD' | date | datetime → date | None."""
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value)[:10], '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return None


def _num(v):
    """Aceita 'R$1.234,56 BRL', '1427', 4.46, None → float."""
    if v is None or v == '':
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v)
    for token in ('R$', 'BRL', ' ', ' ', '%'):
        s = s.replace(token, '')
    s = s.strip()
    if not s:
        return 0.0
    if ',' in s and '.' in s:      # 1.234,56
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:                  # 1234,56
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0


def _div(a, b):
    """Divisão segura: denominador 0/None → None (nunca exibir zero falso)."""
    a, b = _num(a), _num(b)
    return (a / b) if b else None


# ── Normalização (entrada: Meta MCP/API, CSV do Ads Manager ou planilha) ──────

# Aliases aceitos por campo → cobre MCP, CSV export e nomes já usados no projeto
FIELD_ALIASES = {
    'data':        ('date_start', 'data', 'Dia', 'Day', 'date'),
    'campanha':    ('name', 'campaign_name', 'campanha', 'Nome da campanha'),
    'campaign_id': ('id', 'campaign_id'),
    'adset':       ('adset_name', 'adset', 'Nome do conjunto de anúncios'),
    'anuncio':     ('ad_name', 'anuncio', 'Nome do anúncio'),
    'ad_id':       ('ad_id',),
    'investimento':('amount_spent', 'spend', 'investimento', 'Valor usado (BRL)'),
    'impressoes':  ('impressions', 'impressoes', 'Impressões'),
    'alcance':     ('reach', 'alcance', 'Alcance'),
    'cliques_link':('actions:link_click', 'inline_link_clicks', 'link_clicks', 'cliques_link',
                    'Cliques no link'),
    'lpv':         ('omni_landing_page_view', 'landing_page_view', 'lpv',
                    'Visualizações da página de destino'),
    'checkouts':   ('omni_initiated_checkout', 'initiate_checkout', 'checkouts',
                    'Finalizações de compra iniciadas'),
    'compras_pixel':('offsite_conversion_fb_pixel_purchase', 'actions:omni_purchase',
                     'compras_pixel', 'Compras'),
    'valor_pixel': ('offsite_conversion_fb_pixel_purchase_values', 'omni_purchase_values',
                    'valor_pixel', 'Valor de conversão de compras'),
}


def normalize_rows(raw_rows):
    """Converte linhas heterogêneas (MCP / CSV / planilha) no schema spend_daily.

    Saída por linha:
      {data, campanha, campaign_id, investimento, impressoes, alcance,
       cliques_link, lpv, checkouts, compras_pixel, valor_pixel}
    Linhas sem data utilizável são descartadas (não viram zero silencioso).
    """
    out = []
    for r in (raw_rows or []):
        row = {}
        for campo, aliases in FIELD_ALIASES.items():
            valor = None
            for a in aliases:
                if a in r and r[a] not in (None, ''):
                    valor = r[a]
                    break
            row[campo] = valor
        d = _d(row['data'])
        if d is None:
            continue
        out.append({
            'data':          d.isoformat(),
            'campanha':      row['campanha'] or '',
            'campaign_id':   str(row['campaign_id'] or ''),
            'adset':         row.get('adset') or '',
            'anuncio':       row.get('anuncio') or '',
            'ad_id':         str(row.get('ad_id') or ''),
            'investimento':  _num(row['investimento']),
            'impressoes':    int(_num(row['impressoes'])),
            'alcance':       int(_num(row['alcance'])),
            'cliques_link':  int(_num(row['cliques_link'])),
            'lpv':           int(_num(row['lpv'])),
            'checkouts':     int(_num(row['checkouts'])),
            'compras_pixel': int(_num(row['compras_pixel'])),
            'valor_pixel':   _num(row['valor_pixel']),
        })
    out.sort(key=lambda x: x['data'])
    return out


def filter_by_patterns(rows, patterns):
    """Mantém só campanhas cujo nome contém algum dos padrões (case-insensitive).

    Inverso do excluded_campaign_patterns usado nas outras dashs: aqui o
    lançamento é definido por INCLUSÃO (ex: '[LP11] [AGO26]').
    """
    if not patterns:
        return rows
    pats = [p.lower() for p in patterns if p]
    return [r for r in rows if any(p in (r.get('campanha') or '').lower() for p in pats)]


# ── Agregação e métricas (brief seção 5) ─────────────────────────────────────

def aggregate(rows):
    """Soma as linhas diárias em totais brutos."""
    t = {'investimento': 0.0, 'impressoes': 0, 'alcance': 0, 'cliques_link': 0,
         'lpv': 0, 'checkouts': 0, 'compras_pixel': 0, 'valor_pixel': 0.0}
    for r in rows:
        for k in t:
            t[k] += r.get(k, 0) or 0
    t['investimento'] = round(t['investimento'], 2)
    t['valor_pixel'] = round(t['valor_pixel'], 2)
    return t


def compute_metrics(rows, vendas_plataforma=None, config=None):
    """Aplica o dicionário de métricas do brief sobre as linhas normalizadas.

    vendas_plataforma: dict opcional da fonte da verdade financeira
        {'ingressos': int, 'faturamento_ingresso': float,
         'vendas_principal': int, 'faturamento_principal': float}
    Retorna dict com blocos: totais, custos, funil, taxas, financeiro.
    """
    cfg = merge_config(config)
    tot = aggregate(rows)

    inv_bruto = tot['investimento']
    imposto = cfg.get('imposto_midia', META_TAX_RATE)
    custo_real = round(inv_bruto * (1.0 + imposto), 2)

    vp = vendas_plataforma or {}
    ingressos = vp.get('ingressos')
    fat_ingresso = vp.get('faturamento_ingresso')
    vendas_princ = vp.get('vendas_principal')
    fat_princ = vp.get('faturamento_principal')

    # Fonte da verdade para CPA/ROAS: plataforma quando existe, senão pixel (rotulado)
    tem_plataforma = ingressos is not None
    ingressos_efetivo = ingressos if tem_plataforma else tot['compras_pixel']
    fat_efetivo = fat_ingresso if fat_ingresso is not None else tot['valor_pixel']
    fonte_financeiro = 'plataforma' if tem_plataforma else 'pixel'

    receita_total = (fat_efetivo or 0) + (fat_princ or 0)

    return {
        'totais': tot,
        'custos': {
            'investimento_bruto': inv_bruto,
            'custo_real_midia':   custo_real,
            'imposto_valor':      round(custo_real - inv_bruto, 2),
            'imposto_aliquota':   imposto,
        },
        'funil': {
            'impressoes':    tot['impressoes'],
            'cliques_link':  tot['cliques_link'],
            'lpv':           tot['lpv'],
            'checkouts':     tot['checkouts'],
            'compras':       ingressos_efetivo,
        },
        'custos_unitarios': {
            'cpm':          _round(_mult(_div(inv_bruto, tot['impressoes']), 1000)),
            'cpc_link':     _round(_div(inv_bruto, tot['cliques_link'])),
            'cp_lpv':       _round(_div(inv_bruto, tot['lpv'])),
            'cp_checkout':  _round(_div(inv_bruto, tot['checkouts'])),
            'cpa_ingresso': _round(_div(inv_bruto, ingressos_efetivo)),
        },
        # Taxas de passagem entre as etapas do funil. O nome de cada taxa é o
        # da PASSAGEM (etapa origem → destino), como aparece na tela:
        #   Impressões —ctr→ Cliques —connect_rate→ Page View
        #   Page View —lp_checkout→ Checkout —taxa_compra→ Vendas
        'taxas': {
            'ctr':             _round(_mult(_div(tot['cliques_link'], tot['impressoes']), 100)),
            'connect_rate':    _round(_mult(_div(tot['lpv'], tot['cliques_link']), 100)),
            'lp_checkout':     _round(_mult(_div(tot['checkouts'], tot['lpv']), 100)),
            'taxa_checkout':   _round(_mult(_div(tot['checkouts'], tot['lpv']), 100)),  # alias
            'taxa_compra':     _round(_mult(_div(ingressos_efetivo, tot['checkouts']), 100)),
            'conversao_total': _round(_mult(_div(ingressos_efetivo, tot['cliques_link']), 100)),
        },
        # Funil pronto para render: 5 etapas, cada uma com volume, custo
        # unitário e a taxa de passagem para a etapa seguinte (badge diagonal).
        'funil_etapas': [
            {'etapa': 'Impressões',    'volume': tot['impressoes'],
             'custo_label': 'CPM',         'custo': _round(_mult(_div(inv_bruto, tot['impressoes']), 1000)),
             'taxa_label': 'CTR',          'taxa': _round(_mult(_div(tot['cliques_link'], tot['impressoes']), 100))},
            {'etapa': 'Cliques no link', 'volume': tot['cliques_link'],
             'custo_label': 'CPC',         'custo': _round(_div(inv_bruto, tot['cliques_link'])),
             'taxa_label': 'Connect rate', 'taxa': _round(_mult(_div(tot['lpv'], tot['cliques_link']), 100))},
            {'etapa': 'Page View',       'volume': tot['lpv'],
             'custo_label': 'CP LPV',      'custo': _round(_div(inv_bruto, tot['lpv'])),
             'taxa_label': 'LP → Checkout','taxa': _round(_mult(_div(tot['checkouts'], tot['lpv']), 100))},
            {'etapa': 'Checkout',        'volume': tot['checkouts'],
             'custo_label': 'CP Checkout', 'custo': _round(_div(inv_bruto, tot['checkouts'])),
             'taxa_label': 'Checkout → Venda', 'taxa': _round(_mult(_div(ingressos_efetivo, tot['checkouts']), 100))},
            {'etapa': 'Vendas',          'volume': ingressos_efetivo,
             'custo_label': 'CPA',         'custo': _round(_div(inv_bruto, ingressos_efetivo)),
             'taxa_label': None,           'taxa': None},
        ],
        'financeiro': {
            'fonte':                 fonte_financeiro,
            'ingressos':             ingressos_efetivo,
            'faturamento_ingresso':  _round(fat_efetivo),
            'vendas_principal':      vendas_princ,
            'faturamento_principal': _round(fat_princ),
            'receita_total':         _round(receita_total),
            'ticket_medio':          _round(_div(fat_efetivo, ingressos_efetivo)),
            'roas_fase1':            _round(_div(fat_efetivo, inv_bruto)),
            'roas_lancamento':       _round(_div(receita_total, inv_bruto)),
            'margem':                _round(_div(receita_total - custo_real, receita_total)),
        },
        'reconciliacao': reconciliar(tot['compras_pixel'], ingressos),
    }


def _mult(v, f):
    return None if v is None else v * f


def _round(v, casas=2):
    return None if v is None else round(float(v), casas)


# ── Reconciliação pixel × plataforma (brief seção 2) ─────────────────────────

def reconciliar(compras_pixel, vendas_plataforma, limite=0.15):
    """Divergência = (pixel − plataforma) / plataforma. Alerta acima de ±15%."""
    if vendas_plataforma is None:
        return {'pixel': compras_pixel, 'plataforma': None,
                'divergencia': None, 'alerta': False,
                'nota': 'Sem dados da plataforma — números financeiros usam o pixel.'}
    div = _div(compras_pixel - vendas_plataforma, vendas_plataforma)
    return {
        'pixel': compras_pixel,
        'plataforma': vendas_plataforma,
        'divergencia': _round(_mult(div, 100)) if div is not None else None,
        'alerta': bool(div is not None and abs(div) > limite),
        'limite_pct': limite * 100,
    }


# ── Fases do lançamento (brief seção 3) ──────────────────────────────────────

FASES = [
    ('venda_ingresso', 'Venda de ingresso'),
    ('confirmacao',    'Confirmação'),
    ('evento',         'Evento'),
    ('carrinho',       'Carrinho aberto'),
    ('fechamento',     'Fechamento'),
]

KPI_HEROI_POR_FASE = {
    'venda_ingresso': 'cpa_ingresso',
    'confirmacao':    'cp_confirmacao',
    'evento':         'comparecimento',
    'carrinho':       'roas_lancamento',
    'fechamento':     'roas_lancamento',
}


def fase_atual(config, hoje=None):
    """Determina a fase do lançamento na data de referência.

    Só usa as datas que existirem na config — sem data de evento, o
    lançamento permanece em 'venda_ingresso' (degradação graciosa).
    """
    cfg = merge_config(config)
    dts = cfg['datas']
    hoje = _d(hoje) or date.today()

    inicio   = _d(dts.get('inicio_venda_ingresso'))
    evento   = _d(dts.get('evento'))
    abertura = _d(dts.get('abertura_carrinho'))
    fecha    = _d(dts.get('fechamento_carrinho'))

    if fecha and hoje > fecha:
        fase = 'fechamento'
    elif abertura and hoje >= abertura:
        fase = 'carrinho'
    elif evento and hoje == evento:
        fase = 'evento'
    elif evento and (evento - hoje).days <= 3 and hoje < evento:
        fase = 'confirmacao'
    else:
        fase = 'venda_ingresso'

    dias_para_evento = (evento - hoje).days if evento else None
    dia_do_lancamento = ((hoje - inicio).days + 1) if inicio else None

    return {
        'fase': fase,
        'label': dict(FASES)[fase],
        'kpi_heroi': KPI_HEROI_POR_FASE[fase],
        'dia_do_lancamento': dia_do_lancamento,
        'dias_para_evento': dias_para_evento,
        'datas_configuradas': {k: (str(_d(v)) if _d(v) else None) for k, v in dts.items()},
    }


# ── Meta, ritmo e projeção (brief seção 4-D) ─────────────────────────────────

def calcular_meta(rows, config, hoje=None, campo='compras_pixel', realizado=None):
    """Progresso da meta + ritmo atual, ritmo necessário e projeção."""
    cfg = merge_config(config)
    meta = (cfg.get('metas') or {}).get('ingressos')
    hoje = _d(hoje) or date.today()

    atual = realizado if realizado is not None else sum(r.get(campo, 0) for r in rows)

    # Ritmo atual = média dos últimos 3 dias com dados
    ult3 = rows[-3:] if len(rows) >= 3 else rows
    ritmo_atual = _div(sum(r.get(campo, 0) for r in ult3), len(ult3)) if ult3 else None

    prazo = _d((cfg.get('datas') or {}).get('evento')) or \
            _d((cfg.get('datas') or {}).get('fechamento_carrinho'))
    dias_restantes = max((prazo - hoje).days, 0) if prazo else None

    ritmo_necessario = None
    projecao = None
    if meta:
        if dias_restantes:
            ritmo_necessario = _round(_div(meta - atual, dias_restantes))
        if ritmo_atual is not None and dias_restantes is not None:
            projecao = int(round(atual + ritmo_atual * dias_restantes))

    return {
        'meta': meta,
        'atual': atual,
        'pct': _round(_mult(_div(atual, meta), 100)) if meta else None,
        'ritmo_atual': _round(ritmo_atual),
        'ritmo_necessario': ritmo_necessario,
        'dias_restantes': dias_restantes,
        'projecao': projecao,
        'projecao_vs_meta_pct': _round(_mult(_div(projecao - meta, meta), 100)) if (meta and projecao is not None) else None,
    }


# ── Semáforo (brief seção 6) ─────────────────────────────────────────────────

def semaforo(metrica, valor, config, amostra=None):
    """Verde / âmbar / vermelho / cinza conforme benchmark configurado.

    Cinza = amostra insuficiente OU alvo não configurado → nunca colorir ruído.
    """
    cfg = merge_config(config)
    bm = (cfg.get('benchmarks') or {}).get(metrica)
    minimo = cfg.get('amostra_minima') or {}

    if amostra is not None:
        if (amostra.get('impressoes', 0) < minimo.get('impressoes', 0) and
                amostra.get('conversoes', 0) < minimo.get('conversoes', 0)):
            return {'cor': 'cinza', 'motivo': 'amostra insuficiente'}

    if not bm or bm.get('alvo') in (None, '') or valor is None:
        return {'cor': 'cinza', 'motivo': 'sem alvo configurado'}

    alvo = float(bm['alvo'])
    if alvo == 0:
        return {'cor': 'cinza', 'motivo': 'alvo inválido'}
    desvio = (float(valor) - alvo) / alvo
    if bm.get('direcao') == 'menor_melhor':
        desvio = -desvio  # abaixo do alvo é bom

    if desvio >= -bm.get('atencao', 0.15):
        return {'cor': 'verde', 'desvio_pct': _round(desvio * 100)}
    if desvio >= -bm.get('critico', 0.30):
        return {'cor': 'ambar', 'desvio_pct': _round(desvio * 100)}
    return {'cor': 'vermelho', 'desvio_pct': _round(desvio * 100)}


# ── Série diária pronta para gráfico ─────────────────────────────────────────

def serie_diaria(rows, config=None, vendas_por_dia=None):
    """Uma linha por dia com todas as métricas derivadas.

    Alimenta a curva diária (bloco E) e a tabela "Dados diários" (bloco 4.1):
      data · investimento · vendas · CPA · CPM · CTR · connect rate ·
      LP→checkout % · checkout→venda %

    vendas_por_dia: dict opcional {'YYYY-MM-DD': {'vendas': int, 'faturamento': float}}
        vindo da plataforma de pagamento (fonte da verdade). Quando ausente,
        a coluna 'vendas' usa o pixel — e 'fonte_vendas' diz qual foi usada,
        para a tabela poder rotular (o brief exige rótulo quando há ambiguidade).
    """
    cfg = merge_config(config)
    imposto = cfg.get('imposto_midia', META_TAX_RATE)
    vpd = vendas_por_dia or {}

    por_dia = {}
    for r in rows:
        d = r['data']
        e = por_dia.setdefault(d, {'data': d, 'investimento': 0.0, 'impressoes': 0,
                                   'cliques_link': 0, 'lpv': 0, 'checkouts': 0,
                                   'compras_pixel': 0, 'valor_pixel': 0.0})
        for k in ('investimento', 'impressoes', 'cliques_link', 'lpv',
                  'checkouts', 'compras_pixel', 'valor_pixel'):
            e[k] += r.get(k, 0) or 0

    # Dias que só existem na plataforma (venda sem veiculação) também entram
    for d in vpd:
        por_dia.setdefault(d, {'data': d, 'investimento': 0.0, 'impressoes': 0,
                               'cliques_link': 0, 'lpv': 0, 'checkouts': 0,
                               'compras_pixel': 0, 'valor_pixel': 0.0})

    saida = []
    for d in sorted(por_dia):
        e = por_dia[d]
        inv = e['investimento']
        # Se a plataforma foi fornecida, ela é a fonte da verdade para TODOS os
        # dias — um dia ausente significa "nenhuma venda", não "usar o pixel".
        # Misturar as duas fontes na mesma coluna produziria um total incoerente.
        plat = vpd.get(d) or {}
        usa_plataforma = bool(vendas_por_dia)
        vendas = plat.get('vendas', 0) if usa_plataforma else e['compras_pixel']
        faturamento = plat.get('faturamento', 0.0) if usa_plataforma else e['valor_pixel']
        tem_plat = usa_plataforma

        saida.append({
            **e,
            'investimento':     round(inv, 2),
            'valor_pixel':      round(e['valor_pixel'], 2),
            'custo_real_midia': round(inv * (1.0 + imposto), 2),
            'vendas':           vendas,
            'fonte_vendas':     'plataforma' if tem_plat else 'pixel',
            'faturamento':      _round(faturamento),
            # Custos unitários
            'cpa':  _round(_div(inv, vendas)),
            'cpm':  _round(_mult(_div(inv, e['impressoes']), 1000)),
            'cpc':  _round(_div(inv, e['cliques_link'])),
            # Taxas do funil (mesma sequência da visão consolidada)
            'ctr':            _round(_mult(_div(e['cliques_link'], e['impressoes']), 100)),
            'connect_rate':   _round(_mult(_div(e['lpv'], e['cliques_link']), 100)),
            'lp_checkout':    _round(_mult(_div(e['checkouts'], e['lpv']), 100)),
            'checkout_venda': _round(_mult(_div(vendas, e['checkouts']), 100)),
            'roas': _round(_div(faturamento, inv)),
        })
    return saida


def totais_serie(serie, config=None):
    """Linha de TOTAL da tabela diária — taxas recalculadas sobre os totais,
    nunca média das médias (que distorceria)."""
    cfg = merge_config(config)
    imposto = cfg.get('imposto_midia', META_TAX_RATE)
    t = {k: 0 for k in ('investimento', 'impressoes', 'cliques_link', 'lpv',
                        'checkouts', 'vendas', 'faturamento')}
    for r in serie:
        for k in t:
            t[k] += r.get(k, 0) or 0
    inv = t['investimento']
    return {
        **t,
        'investimento':     round(inv, 2),
        'faturamento':      round(t['faturamento'], 2),
        'custo_real_midia': round(inv * (1.0 + imposto), 2),
        'cpa':  _round(_div(inv, t['vendas'])),
        'cpm':  _round(_mult(_div(inv, t['impressoes']), 1000)),
        'cpc':  _round(_div(inv, t['cliques_link'])),
        'ctr':            _round(_mult(_div(t['cliques_link'], t['impressoes']), 100)),
        'connect_rate':   _round(_mult(_div(t['lpv'], t['cliques_link']), 100)),
        'lp_checkout':    _round(_mult(_div(t['checkouts'], t['lpv']), 100)),
        'checkout_venda': _round(_mult(_div(t['vendas'], t['checkouts']), 100)),
        'roas': _round(_div(t['faturamento'], inv)),
    }
