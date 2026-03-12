"""
Dashboard de Cruzamento: Facebook Ads × Google Sheets (MQLs + Wons)

Fluxo:
  1. Fetch paralelo: FB Ads API + Google Sheets (abas MQLs e Wons)
  2. Join 1: MQLs × Wons por Deal ID  → lead enriquecido com dados de venda
  3. Join 2: leads enriquecidos × FB  → agrupados por campanha > conjunto > ad usando UTMs

Autenticação Google: Service Account JSON (GOOGLE_CREDENTIALS_FILE)
"""

import os
import json
import time
import requests
import concurrent.futures
from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify, render_template, request, session, redirect, url_for

# ── Blueprint ─────────────────────────────────────────────────────────────────
cruzamento_bp = Blueprint('cruzamento', __name__)

# ── Configuração Hardcoded (via .env) ─────────────────────────────────────────
APP_ID              = os.getenv('APP_ID')
APP_SECRET          = os.getenv('APP_SECRET')
AD_ACCOUNT_ID       = os.getenv('CRUZAMENTO_AD_ACCOUNT_ID', 'act_2023939324650844')
SPREADSHEET_ID      = os.getenv('CRUZAMENTO_SPREADSHEET_ID', '1m6syDzMDZqB44ZTKaRj5t79HUDuyEqaN2RgAo0kpECc')
GOOGLE_CREDS_FILE   = os.getenv('GOOGLE_CREDENTIALS_FILE', 'google_credentials.json')

# Nomes das abas na planilha
ABA_MQLS  = 'MQLs'
ABA_WONS  = 'Wons'

# Normaliza strings para join case-insensitive
def _norm(s):
    return str(s).strip().lower() if s else ''

# ── Filtro de Datas ───────────────────────────────────────────────────────────
from datetime import date as _date

def preset_to_dates(preset, since_str=None, until_str=None):
    """Converte date_preset do FB em (since: date, until: date)."""
    today = _date.today()
    if since_str and until_str:
        try:
            return (datetime.strptime(since_str, '%Y-%m-%d').date(),
                    datetime.strptime(until_str, '%Y-%m-%d').date())
        except ValueError:
            pass
    mapping = {
        'last_7_days':  (today - timedelta(days=6),  today),
        'last_14_days': (today - timedelta(days=13), today),
        'last_30_days': (today - timedelta(days=29), today),
        'last_90_days': (today - timedelta(days=89), today),
        'yesterday':    (today - timedelta(days=1),  today - timedelta(days=1)),
        'today':        (today, today),
    }
    if preset in mapping:
        return mapping[preset]
    if preset == 'this_month':
        return (today.replace(day=1), today)
    if preset == 'last_month':
        first_this = today.replace(day=1)
        last_prev  = first_this - timedelta(days=1)
        return (last_prev.replace(day=1), last_prev)
    return (None, None)


def _parse_date_br(s):
    """Tenta parsear DD/MM/YYYY, DD/MM/YY ou YYYY-MM-DD."""
    s = str(s).strip()
    for fmt in ('%d/%m/%Y', '%d/%m/%y', '%Y-%m-%d', '%Y/%m/%d'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def filter_rows_by_date(rows, date_col, since, until):
    """Filtra linhas mantendo apenas as com date_col entre since e until."""
    if since is None and until is None:
        return rows
    result = []
    for row in rows:
        d = _parse_date_br(row.get(date_col, ''))
        if d is None:
            continue
        if since and d < since:
            continue
        if until and d > until:
            continue
        result.append(row)
    return result

# ── Google Auth via Service Account ───────────────────────────────────────────
_google_token_cache = {'token': None, 'expires_at': 0}

def _get_google_token():
    """Obtém token de acesso Google via Service Account JWT (sem deps externas)."""
    import base64, hashlib, hmac, struct

    now = time.time()
    if _google_token_cache['token'] and now < _google_token_cache['expires_at'] - 30:
        return _google_token_cache['token']

    creds_path = GOOGLE_CREDS_FILE
    if not os.path.exists(creds_path):
        raise FileNotFoundError(
            f"Arquivo de credenciais não encontrado: {creds_path}\n"
            "Coloque o google_credentials.json na raiz do projeto."
        )

    with open(creds_path, 'r') as f:
        creds = json.load(f)

    # Montar JWT manualmente para evitar dependência da lib google-auth
    try:
        from cryptography.hazmat.primitives import serialization, hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.backends import default_backend
    except ImportError:
        raise ImportError(
            "Instale: pip install cryptography\n"
            "Esta lib já é incluída pelo google-auth mas pode ser instalada sozinha."
        )

    header = {"alg": "RS256", "typ": "JWT"}
    iat = int(time.time())
    exp = iat + 3600
    payload = {
        "iss": creds['client_email'],
        "scope": "https://www.googleapis.com/auth/spreadsheets.readonly",
        "aud": "https://oauth2.googleapis.com/token",
        "iat": iat,
        "exp": exp,
    }

    def b64url(data):
        if isinstance(data, dict):
            data = json.dumps(data, separators=(',', ':')).encode()
        return base64.urlsafe_b64encode(data).rstrip(b'=').decode()

    header_b64  = b64url(header)
    payload_b64 = b64url(payload)
    signing_input = f"{header_b64}.{payload_b64}".encode()

    private_key = serialization.load_pem_private_key(
        creds['private_key'].encode(), password=None, backend=default_backend()
    )
    signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    jwt_token = f"{header_b64}.{payload_b64}.{b64url(signature)}"

    resp = requests.post(
        'https://oauth2.googleapis.com/token',
        data={
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': jwt_token,
        },
        timeout=15
    )
    resp.raise_for_status()
    data = resp.json()

    _google_token_cache['token'] = data['access_token']
    _google_token_cache['expires_at'] = time.time() + data.get('expires_in', 3600)
    return _google_token_cache['token']

# ── Fetch Google Sheets ────────────────────────────────────────────────────────
def fetch_sheets_data(spreadsheet_id):
    """
    Lê as abas MQLs e Wons em uma única chamada batchGet.
    Retorna (mqls_rows: list[dict], wons_rows: list[dict])
    """
    token = _get_google_token()
    ranges = [f"'{ABA_MQLS}'!A:Z", f"'{ABA_WONS}'!A:Z"]
    url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values:batchGet"
        f"?ranges={'&ranges='.join(ranges)}"
    )
    resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    def parse_range(value_range):
        values = value_range.get('values', [])
        if not values:
            return []
        headers = [h.strip() for h in values[0]]
        rows = []
        for row in values[1:]:
            # Preenche colunas faltantes com ''
            padded = row + [''] * (len(headers) - len(row))
            rows.append(dict(zip(headers, padded)))
        return rows

    mqls_rows = parse_range(data['valueRanges'][0])
    wons_rows = parse_range(data['valueRanges'][1])
    return mqls_rows, wons_rows

# ── Fetch Facebook Ads ────────────────────────────────────────────────────────
def fetch_fb_insights(account_id, access_token, date_preset='last_30d', since=None, until=None):
    """
    Busca insights a nível de Ad com campos hierárquicos (campaign/adset).
    Retorna lista de dicts com: campaign_id, campaign_name, adset_id, adset_name,
                                ad_id, ad_name, spend, impressions, clicks
    """
    base_url = f"https://graph.facebook.com/v22.0/{account_id}/insights"

    params = {
        'access_token': access_token,
        'level': 'ad',
        'fields': 'campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,clicks',
        'limit': 500,
    }

    if since and until:
        params['time_range'] = json.dumps({'since': since, 'until': until})
    else:
        params['date_preset'] = date_preset

    ads = []
    url = base_url
    while url:
        resp = requests.get(url, params=params if url == base_url else None, timeout=30)
        resp.raise_for_status()
        body = resp.json()

        for item in body.get('data', []):
            ads.append({
                'campaign_id':   item.get('campaign_id', ''),
                'campaign_name': item.get('campaign_name', ''),
                'adset_id':      item.get('adset_id', ''),
                'adset_name':    item.get('adset_name', ''),
                'ad_id':         item.get('ad_id', ''),
                'ad_name':       item.get('ad_name', ''),
                'spend':         float(item.get('spend', 0) or 0),
                'impressions':   int(item.get('impressions', 0) or 0),
                'clicks':        int(item.get('clicks', 0) or 0),
            })

        # Paginação cursor
        paging = body.get('paging', {})
        next_url = paging.get('next')
        url = next_url if next_url else None
        params = None  # next_url já tem params embutidos

    return ads

# ── Processamento: Duplo Join em Memória ──────────────────────────────────────
def processar_cruzamento(fb_ads, mqls_rows, wons_rows):
    """
    Passo 1: MQLs × Wons por 'Deal ID'  → enriquece cada lead com venda
    Passo 2: leads × FB ads por utm_content (norm) ↔ ad_name (norm)
             fallback: utm_campaign ↔ campaign_name

    Retorna hierarquia:
    [
      {
        campaign_id, campaign_name, spend, impressions, clicks,
        leads_total, leads_a, leads_b, vendas_a, vendas_b,
        fat_a, fat_b, fat_total, lucro, cpl, cpl_a, cpl_b,
        adsets: [
          { ...mesmas métricas..., ads: [ ...mesmas métricas... ] }
        ]
      }
    ]
    """

    # ── Passo 1: Join Wons → indexar por Deal ID ──────────────────────────────
    # Mapeamento: deal_id → { produto, valor }
    # Na aba Wons: colunas 'Deal ID', 'Produto', 'Valor'
    wons_idx = {}
    for row in wons_rows:
        deal_id = _norm(row.get('Deal ID', ''))
        if deal_id:
            wons_idx[deal_id] = {
                'produto_won': _norm(row.get('Produto', '')),
                'valor':       _parse_valor(row.get('Valor', 0)),
            }

    # ── Enriquecer MQLs ───────────────────────────────────────────────────────
    leads_enriquecidos = []
    for row in mqls_rows:
        deal_id = _norm(row.get('Deal ID', ''))
        produto = _norm(row.get('Produto indicado', ''))  # Aba MQLs
        won_data = wons_idx.get(deal_id, {})

        # Determinar produto A ou B (flexível: verifica letra/número no nome)
        is_a = _is_produto_a(produto)

        lead = {
            'deal_id':       deal_id,
            'produto':       produto,
            'is_a':          is_a,
            'utm_campaign':  _norm(row.get('utm_campaign', '')),
            'utm_content':   _norm(row.get('utm_content', '')),
            'utm_medium':    _norm(row.get('utm_medium', '')),
            'utm_source':    _norm(row.get('utm_source', '')),
            'utm_term':      _norm(row.get('utm_term', '')),
            'vendeu':        bool(won_data),
            'valor_venda':   won_data.get('valor', 0.0),
        }
        leads_enriquecidos.append(lead)

    # ── Passo 2: Indexar leads por utm_content e utm_campaign ─────────────────
    # Estrutura: { utm_content_norm: [leads] }
    leads_by_content  = {}
    leads_by_campaign = {}

    for lead in leads_enriquecidos:
        uc = lead['utm_content']
        if uc:
            leads_by_content.setdefault(uc, []).append(lead)
        uc2 = lead['utm_campaign']
        if uc2:
            leads_by_campaign.setdefault(uc2, []).append(lead)

    # ── Passo 2: Cruzar com FB Ads ────────────────────────────────────────────
    # Hierarquia: campanha → conjunto → ad
    campaigns = {}   # campaign_id → {...}
    adsets    = {}   # adset_id    → {...}
    ads_map   = {}   # ad_id       → {...}

    for ad in fb_ads:
        cid = ad['campaign_id']
        sid = ad['adset_id']
        aid = ad['ad_id']

        ad_name_norm      = _norm(ad['ad_name'])
        campaign_name_norm = _norm(ad['campaign_name'])

        # Tentar match por utm_content → ad_name (principal)
        matched_leads = leads_by_content.get(ad_name_norm, [])

        # Fallback: utm_campaign → campaign_name (se não houver match)
        if not matched_leads:
            matched_leads = leads_by_campaign.get(campaign_name_norm, [])

        metrics = _calc_metrics(ad, matched_leads)

        # Acumular no nível Ad
        ads_map[aid] = {
            'ad_id':   aid,
            'ad_name': ad['ad_name'],
            **metrics,
            'adset_id': sid,
        }

        # Acumular no nível AdSet
        if sid not in adsets:
            adsets[sid] = {
                'adset_id':   sid,
                'adset_name': ad['adset_name'],
                'campaign_id': cid,
                **_empty_metrics(),
                'ads': [],
            }
        _accumulate(adsets[sid], metrics)
        adsets[sid]['ads'].append(ads_map[aid])

        # Acumular no nível Campanha
        if cid not in campaigns:
            campaigns[cid] = {
                'campaign_id':   cid,
                'campaign_name': ad['campaign_name'],
                **_empty_metrics(),
                'adsets': [],
            }
        _accumulate(campaigns[cid], metrics)

    # Montar adsets nas campanhas
    adset_by_campaign = {}
    for sid, adset in adsets.items():
        cid = adset.pop('campaign_id')
        adset_by_campaign.setdefault(cid, []).append(adset)

    result = []
    for cid, camp in campaigns.items():
        camp['adsets'] = adset_by_campaign.get(cid, [])
        _calc_derived(camp)
        for adset in camp['adsets']:
            _calc_derived(adset)
            for ad_item in adset.get('ads', []):
                _calc_derived(ad_item)
        result.append(camp)

    # Ordenar por spend desc
    result.sort(key=lambda x: x['spend'], reverse=True)

    # Leads sem match (sem UTM ou UTM não encontrada no FB)
    matched_deal_ids = set()
    for ad_item in ads_map.values():
        for lead in leads_enriquecidos:
            if _norm(lead['utm_content']) == _norm(ad_item.get('ad_name', '')) or \
               _norm(lead['utm_campaign']) == _norm(ad_item.get('ad_name', '')):
                matched_deal_ids.add(lead['deal_id'])

    organicos = [l for l in leads_enriquecidos if l['deal_id'] not in matched_deal_ids]
    organico_metrics = _calc_organic_metrics(organicos)

    # Somar faturamento total do Sheets (Wons filtradas)
    fat_total_sheets = sum(_parse_valor(row.get('Valor', 0)) for row in wons_rows)

    return {
        'campaigns': result,
        'organicos': organico_metrics,
        'total_leads': len(leads_enriquecidos),
        'total_mqls': len(mqls_rows),
        'total_wons': len(wons_rows),
        'fat_total_sheets': round(fat_total_sheets, 2),
    }

# ── Helpers ───────────────────────────────────────────────────────────────────
def _is_produto_a(produto_str):
    """Determina se o produto é A (True) ou B (False)."""
    s = produto_str.lower()
    # Heurística flexível: se contiver 'a' como palavra ou 'produto a'
    # Ajuste conforme os nomes reais dos seus produtos
    if 'produto a' in s or ' a ' in s or s.endswith(' a') or s.startswith('a '):
        return True
    return False

def _parse_valor(v):
    """Converte string de valor para float (trata R$ e vírgulas)."""
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace('R$', '').replace(' ', '').replace('.', '').replace(',', '.')
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0

def _empty_metrics():
    return {
        'spend': 0.0, 'impressions': 0, 'clicks': 0,
        'leads_total': 0, 'leads_a': 0, 'leads_b': 0,
        'vendas_a': 0, 'vendas_b': 0,
        'fat_a': 0.0, 'fat_b': 0.0, 'fat_total': 0.0,
    }

def _calc_metrics(ad, leads):
    """Calcula métricas para um ad + lista de leads associados."""
    m = _empty_metrics()
    m['spend']       = ad['spend']
    m['impressions'] = ad['impressions']
    m['clicks']      = ad['clicks']

    for lead in leads:
        m['leads_total'] += 1
        if lead['is_a']:
            m['leads_a'] += 1
            if lead['vendeu']:
                m['vendas_a'] += 1
                m['fat_a']    += lead['valor_venda']
        else:
            m['leads_b'] += 1
            if lead['vendeu']:
                m['vendas_b'] += 1
                m['fat_b']    += lead['valor_venda']

    m['fat_total'] = m['fat_a'] + m['fat_b']
    return m

def _accumulate(target, metrics):
    """Soma métricas em um container (adset ou campaign)."""
    for key in ('spend', 'impressions', 'clicks', 'leads_total', 'leads_a', 'leads_b',
                'vendas_a', 'vendas_b', 'fat_a', 'fat_b', 'fat_total'):
        target[key] = target.get(key, 0) + metrics.get(key, 0)

def _calc_derived(node):
    """Calcula lucro e CPL in-place."""
    spend       = node.get('spend', 0)
    fat_total   = node.get('fat_total', 0)
    leads_total = node.get('leads_total', 0)
    leads_a     = node.get('leads_a', 0)
    leads_b     = node.get('leads_b', 0)

    node['lucro']  = round(fat_total - spend, 2)
    node['cpl']    = round(spend / leads_total, 2) if leads_total > 0 else None
    node['cpl_a']  = round(spend / leads_a, 2)     if leads_a > 0     else None
    node['cpl_b']  = round(spend / leads_b, 2)     if leads_b > 0     else None
    node['perc_a'] = round(leads_a / leads_total * 100, 1) if leads_total > 0 else 0
    node['perc_b'] = round(leads_b / leads_total * 100, 1) if leads_total > 0 else 0
    node['spend']  = round(spend, 2)
    node['fat_a']  = round(node.get('fat_a', 0), 2)
    node['fat_b']  = round(node.get('fat_b', 0), 2)
    node['fat_total'] = round(fat_total, 2)

def _calc_organic_metrics(leads):
    """Métricas para leads sem match no FB."""
    m = {'leads_total': 0, 'leads_a': 0, 'leads_b': 0,
         'vendas_a': 0, 'vendas_b': 0, 'fat_a': 0.0, 'fat_b': 0.0, 'fat_total': 0.0}
    for lead in leads:
        m['leads_total'] += 1
        if lead['is_a']:
            m['leads_a'] += 1
            if lead['vendeu']:
                m['vendas_a'] += 1
                m['fat_a']    += lead['valor_venda']
        else:
            m['leads_b'] += 1
            if lead['vendeu']:
                m['vendas_b'] += 1
                m['fat_b']    += lead['valor_venda']
    m['fat_total'] = m['fat_a'] + m['fat_b']
    return m

# ── Rotas ─────────────────────────────────────────────────────────────────────
@cruzamento_bp.route('/cruzamento')
def cruzamento_page():
    from app import obter_token
    token = obter_token()
    if not token:
        return redirect(url_for('pagina_login'))
    return render_template('cruzamento.html')


@cruzamento_bp.route('/api/cruzamento/data')
def api_cruzamento_data():
    from app import obter_token
    token = obter_token()
    if not token:
        return jsonify({'success': False, 'error': 'Não autenticado'}), 401

    date_preset = request.args.get('date_preset', 'last_7_days')
    since       = request.args.get('since')
    until       = request.args.get('until')

    try:
        t0 = time.time()
        # Fetch paralelo: FB + Sheets simultaneamente
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            fb_future     = executor.submit(fetch_fb_insights, AD_ACCOUNT_ID, token, date_preset, since, until)
            sheets_future = executor.submit(fetch_sheets_data, SPREADSHEET_ID)

            fb_ads               = fb_future.result()
            mqls_rows_all, wons_rows_all = sheets_future.result()

        # ── Aplicar filtro de data nas linhas do Sheets ─────────────────────────────────────
        since_d, until_d = preset_to_dates(date_preset, since, until)
        # MQLs: filtra por 'Data do preenchimento'
        mqls_rows = filter_rows_by_date(mqls_rows_all, 'Data do preenchimento', since_d, until_d)
        # Wons: filtra por 'Data de fechamento'
        wons_rows = filter_rows_by_date(wons_rows_all, 'Data de fechamento', since_d, until_d)

        resultado = processar_cruzamento(fb_ads, mqls_rows, wons_rows)
        elapsed = round(time.time() - t0, 2)

        return jsonify({
            'success':  True,
            'data':     resultado,
            'meta': {
                'fb_ads_count':   len(fb_ads),
                'mqls_count':     resultado['total_mqls'],
                'wons_count':     resultado['total_wons'],
                'elapsed_sec':    elapsed,
                'date_preset':    date_preset,
                'timestamp':      datetime.now().isoformat(),
            }
        })

    except FileNotFoundError as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
