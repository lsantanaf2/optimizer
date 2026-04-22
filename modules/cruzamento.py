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
import threading
import requests
import concurrent.futures
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
_BR_TZ = ZoneInfo('America/Sao_Paulo')

from flask import Blueprint, jsonify, render_template, request, session, redirect, url_for

# ── Blueprint ─────────────────────────────────────────────────────────────────
cruzamento_bp = Blueprint('cruzamento', __name__)

# ── Configuração Hardcoded (via .env) ─────────────────────────────────────────
APP_ID              = os.getenv('APP_ID')
APP_SECRET          = os.getenv('APP_SECRET')
AD_ACCOUNT_ID       = os.getenv('CRUZAMENTO_AD_ACCOUNT_ID', 'act_2023939324650844')
SPREADSHEET_ID      = os.getenv('CRUZAMENTO_SPREADSHEET_ID', '1m6syDzMDZqB44ZTKaRj5t79HUDuyEqaN2RgAo0kpECc')
GOOGLE_CREDS_FILE   = os.getenv('GOOGLE_CREDENTIALS_FILE', 'google_credentials.json')
# Action type do evento TypeForm na Meta API.
# Custom pixel events (fbq trackCustom) aparecem como 'offsite_conversion.fb_pixel_custom'.
# Se for uma Custom Conversion criada no Ads Manager, usar 'offsite_conversion.custom.{ID}'.
TYPEFORM_ACTION_TYPE = os.getenv('TYPEFORM_ACTION_TYPE', 'offsite_conversion.fb_pixel_custom')

# Padrão de campanhas que são "Posts do Instagram impulsionados" — separadas em aba própria.
INSTAGRAM_POST_PREFIX = 'post do instagram'

# Persistência de filtros configuráveis (excluir campanhas das views principais).
FILTERS_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'cruzamento_filters.json')
DEFAULT_EXCLUDED_PATTERNS = [
    '[DEMO-180]',
    '[EVENTO MQL]',
    '[BRANDING RENAISSANCE]',
]

def load_excluded_patterns():
    """Carrega lista de padrões a excluir do arquivo de config (ou usa default)."""
    try:
        if os.path.exists(FILTERS_FILE):
            with open(FILTERS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                patterns = data.get('excluded_patterns', [])
                if isinstance(patterns, list):
                    return [str(p).strip() for p in patterns if str(p).strip()]
    except Exception as e:
        print(f"⚠️ Erro ao carregar filtros: {e}")
    return list(DEFAULT_EXCLUDED_PATTERNS)


def save_excluded_patterns(patterns):
    """Salva lista de padrões no arquivo de config."""
    try:
        clean = [str(p).strip() for p in patterns if str(p).strip()]
        with open(FILTERS_FILE, 'w', encoding='utf-8') as f:
            json.dump({'excluded_patterns': clean}, f, ensure_ascii=False, indent=2)
        return True, clean
    except Exception as e:
        print(f"❌ Erro ao salvar filtros: {e}")
        return False, []


def _matches_excluded(campaign_name, patterns):
    """True se campaign_name contém qualquer um dos padrões (case-insensitive)."""
    if not campaign_name or not patterns:
        return False
    cn = campaign_name.lower()
    return any(p.lower() in cn for p in patterns)


def _is_instagram_post(campaign_name):
    """True se campanha é um Post do Instagram impulsionado."""
    if not campaign_name:
        return False
    return campaign_name.strip().lower().startswith(INSTAGRAM_POST_PREFIX)

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
        'fields': 'campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,clicks,inline_link_clicks,actions,date_start',
        'limit': 500,
        'time_increment': 1,  # dados diarios
    }

    if since and until:
        params['time_range'] = json.dumps({'since': since, 'until': until})
    else:
        # Converte preset interno em datas reais (Meta API usa last_7d, não last_7_days)
        since_d, until_d = preset_to_dates(date_preset)
        if since_d and until_d:
            params['time_range'] = json.dumps({'since': str(since_d), 'until': str(until_d)})
        else:
            params['date_preset'] = 'last_30d'  # fallback seguro

    ads = []
    url = base_url
    while url:
        resp = requests.get(url, params=params if url == base_url else None, timeout=30)
        resp.raise_for_status()
        body = resp.json()

        for item in body.get('data', []):
            actions = item.get('actions', [])

            def _act(atype, _a=actions):
                return sum(int(float(a.get('value', 0) or 0)) for a in _a if a.get('action_type') == atype)

            def _act_typeform(_a=actions):
                if TYPEFORM_ACTION_TYPE:
                    return _act(TYPEFORM_ACTION_TYPE, _a)
                return sum(int(float(a.get('value', 0) or 0)) for a in _a if 'typeform' in a.get('action_type', '').lower())

            def _act_ig_follows(_a=actions):
                # Seguidores ganhos no Instagram via boost — Meta retorna em 'actions' com
                # action_type contendo 'follow' (ex: 'onsite_conversion.follow', 'follow').
                # Soma todos os tipos contendo 'follow' para cobrir variações da API.
                return sum(int(float(a.get('value', 0) or 0)) for a in _a if 'follow' in a.get('action_type', '').lower())

            ads.append({
                'campaign_id':        item.get('campaign_id', ''),
                'campaign_name':      item.get('campaign_name', ''),
                'adset_id':           item.get('adset_id', ''),
                'adset_name':         item.get('adset_name', ''),
                'ad_id':              item.get('ad_id', ''),
                'ad_name':            item.get('ad_name', ''),
                'spend':              float(item.get('spend', 0) or 0),
                'impressions':        int(item.get('impressions', 0) or 0),
                'clicks':             int(item.get('clicks', 0) or 0),
                'link_clicks':        int(item.get('inline_link_clicks', 0) or 0),
                'landing_page_views': _act('landing_page_view'),
                'typeform_submits':   _act_typeform(),
                'instagram_follows':  _act_ig_follows(),
                'date_start':         item.get('date_start', ''),
            })

        # Paginação cursor
        paging = body.get('paging', {})
        next_url = paging.get('next')
        url = next_url if next_url else None
        params = None  # next_url já tem params embutidos

    return ads

def fetch_vinci_daily(since_dt=None, until_dt=None):
    """
    Lê planilha pública VINCI (Google Ads) e retorna dict agregado por data:
        { 'YYYY-MM-DD': {'spend': float, 'clicks': int} }
    Filtra por data (se fornecidas) e apenas linhas com 'VINCI' no nome da campanha.
    Não levanta exceção — em caso de erro retorna {}.
    """
    import csv
    import io

    SHEET_ID  = os.getenv('GOOGLE_ADS_SHEET_ID', '1vhctrrIBQujABaD0VROW8dNHuZIqA-MIX8ESZC77tLg')
    SHEET_GID = os.getenv('GOOGLE_ADS_SHEET_GID', '2054617579')
    FILTER_KW = 'VINCI'

    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&gid={SHEET_GID}'
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"⚠️ VINCI daily fetch falhou (non-blocking): {e}")
        return {}

    def _parse_num(s):
        if not s:
            return 0.0
        s = s.strip().replace('%', '')
        if ',' in s and '.' in s:
            s = s.replace('.', '').replace(',', '.')
        elif ',' in s:
            s = s.replace(',', '.')
        try:
            return float(s)
        except ValueError:
            return 0.0

    result = {}
    try:
        reader = csv.DictReader(io.StringIO(resp.text))
        headers_map = {}
        for row in reader:
            if not headers_map:
                for k in row.keys():
                    kl = k.lower()
                    if 'date' in kl or 'data' in kl:
                        headers_map['date'] = k
                    elif 'campaign' in kl or 'campanha' in kl:
                        headers_map['campaign'] = k
                    elif 'cost' in kl or 'custo' in kl or 'spend' in kl or 'gasto' in kl:
                        headers_map['cost'] = k
                    elif 'click' in kl or 'clique' in kl:
                        headers_map['clicks'] = k

            date_col     = headers_map.get('date', 'Date (Segment)')
            campaign_col = headers_map.get('campaign', 'Campaign Name')
            cost_col     = headers_map.get('cost', 'Cost')
            clicks_col   = headers_map.get('clicks', 'Clicks')

            name = row.get(campaign_col, '').strip()
            if FILTER_KW.upper() not in name.upper():
                continue

            date_str = row.get(date_col, '').strip()
            if not date_str:
                continue
            try:
                row_dt = datetime.strptime(date_str, '%d/%m/%Y').date()
            except ValueError:
                continue

            if since_dt and row_dt < since_dt:
                continue
            if until_dt and row_dt > until_dt:
                continue

            key = row_dt.strftime('%Y-%m-%d')
            spend  = _parse_num(row.get(cost_col, '0'))
            clicks = int(_parse_num(row.get(clicks_col, '0')))

            entry = result.setdefault(key, {'spend': 0.0, 'clicks': 0})
            entry['spend']  += spend
            entry['clicks'] += clicks
    except Exception as e:
        print(f"⚠️ VINCI daily parse falhou: {e}")
        return {}

    return result


def fetch_ads_status(account_id, access_token):
    """Busca o status (ACTIVE, PAUSED, etc) de todos os ads."""
    base_url = f"https://graph.facebook.com/v22.0/{account_id}/ads"
    params = {'access_token': access_token, 'fields': 'id,status', 'limit': 1000}
    status_map = {}
    url = base_url
    try:
        while url:
            resp = requests.get(url, params=params if url == base_url else None, timeout=30)
            if resp.status_code != 200:
                print(f"⚠️ Aviso: Falha ao buscar status dos ads na URL {url}: {resp.text}")
                break
            body = resp.json()
            for item in body.get('data', []):
                status_map[item['id']] = item.get('status', 'UNKNOWN')
            url = body.get('paging', {}).get('next')
            params = None
    except Exception as e:
        print(f"⚠️ Erro ao buscar status dos ads: {e}")
    return status_map


def fetch_campaigns_status(account_id, access_token):
    """Busca o effective_status de todas as campanhas da conta."""
    base_url = f"https://graph.facebook.com/v22.0/{account_id}/campaigns"
    params = {'access_token': access_token, 'fields': 'id,effective_status', 'limit': 500}
    status_map = {}
    url = base_url
    try:
        while url:
            resp = requests.get(url, params=params if url == base_url else None, timeout=30)
            if resp.status_code != 200:
                break
            body = resp.json()
            for item in body.get('data', []):
                status_map[item['id']] = item.get('effective_status', 'UNKNOWN')
            url = body.get('paging', {}).get('next')
            params = None
    except Exception as e:
        print(f"⚠️ Erro ao buscar status das campanhas: {e}")
    return status_map


def fetch_adsets_status(account_id, access_token):
    """Busca o effective_status de todos os adsets da conta."""
    base_url = f"https://graph.facebook.com/v22.0/{account_id}/adsets"
    params = {'access_token': access_token, 'fields': 'id,effective_status', 'limit': 500}
    status_map = {}
    url = base_url
    try:
        while url:
            resp = requests.get(url, params=params if url == base_url else None, timeout=30)
            if resp.status_code != 200:
                break
            body = resp.json()
            for item in body.get('data', []):
                status_map[item['id']] = item.get('effective_status', 'UNKNOWN')
            url = body.get('paging', {}).get('next')
            params = None
    except Exception as e:
        print(f"⚠️ Erro ao buscar status dos adsets: {e}")
    return status_map

# ── Processamento: Duplo Join em Memória ──────────────────────────────────────
def processar_cruzamento(fb_ads, mqls_rows, wons_rows, mqls_all=None, excluded_patterns=None):
    """
    Passo 1: MQLs × Wons por 'Deal ID'  → enriquece cada lead com venda
    Passo 2: leads × FB ads por utm_content (norm) ↔ ad_name (norm)
             fallback: utm_campaign ↔ campaign_name

    excluded_patterns: lista de strings — campanhas contendo qualquer padrão
                       são EXCLUÍDAS de todas as views (filtro global).
                       Se None, usa load_excluded_patterns().

    Posts do Instagram impulsionados (campanhas começando com "Post do Instagram")
    são separados das views principais e agregados em instagram_posts_consolidated
    para a aba "Fase 1 - Ganho de seguidores".
    """

    # ── Filtro global: remove campanhas excluídas e separa Posts do Instagram ──
    if excluded_patterns is None:
        excluded_patterns = load_excluded_patterns()

    fb_ads_main = []
    fb_ads_ig_posts = []
    for ad in fb_ads:
        cn = ad.get('campaign_name', '')
        if _matches_excluded(cn, excluded_patterns):
            continue  # exclui completamente
        if _is_instagram_post(cn):
            fb_ads_ig_posts.append(ad)
            continue  # vai pra aba Fase 1, fora das views principais
        fb_ads_main.append(ad)

    # A partir daqui o fluxo principal opera sobre fb_ads filtrado.
    fb_ads = fb_ads_main

    # ── Defesa em profundidade: leads do Google Ads não devem ser atribuídos ao FB ──
    # O caller já filtra, mas reforçamos aqui para proteger qualquer chamada futura.
    _GOOGLE_SRC = {'adwords', 'google'}
    mqls_rows = [r for r in mqls_rows
                 if _norm(r.get('utm_source', '')) not in _GOOGLE_SRC]

    # ── Passo 1: Join Wons → indexar por Deal ID ──────────────────────────────
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
        produto = _norm(row.get('Produto indicado', ''))
        produto_label = (row.get('Produto indicado', '') or '').strip() or 'Sem produto'
        won_data = wons_idx.get(deal_id, {})
        is_a = _is_produto_a(produto)
        d_preench = _parse_date_br(row.get('Data do preenchimento', ''))
        leads_enriquecidos.append({
            'deal_id':       deal_id,
            'produto':       produto,
            'produto_label': produto_label,
            'is_a':          is_a,
            'utm_campaign':  _norm(row.get('utm_campaign', '')),
            'utm_content':   _norm(row.get('utm_content', '')),
            'utm_medium':    _norm(row.get('utm_medium', '')),
            'utm_source':    _norm(row.get('utm_source', '')),
            'utm_term':      _norm(row.get('utm_term', '')),
            'vendeu':        bool(won_data),
            'valor_venda':   won_data.get('valor', 0.0),
            'data_preenchimento': d_preench.strftime('%Y-%m-%d') if d_preench else '',
        })

    mqls_in_period_count = len(leads_enriquecidos)

    # ── Recuperar wons do período cujo MQL foi criado ANTES do período ─────────
    if mqls_all:
        leads_ids_in_period = {l['deal_id'] for l in leads_enriquecidos}
        mqls_all_by_deal = {
            _norm(row.get('Deal ID', '')): row
            for row in mqls_all
            if _norm(row.get('Deal ID', ''))
               and _norm(row.get('utm_source', '')) not in _GOOGLE_SRC
        }
        for deal_id, won_data in wons_idx.items():
            if deal_id not in leads_ids_in_period:
                mql_row = mqls_all_by_deal.get(deal_id)
                if mql_row:
                    produto = _norm(mql_row.get('Produto indicado', ''))
                    produto_label = (mql_row.get('Produto indicado', '') or '').strip() or 'Sem produto'
                    is_a = _is_produto_a(produto)
                    d_preench = _parse_date_br(mql_row.get('Data do preenchimento', ''))
                    leads_enriquecidos.append({
                        'deal_id':      deal_id,
                        'produto':      produto,
                        'produto_label': produto_label,
                        'is_a':         is_a,
                        'utm_campaign': _norm(mql_row.get('utm_campaign', '')),
                        'utm_content':  _norm(mql_row.get('utm_content', '')),
                        'utm_medium':   _norm(mql_row.get('utm_medium', '')),
                        'utm_source':   _norm(mql_row.get('utm_source', '')),
                        'utm_term':     _norm(mql_row.get('utm_term', '')),
                        'vendeu':       True,
                        'valor_venda':  won_data['valor'],
                        'data_preenchimento': d_preench.strftime('%Y-%m-%d') if d_preench else '',
                    })

    # ── Passo 2: Consolidar FB Ads/AdSets/Campaigns — TODAS as chaves ESCOPADAS
    # POR CAMPANHA para evitar colisão cross-campanha (adsets/ads com nomes
    # idênticos em campanhas diferentes).
    fb_ads_by_name       = {}   # "camp|||adset|||ad_name" → entry
    fb_adsets_by_name    = {}   # "camp|||adset"           → entry
    fb_campaigns_by_name = {}   # "camp"                   → entry

    # ID → nome normalizado (para UTMs que vêm com ID em vez do nome)
    _ad_id_to_name    = {}
    _adset_id_to_name = {}
    _camp_id_to_name  = {}

    for ad in fb_ads:
        name_norm     = _norm(ad.get('ad_name', ''))
        adset_norm    = _norm(ad.get('adset_name', ''))
        campaign_norm = _norm(ad.get('campaign_name', ''))

        ad_id_raw       = ad.get('ad_id', '')
        adset_id_raw    = ad.get('adset_id', '')
        campaign_id_raw = ad.get('campaign_id', '')

        if ad_id_raw:       _ad_id_to_name[_norm(ad_id_raw)]         = name_norm
        if adset_id_raw:    _adset_id_to_name[_norm(adset_id_raw)]   = adset_norm
        if campaign_id_raw: _camp_id_to_name[_norm(campaign_id_raw)] = campaign_norm

        _METRIC_KEYS = ('spend', 'impressions', 'clicks', 'link_clicks',
                        'landing_page_views', 'typeform_submits')

        # Ad — chave composta (camp, adset, ad_name)
        if name_norm:
            ad_key = f"{campaign_norm}|||{adset_norm}|||{name_norm}"
            if ad_key not in fb_ads_by_name:
                fb_ads_by_name[ad_key] = {
                    'ad_name':            ad.get('ad_name', 'Desconhecido'),
                    'campaign_name':      ad.get('campaign_name', ''),
                    'campaign_status':    ad.get('campaign_status', 'UNKNOWN'),
                    'adset_name':         ad.get('adset_name', ''),
                    'adset_status':       ad.get('adset_status', 'UNKNOWN'),
                    'ad_status':          ad.get('ad_status', 'UNKNOWN'),
                    'ad_ids':             set(),
                    '_name_norm':         name_norm,
                    '_adset_norm':        adset_norm,
                    '_campaign_norm':     campaign_norm,
                    'spend': 0.0, 'impressions': 0, 'clicks': 0,
                    'link_clicks': 0, 'landing_page_views': 0, 'typeform_submits': 0,
                }
            if ad_id_raw:
                fb_ads_by_name[ad_key]['ad_ids'].add(ad_id_raw)
            for _k in _METRIC_KEYS:
                fb_ads_by_name[ad_key][_k] += ad.get(_k, 0) or 0

        # AdSet — chave composta (camp, adset) — ESCOPA POR CAMPANHA
        if adset_norm:
            adset_key = f"{campaign_norm}|||{adset_norm}"
            if adset_key not in fb_adsets_by_name:
                fb_adsets_by_name[adset_key] = {
                    'adset_name':         ad.get('adset_name', 'Desconhecido'),
                    'campaign_name':      ad.get('campaign_name', ''),
                    'adset_id':           adset_id_raw,
                    '_adset_norm':        adset_norm,
                    '_campaign_norm':     campaign_norm,
                    'ad_status':          ad.get('ad_status', 'PAUSED'),
                    'spend': 0.0, 'impressions': 0, 'clicks': 0,
                    'link_clicks': 0, 'landing_page_views': 0, 'typeform_submits': 0,
                }
            if ad.get('ad_status') == 'ACTIVE':
                fb_adsets_by_name[adset_key]['ad_status'] = 'ACTIVE'
            for _k in _METRIC_KEYS:
                fb_adsets_by_name[adset_key][_k] += ad.get(_k, 0) or 0

        # Campaign — chave por camp_norm (único)
        if campaign_norm:
            if campaign_norm not in fb_campaigns_by_name:
                fb_campaigns_by_name[campaign_norm] = {
                    'campaign_name':      ad.get('campaign_name', 'Desconhecida'),
                    'campaign_id':        campaign_id_raw,
                    'ad_status':          ad.get('ad_status', 'PAUSED'),
                    'spend': 0.0, 'impressions': 0, 'clicks': 0,
                    'link_clicks': 0, 'landing_page_views': 0, 'typeform_submits': 0,
                }
            if ad.get('ad_status') == 'ACTIVE':
                fb_campaigns_by_name[campaign_norm]['ad_status'] = 'ACTIVE'
            for _k in _METRIC_KEYS:
                fb_campaigns_by_name[campaign_norm][_k] += ad.get(_k, 0) or 0

    # ── Passo 3: Resolvers — mapeiam UTMs do lead (nome OU id) para entidades FB reais ──
    def _resolve_camp(lead):
        """utm_campaign → camp_norm existente (ou '')."""
        u = lead.get('utm_campaign', '')
        if not u or u == 'null':
            return ''
        if u in fb_campaigns_by_name:
            return u
        resolved = _camp_id_to_name.get(u, '')
        return resolved if resolved in fb_campaigns_by_name else ''

    def _resolve_adset(lead, camp_hint):
        """
        utm_content → (camp, adset) existente em fb_adsets_by_name.
        Com camp_hint: exige que o adset pertença àquela campanha.
        Sem camp_hint: só resolve se o nome for único em UMA campanha (sem ambiguidade).
        Retorna ('','') se não resolver com segurança.
        """
        u = lead.get('utm_content', '')
        if not u or u == 'null':
            return ('', '')
        # Candidatos por nome exato
        cands = [k for k in fb_adsets_by_name if k.endswith(f"|||{u}")]
        # Candidatos via ID → nome
        resolved_by_id = _adset_id_to_name.get(u, '')
        if resolved_by_id:
            cands += [k for k in fb_adsets_by_name
                      if k.endswith(f"|||{resolved_by_id}") and k not in cands]
        if camp_hint:
            for k in cands:
                if k.startswith(f"{camp_hint}|||"):
                    return (camp_hint, k.split('|||', 1)[1])
            return ('', '')
        # Sem camp_hint: só resolve se for inequívoco (uma única campanha)
        if len(cands) == 1:
            camp, adset = cands[0].split('|||', 1)
            return (camp, adset)
        return ('', '')

    def _resolve_ad(lead, camp_hint, adset_hint):
        """
        utm_term → ad_name_norm existente. Exige camp+adset já resolvidos para evitar
        cross-adset/cross-campanha. Retorna ('','','') se não resolver.
        """
        u = lead.get('utm_term', '')
        if not u or u == 'null':
            return ('', '', '')
        if not (camp_hint and adset_hint):
            return ('', '', '')
        key_name = f"{camp_hint}|||{adset_hint}|||{u}"
        if key_name in fb_ads_by_name:
            return (camp_hint, adset_hint, u)
        resolved = _ad_id_to_name.get(u, '')
        if resolved:
            key_id = f"{camp_hint}|||{adset_hint}|||{resolved}"
            if key_id in fb_ads_by_name:
                return (camp_hint, adset_hint, resolved)
        return ('', '', '')

    # ── Atribuição por lead ──
    _ad_leads       = {k: [] for k in fb_ads_by_name}
    _adset_leads    = {k: [] for k in fb_adsets_by_name}
    _campaign_leads = {k: [] for k in fb_campaigns_by_name}

    # Buckets para matches parciais (viram linhas "⚠️ indeterminado" na UI)
    _adset_indet_ads   = {}  # adset_key → [leads]  (chegou no adset, ad não bateu)
    _camp_indet_ads    = {}  # camp_norm → [leads]  (só camp, ad indeterminado)
    _camp_indet_adsets = {}  # camp_norm → [leads]  (só camp, adset indeterminado)

    organicos_ads       = []
    organicos_adsets    = []
    organicos_campaigns = []

    for lead in leads_enriquecidos:
        camp = _resolve_camp(lead)
        adset_camp, adset = _resolve_adset(lead, camp)
        # Se veio adset sem camp explícito, adota a camp do adset
        if adset and not camp:
            camp = adset_camp
        # Se camp conflita com a do adset, descarta o adset (cross-campanha suspeito)
        if adset and adset_camp and adset_camp != camp:
            adset = ''
            adset_camp = ''
        _, _, ad = _resolve_ad(lead, camp, adset)

        # ── Ads-level ──
        if camp and adset and ad:
            _ad_leads[f"{camp}|||{adset}|||{ad}"].append(lead)
        elif camp and adset and f"{camp}|||{adset}" in fb_adsets_by_name:
            _adset_indet_ads.setdefault(f"{camp}|||{adset}", []).append(lead)
        elif camp and camp in fb_campaigns_by_name:
            _camp_indet_ads.setdefault(camp, []).append(lead)
        else:
            organicos_ads.append(lead)

        # ── AdSet-level ──
        if camp and adset and f"{camp}|||{adset}" in fb_adsets_by_name:
            _adset_leads[f"{camp}|||{adset}"].append(lead)
        elif camp and camp in fb_campaigns_by_name:
            _camp_indet_adsets.setdefault(camp, []).append(lead)
        else:
            organicos_adsets.append(lead)

        # ── Campaign-level ──
        if camp and camp in fb_campaigns_by_name:
            _campaign_leads[camp].append(lead)
        else:
            organicos_campaigns.append(lead)

    # ── Consolidar Ads ──
    ads_consolidated = []
    for ad_key, ad_entry in fb_ads_by_name.items():
        metrics = _calc_metrics(ad_entry, _ad_leads[ad_key])
        ads_consolidated.append({
            'ad_name':         ad_entry['ad_name'],
            'campaign_name':   ad_entry['campaign_name'],
            'campaign_status': ad_entry.get('campaign_status', 'UNKNOWN'),
            'adset_name':      ad_entry['adset_name'],
            'adset_status':    ad_entry.get('adset_status', 'UNKNOWN'),
            'ad_status':       ad_entry['ad_status'],
            **metrics
        })
    # Linhas "⚠️ Ad indeterminado (sem utm_term válido)" por adset
    for adset_key, ileads in _adset_indet_ads.items():
        adset_entry = fb_adsets_by_name[adset_key]
        metrics = _calc_metrics(_empty_metrics(), ileads)
        ads_consolidated.append({
            'ad_name':         '⚠️ Ad indeterminado (sem utm_term)',
            'campaign_name':   adset_entry['campaign_name'],
            'campaign_status': 'UNKNOWN',
            'adset_name':      adset_entry['adset_name'],
            'adset_status':    'UNKNOWN',
            'ad_status':       'INDETERMINATE',
            **metrics
        })
    # Linhas "⚠️ Ad+AdSet indeterminados" por campanha
    for camp_norm, ileads in _camp_indet_ads.items():
        camp_entry = fb_campaigns_by_name[camp_norm]
        metrics = _calc_metrics(_empty_metrics(), ileads)
        ads_consolidated.append({
            'ad_name':         '⚠️ Ad indeterminado (sem utm_content/term)',
            'campaign_name':   camp_entry['campaign_name'],
            'campaign_status': 'UNKNOWN',
            'adset_name':      '⚠️ AdSet indeterminado',
            'adset_status':    'UNKNOWN',
            'ad_status':       'INDETERMINATE',
            **metrics
        })

    # ── Consolidar AdSets ──
    adsets_consolidated = []
    for adset_key, adset_entry in fb_adsets_by_name.items():
        metrics = _calc_metrics(adset_entry, _adset_leads[adset_key])
        adsets_consolidated.append({
            'adset_name':    adset_entry['adset_name'],
            'campaign_name': adset_entry['campaign_name'],
            'ad_status':     adset_entry['ad_status'],
            **metrics
        })
    for camp_norm, ileads in _camp_indet_adsets.items():
        camp_entry = fb_campaigns_by_name[camp_norm]
        metrics = _calc_metrics(_empty_metrics(), ileads)
        adsets_consolidated.append({
            'adset_name':    '⚠️ AdSet indeterminado (sem utm_content)',
            'campaign_name': camp_entry['campaign_name'],
            'ad_status':     'INDETERMINATE',
            **metrics
        })

    # ── Consolidar Campaigns ──
    campaigns_consolidated = []
    for camp_norm, camp_entry in fb_campaigns_by_name.items():
        metrics = _calc_metrics(camp_entry, _campaign_leads[camp_norm])
        campaigns_consolidated.append({
            'campaign_name': camp_entry['campaign_name'],
            'ad_status':     camp_entry['ad_status'],
            **metrics
        })

    organico_metrics = _calc_organic_metrics(organicos_ads)  # backward compatibility

    # ── Adiciona os Não-Encontrados nas Tabelas ─────────────────────
    
    # Injetar Ads Órfãos
    organicos_by_term = {}
    for l in organicos_ads:
        ut = l.get('utm_term', '')
        key = '🌿 Orgânico / Sem UTM' if not ut or ut == 'null' else f"⚠️ {ut} (Fora do FB)"
        organicos_by_term.setdefault(key, []).append(l)

    for nome_exibicao, m_leads in organicos_by_term.items():
        metrics = _calc_metrics(_empty_metrics(), m_leads)
        ads_consolidated.append({
            'ad_name':       nome_exibicao,
            'campaign_name': '-',
            'adset_name':    '-',
            'ad_status':     'ORGANIC',
            **metrics
        })
        
    # Injetar AdSets Órfãos
    organicos_by_content = {}
    for l in organicos_adsets:
        uc = l.get('utm_content', '')
        key = '🌿 Orgânico / Sem UTM' if not uc or uc == 'null' else f"⚠️ {uc} (Fora do FB)"
        organicos_by_content.setdefault(key, []).append(l)

    for nome_exibicao, m_leads in organicos_by_content.items():
        metrics = _calc_metrics(_empty_metrics(), m_leads)
        adsets_consolidated.append({
            'adset_name':    nome_exibicao,
            'campaign_name': '-',
            'ad_status':     'ORGANIC',
            **metrics
        })

    # Injetar Campaigns Órfãs
    organicos_by_camp = {}
    for l in organicos_campaigns:
        ucamp = l.get('utm_campaign', '')
        key = '🌿 Orgânico / Sem UTM' if not ucamp or ucamp == 'null' else f"⚠️ {ucamp} (Fora do FB)"
        organicos_by_camp.setdefault(key, []).append(l)

    for nome_exibicao, m_leads in organicos_by_camp.items():
        metrics = _calc_metrics(_empty_metrics(), m_leads)
        campaigns_consolidated.append({
            'campaign_name': nome_exibicao,
            'ad_status':     'ORGANIC',
            **metrics
        })

    # Ordenações
    ads_consolidated.sort(key=lambda x: (x.get('spend', 0), x.get('leads_total', 0)), reverse=True)
    adsets_consolidated.sort(key=lambda x: (x.get('spend', 0), x.get('leads_total', 0)), reverse=True)
    campaigns_consolidated.sort(key=lambda x: (x.get('spend', 0), x.get('leads_total', 0)), reverse=True)

    # ── Faturamento total do Sheets (Wons filtradas) ───────────────────────────
    fat_total_sheets = sum(_parse_valor(row.get('Valor', 0)) for row in wons_rows)

    # ── Spend diário do FB (soma de todos os ads por date_start) ──────────────
    daily_spend = {}
    for ad in fb_ads:
        d = ad.get('date_start', '')
        if d:
            daily_spend[d] = daily_spend.get(d, 0.0) + ad['spend']

    # ── MQLs agrupados por data (Data do preenchimento) ───────────────────────
    by_date_raw = {}
    for row in mqls_rows:
        d = _parse_date_br(row.get('Data do preenchimento', ''))
        if d is None:
            continue
        key = d.strftime('%Y-%m-%d')
        entry = by_date_raw.setdefault(key, {'mqls': 0, 'produtos': {}})
        entry['mqls'] += 1
        prod = row.get('Produto indicado', '').strip() or 'Sem produto'
        entry['produtos'][prod] = entry['produtos'].get(prod, 0) + 1

    # Montar lista de datas ordenada com spend e CPL
    by_date = []
    for date_key in sorted(by_date_raw.keys(), reverse=True):
        entry  = by_date_raw[date_key]
        spend  = round(daily_spend.get(date_key, 0.0), 2)
        mqls   = entry['mqls']
        by_date.append({
            'date':     date_key,
            'mqls':     mqls,
            'spend':    spend,
            'cpl':      round(spend / mqls, 2) if mqls > 0 and spend > 0 else None,
            'produtos': entry['produtos'],
        })

    # ── MQLs totais por Produto indicado ──────────────────────────────────────
    by_produto = {}
    for row in mqls_rows:
        prod = row.get('Produto indicado', '').strip() or 'Sem produto'
        by_produto[prod] = by_produto.get(prod, 0) + 1

    # ── Funil diário (Painel de Acompanhamento) ────────────────────────────────
    # Agrega todas as métricas de funil por dia a partir dos fb_ads
    _daily_fb = {}
    for ad in fb_ads:
        d = ad.get('date_start', '')
        if not d:
            continue
        e = _daily_fb.setdefault(d, {
            'spend': 0.0, 'impressions': 0,
            'link_clicks': 0, 'landing_page_views': 0, 'typeform_submits': 0
        })
        e['spend']              += ad.get('spend', 0.0)
        e['impressions']        += ad.get('impressions', 0)
        e['link_clicks']        += ad.get('link_clicks', 0)
        e['landing_page_views'] += ad.get('landing_page_views', 0)
        e['typeform_submits']   += ad.get('typeform_submits', 0)

    all_panel_dates = sorted(set(list(_daily_fb.keys()) + list(by_date_raw.keys())))
    daily_funnel = []
    for dk in all_panel_dates:
        fb  = _daily_fb.get(dk, {})
        mql = by_date_raw.get(dk, {}).get('mqls', 0)
        sp  = round(fb.get('spend', 0.0), 2)
        imp = fb.get('impressions', 0)
        lc  = fb.get('link_clicks', 0)
        lpv = fb.get('landing_page_views', 0)
        tf  = fb.get('typeform_submits', 0)
        daily_funnel.append({
            'date':          dk,
            'spend':         sp,
            'impressions':   imp,
            'link_clicks':   lc,
            'lpv':           lpv,
            'typeform':      tf,
            'mqls':          mql,
            'ctr':           round(lc  / imp * 100, 2) if imp > 0 else None,
            'connect_rate':  round(lpv / lc  * 100, 2) if lc  > 0 else None,
            'taxa_lead':     round(tf  / lpv * 100, 2) if lpv > 0 else None,
            'taxa_mql':      round(mql / tf  * 100, 2) if tf  > 0 else None,
        })

    # ── Breakdown diário por entidade ────────────────────────────────────────
    # IMPORTANTE: para garantir que MQLs/Wons sejam contabilizados no MESMO
    # escopo (campanha/adset/ad) já resolvido acima, os MQLs por data são
    # derivados diretamente das listas de leads atribuídos (_ad_leads,
    # _adset_leads, _campaign_leads). Isso elimina cross-campanha por
    # colisão de nomes de adset/ad entre campanhas diferentes.
    def _build_entity_series(spend_map, mqls_map, prods_map=None):
        all_keys = sorted(set(list(spend_map.keys()) + list(mqls_map.keys())))
        result = []
        for dk in all_keys:
            sp = round(spend_map.get(dk, 0.0), 2)
            mq = mqls_map.get(dk, 0)
            entry = {
                'date':  dk,
                'mqls':  mq,
                'spend': sp,
                'cpl':   round(sp / mq, 2) if mq > 0 and sp > 0 else None,
            }
            if prods_map is not None:
                entry['produtos'] = prods_map.get(dk, {})
            result.append(entry)
        return result

    def _mqls_by_date_from_leads(lead_list):
        """Agrega MQLs + produtos por data a partir de uma lista de leads já atribuídos."""
        m_by_d = {}
        p_by_d = {}
        for l in lead_list:
            dk = l.get('data_preenchimento', '')
            if not dk:
                continue
            m_by_d[dk] = m_by_d.get(dk, 0) + 1
            prod = l.get('produto_label', 'Sem produto')
            pd = p_by_d.setdefault(dk, {})
            pd[prod] = pd.get(prod, 0) + 1
        return m_by_d, p_by_d

    # Pré-indexar fb_ads spend por chave composta (campanha|||adset|||ad, date) — O(n)
    _spend_by_campkey_date  = {}  # camp_norm                    → {date: spend}
    _spend_by_adsetkey_date = {}  # "camp|||adset"               → {date: spend}
    _spend_by_adkey_date    = {}  # "camp|||adset|||ad"          → {date: spend}
    for ad in fb_ads:
        d = ad.get('date_start', '')
        if not d:
            continue
        sp = ad.get('spend', 0.0) or 0.0
        cn = _norm(ad.get('campaign_name', ''))
        an = _norm(ad.get('adset_name', ''))
        adn = _norm(ad.get('ad_name', ''))
        if cn:
            _spend_by_campkey_date.setdefault(cn, {})
            _spend_by_campkey_date[cn][d] = _spend_by_campkey_date[cn].get(d, 0.0) + sp
        if cn and an:
            k = f"{cn}|||{an}"
            _spend_by_adsetkey_date.setdefault(k, {})
            _spend_by_adsetkey_date[k][d] = _spend_by_adsetkey_date[k].get(d, 0.0) + sp
        if cn and an and adn:
            k = f"{cn}|||{an}|||{adn}"
            _spend_by_adkey_date.setdefault(k, {})
            _spend_by_adkey_date[k][d] = _spend_by_adkey_date[k].get(d, 0.0) + sp

    # Por campanha — chave de saída = campaign_name (único por camp_norm)
    by_date_per_campaign = {}
    for camp_norm, camp_data in fb_campaigns_by_name.items():
        ds = _spend_by_campkey_date.get(camp_norm, {})
        dm, dp = _mqls_by_date_from_leads(_campaign_leads.get(camp_norm, []))
        by_date_per_campaign[camp_data['campaign_name']] = _build_entity_series(ds, dm, dp)

    # Por conjunto — chave de saída = adset_name (pode haver colisão cross-camp,
    # mas cada entrada aqui corresponde ao par (camp, adset); em colisão mescla
    # as séries via merge por data — o frontend filtra primeiro por campanha
    # então a visualização permanece coerente).
    def _merge_series(a, b):
        by_date = {}
        for e in a + b:
            dk = e['date']
            slot = by_date.setdefault(dk, {'date': dk, 'mqls': 0, 'spend': 0.0, 'produtos': {}})
            slot['mqls']  += e.get('mqls', 0)
            slot['spend'] += e.get('spend', 0.0)
            for p, v in (e.get('produtos') or {}).items():
                slot['produtos'][p] = slot['produtos'].get(p, 0) + v
        out = []
        for dk in sorted(by_date.keys()):
            s = by_date[dk]
            s['spend'] = round(s['spend'], 2)
            s['cpl'] = round(s['spend'] / s['mqls'], 2) if s['mqls'] > 0 and s['spend'] > 0 else None
            out.append(s)
        return out

    by_date_per_adset = {}
    for adset_key, adset_data in fb_adsets_by_name.items():
        ds = _spend_by_adsetkey_date.get(adset_key, {})
        dm, dp = _mqls_by_date_from_leads(_adset_leads.get(adset_key, []))
        series = _build_entity_series(ds, dm, dp)
        display_name = adset_data['adset_name']
        if display_name in by_date_per_adset:
            by_date_per_adset[display_name] = _merge_series(by_date_per_adset[display_name], series)
        else:
            by_date_per_adset[display_name] = series

    # Por anúncio — chave de saída = ad_name (mesmo tratamento de colisão)
    by_date_per_ad = {}
    for ad_key, ad_data_item in fb_ads_by_name.items():
        ds = _spend_by_adkey_date.get(ad_key, {})
        dm, dp = _mqls_by_date_from_leads(_ad_leads.get(ad_key, []))
        series = _build_entity_series(ds, dm, dp)
        display_name = ad_data_item['ad_name']
        if display_name in by_date_per_ad:
            by_date_per_ad[display_name] = _merge_series(by_date_per_ad[display_name], series)
        else:
            by_date_per_ad[display_name] = series

    # ── Funil de Conversão (agregado de todos os fb_ads do período) ──────────────
    _f_imp  = sum(ad.get('impressions', 0)        for ad in fb_ads)
    _f_lc   = sum(ad.get('link_clicks', 0)         for ad in fb_ads)
    _f_lpv  = sum(ad.get('landing_page_views', 0)  for ad in fb_ads)
    _f_tf   = sum(ad.get('typeform_submits', 0)    for ad in fb_ads)
    _f_mql  = mqls_in_period_count

    funnel = {
        'impressions':       _f_imp,
        'link_clicks':       _f_lc,
        'ctr':               round(_f_lc  / _f_imp * 100, 2) if _f_imp > 0 else 0,
        'landing_page_views': _f_lpv,
        'connect_rate':      round(_f_lpv / _f_lc  * 100, 2) if _f_lc  > 0 else 0,
        'typeform_submits':  _f_tf,
        'typeform_rate':     round(_f_tf  / _f_lpv * 100, 2) if _f_lpv > 0 else 0,
        'mqls':              _f_mql,
        'mql_rate':          round(_f_mql / _f_tf  * 100, 2) if _f_tf  > 0 else 0,
    }

    # ── Aba "Fase 1 - Ganho de seguidores": agrega Posts do Instagram por campanha ──
    # Cada campanha "Post do Instagram: ..." vira uma linha com spend + follows.
    ig_posts_by_campaign = {}
    for ad in fb_ads_ig_posts:
        camp_id = ad.get('campaign_id', '')
        camp_name = ad.get('campaign_name', '') or 'Sem nome'
        key = camp_id or camp_name
        if key not in ig_posts_by_campaign:
            ig_posts_by_campaign[key] = {
                'campaign_id':       camp_id,
                'campaign_name':     camp_name,
                'campaign_status':   ad.get('campaign_status', 'UNKNOWN'),
                'spend':             0.0,
                'instagram_follows': 0,
                'impressions':       0,
                'clicks':            0,
            }
        slot = ig_posts_by_campaign[key]
        slot['spend']             += float(ad.get('spend', 0) or 0)
        slot['instagram_follows'] += int(ad.get('instagram_follows', 0) or 0)
        slot['impressions']       += int(ad.get('impressions', 0) or 0)
        slot['clicks']            += int(ad.get('clicks', 0) or 0)
        if ad.get('campaign_status') == 'ACTIVE':
            slot['campaign_status'] = 'ACTIVE'

    instagram_posts_consolidated = sorted(
        ig_posts_by_campaign.values(),
        key=lambda x: -x['spend']
    )
    for p in instagram_posts_consolidated:
        p['cost_per_follow'] = round(p['spend'] / p['instagram_follows'], 2) if p['instagram_follows'] > 0 else 0.0
        p['spend'] = round(p['spend'], 2)

    return {
        'ads_consolidated': ads_consolidated,
        'adsets_consolidated': adsets_consolidated,
        'campaigns_consolidated': campaigns_consolidated,
        'instagram_posts_consolidated': instagram_posts_consolidated,
        'organicos':             organico_metrics,
        'total_leads':           len(leads_enriquecidos),
        'total_mqls':            mqls_in_period_count,
        'total_wons':            len(wons_rows),
        'fat_total_sheets':      round(fat_total_sheets, 2),
        'by_date':               by_date,
        'by_produto':            by_produto,
        'by_date_per_campaign':  by_date_per_campaign,
        'by_date_per_adset':     by_date_per_adset,
        'by_date_per_ad':        by_date_per_ad,
        'funnel':                funnel,
        'daily_funnel':          daily_funnel,
    }

# ── Helpers ───────────────────────────────────────────────────────────────────
def _is_produto_a(produto_str):
    """Determina se o produto é A (Negócios Creators) ou B (Outros)."""
    if not produto_str:
        return False
    import unicodedata
    s = ''.join(c for c in unicodedata.normalize('NFD', str(produto_str)) if unicodedata.category(c) != 'Mn')
    return 'negocio' in s.lower()

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
        'link_clicks': 0, 'landing_page_views': 0, 'typeform_submits': 0,
        'leads_total': 0, 'leads_a': 0, 'leads_b': 0,
        'vendas_a': 0, 'vendas_b': 0,
        'fat_a': 0.0, 'fat_b': 0.0, 'fat_total': 0.0,
    }

def _calc_metrics(ad, leads):
    """Calcula métricas para um ad + lista de leads associados."""
    m = _empty_metrics()
    m['spend']               = ad['spend']
    m['impressions']         = ad['impressions']
    m['clicks']              = ad['clicks']
    m['link_clicks']         = ad.get('link_clicks', 0)
    m['landing_page_views']  = ad.get('landing_page_views', 0)
    m['typeform_submits']    = ad.get('typeform_submits', 0)

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
    for key in ('spend', 'impressions', 'clicks', 'link_clicks', 'landing_page_views',
                'typeform_submits', 'leads_total', 'leads_a', 'leads_b',
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
    return render_template('cruzamento.html', client_mode=False)


@cruzamento_bp.route('/cruzamento/vinci')
def cruzamento_vinci_page():
    from app import obter_token
    # O token persistente do sistema já é retornado automaticamente
    token = obter_token()
    if not token:
        return "Sistema não autenticado pelo administrador. Por favor, contate o suporte.", 403
    return render_template('cruzamento.html', client_mode=True, client_name='Vinci', locked_period='this_month')


@cruzamento_bp.route('/api/cruzamento/data')
def api_cruzamento_data():
    from flask import Response, stream_with_context
    from app import obter_token
    token = obter_token()
    if not token:
        return jsonify({'success': False, 'error': 'Não autenticado'}), 401

    date_preset = request.args.get('date_preset', 'last_7_days')
    since       = request.args.get('since')
    until       = request.args.get('until')

    def _sse(stage, payload):
        return f"data: {json.dumps({'stage': stage, **payload})}\n\n"

    def generate():
        try:
            t0 = time.time()

            yield _sse('status', {'message': 'Buscando Facebook Ads + Google Sheets...'})

            # ── Google Ads: verificar se está configurado para este user ──
            google_ads_future = None
            google_ads_enabled = False
            try:
                from modules.google_ads import (
                    is_google_ads_configured, get_google_ads_config_from_db,
                    fetch_google_ads_insights, _get_valid_token, save_google_ads_config,
                )
                user_id    = session.get('user_id')
                account_id = session.get('account_id', '')

                # Fallback para acesso anônimo (ex: /cruzamento/vinci sem login ativo).
                # O admin deve configurar CRUZAMENTO_USER_ID e CRUZAMENTO_ACCOUNT_ID no
                # deploy.sh para que visitantes sem sessão também vejam dados do Google Ads.
                if not user_id:
                    user_id = os.environ.get('CRUZAMENTO_USER_ID')
                if not account_id:
                    account_id = os.environ.get('CRUZAMENTO_ACCOUNT_ID', '')

                if is_google_ads_configured() and user_id:
                    ga_config = get_google_ads_config_from_db(user_id, account_id)
                    if ga_config:
                        ga_token, ga_config_updated = _get_valid_token(ga_config)
                        if ga_token:
                            google_ads_enabled = True
                            # Salvar token renovado se mudou
                            if ga_config_updated and ga_config_updated.get('access_token') != ga_config.get('access_token'):
                                save_google_ads_config(user_id, account_id, ga_config_updated)
            except Exception as ga_init_err:
                print(f"[cruzamento] Google Ads init check failed (non-blocking): {ga_init_err}")

            # Fetch paralelo com keepalive — envia heartbeat a cada 3s
            # para evitar que proxy/browser cortem a conexão por inatividade
            with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
                fb_future     = executor.submit(fetch_fb_insights, AD_ACCOUNT_ID, token, date_preset, since, until)
                sheets_future = executor.submit(fetch_sheets_data, SPREADSHEET_ID)
                status_future = executor.submit(fetch_ads_status, AD_ACCOUNT_ID, token)
                camp_status_future = executor.submit(fetch_campaigns_status, AD_ACCOUNT_ID, token)
                adset_status_future = executor.submit(fetch_adsets_status, AD_ACCOUNT_ID, token)

                # VINCI daily (Google Ads via planilha pública) — usado para mesclar no Painel Diário
                _since_d_pre, _until_d_pre = preset_to_dates(date_preset, since, until)
                vinci_daily_future = executor.submit(fetch_vinci_daily, _since_d_pre, _until_d_pre)

                # Google Ads fetch em paralelo (se configurado)
                if google_ads_enabled:
                    since_d, until_d = preset_to_dates(date_preset, since, until)
                    ga_since = str(since_d) if since_d else str((_date.today() - timedelta(days=29)))
                    ga_until = str(until_d) if until_d else str(_date.today())
                    google_ads_future = executor.submit(
                        fetch_google_ads_insights, ga_token, ga_config['customer_id'],
                        ga_since, ga_until
                    )
                    yield _sse('status', {'message': 'Buscando Facebook Ads + Google Ads + Google Sheets...'})

                futures = [fb_future, sheets_future, status_future, camp_status_future, adset_status_future, vinci_daily_future]
                if google_ads_future:
                    futures.append(google_ads_future)
                while not all(f.done() for f in futures):
                    yield ": keepalive\n\n"
                    time.sleep(3)

                fb_ads                       = fb_future.result()
                mqls_rows_all, wons_rows_all = sheets_future.result()
                status_map                   = status_future.result()
                camp_status_map              = camp_status_future.result()
                adset_status_map             = adset_status_future.result()

                # ── Merge Google Ads data (soma ao fb_ads) ──
                google_ads_count = 0
                if google_ads_future:
                    try:
                        google_ads_data = google_ads_future.result()
                        if google_ads_data:
                            google_ads_count = len(google_ads_data)
                            fb_ads.extend(google_ads_data)
                            print(f"[cruzamento] Google Ads: {google_ads_count} registros somados aos {len(fb_ads) - google_ads_count} do Meta")
                    except Exception as ga_err:
                        print(f"[cruzamento] Google Ads fetch failed (non-blocking): {ga_err}")

                # VINCI daily (Google via planilha) — usado no Painel Diário para mesclar spend/clicks
                vinci_daily = {}
                try:
                    vinci_daily = vinci_daily_future.result() or {}
                    if vinci_daily:
                        print(f"[cruzamento] VINCI daily: {len(vinci_daily)} dias de Google Ads (planilha) para merge no painel")
                except Exception as vd_err:
                    print(f"[cruzamento] VINCI daily fetch falhou (non-blocking): {vd_err}")

            yield _sse('status', {'message': f'Processando {len(fb_ads)} registros...'})
            yield ": keepalive\n\n"

            for ad in fb_ads:
                ad['ad_status'] = status_map.get(ad.get('ad_id'), 'UNKNOWN')
                ad['campaign_status'] = camp_status_map.get(ad.get('campaign_id'), 'UNKNOWN')
                ad['adset_status'] = adset_status_map.get(ad.get('adset_id'), 'UNKNOWN')

            since_d, until_d = preset_to_dates(date_preset, since, until)
            mqls_rows = filter_rows_by_date(mqls_rows_all, 'Data do preenchimento', since_d, until_d)
            wons_rows = filter_rows_by_date(wons_rows_all, 'Data de fechamento', since_d, until_d)

            # ── Filtro Facebook-only: exclui MQLs e WONs do Google/AdWords ────────
            # A aba "Facebook Ads" só deve contabilizar leads originados no Meta.
            # utm_source = 'adwords' ou 'google' indica origem no Google Ads.
            _GOOGLE_SOURCES = {'adwords', 'google'}

            # Deal IDs de todos os MQLs do Google (histórico completo, não só o período)
            # para garantir que WONs desse período vindas de leads antigos do Google
            # também sejam excluídas.
            _google_deal_ids = {
                _norm(r.get('Deal ID', ''))
                for r in mqls_rows_all
                if _norm(r.get('utm_source', '')) in _GOOGLE_SOURCES
            }

            mqls_rows = [r for r in mqls_rows
                         if _norm(r.get('utm_source', '')) not in _GOOGLE_SOURCES]
            wons_rows = [r for r in wons_rows
                         if _norm(r.get('Deal ID', '')) not in _google_deal_ids]
            # ─────────────────────────────────────────────────────────────────────

            # Processa em thread separada com heartbeat para manter conexão viva
            _resultado_box = [None, None]  # [resultado, error]
            def _process():
                try:
                    _resultado_box[0] = processar_cruzamento(fb_ads, mqls_rows, wons_rows, mqls_all=mqls_rows_all)
                except Exception as e:
                    _resultado_box[1] = e

            proc_thread = threading.Thread(target=_process, daemon=True)
            proc_thread.start()
            while proc_thread.is_alive():
                yield ": keepalive\n\n"
                proc_thread.join(timeout=3)

            if _resultado_box[1]:
                raise _resultado_box[1]
            resultado = _resultado_box[0]
            elapsed = round(time.time() - t0, 2)

            # Envia cada seção como evento SSE separado
            yield _sse('kpis', {
                'ads_consolidated': resultado['ads_consolidated'],
                'total_mqls':       resultado['total_mqls'],
                'fat_total_sheets': resultado['fat_total_sheets'],
            })
            yield ": keepalive\n\n"

            yield _sse('funnel', {'funnel': resultado.get('funnel')})

            # ── Merge VINCI daily (Google Ads via planilha) no Painel Diário ──
            # Só mescla `spend` e `link_clicks` — são as únicas métricas que o Google
            # fornece na planilha. Impressões, LPV e TypeForm submits continuam
            # Meta-only (Google não expõe esses dados via planilha), por isso
            # CTR/connect_rate/taxa_lead/taxa_mql NÃO são recalculados para evitar
            # distorções (dividir clicks totais por impressões só do Meta seria incorreto).
            daily_funnel = resultado.get('daily_funnel', [])
            existing_dates = {entry['date'] for entry in daily_funnel}
            # 1) Soma em dias que já existem em daily_funnel
            for entry in daily_funnel:
                gd = vinci_daily.get(entry['date'])
                if gd:
                    entry['spend']       = round(entry.get('spend', 0.0) + gd.get('spend', 0.0), 2)
                    entry['link_clicks'] = entry.get('link_clicks', 0) + gd.get('clicks', 0)
                    entry['has_google']  = True
            # 2) Acrescenta dias do Google que não apareceram no Meta/Sheet (raro mas possível)
            for dk, gd in vinci_daily.items():
                if dk not in existing_dates:
                    daily_funnel.append({
                        'date':          dk,
                        'spend':         round(gd.get('spend', 0.0), 2),
                        'impressions':   0,
                        'link_clicks':   gd.get('clicks', 0),
                        'lpv':           0,
                        'typeform':      0,
                        'mqls':          0,
                        'ctr':           None,
                        'connect_rate':  None,
                        'taxa_lead':     None,
                        'taxa_mql':      None,
                        'has_google':    True,
                    })
            daily_funnel.sort(key=lambda e: e['date'])

            yield _sse('panel', {
                'daily_funnel': daily_funnel,
                'google_merged': bool(vinci_daily),
            })
            yield ": keepalive\n\n"

            yield _sse('charts', {
                'by_produto': resultado.get('by_produto', {}),
                'by_date':    resultado.get('by_date', []),
            })
            yield ": keepalive\n\n"

            yield _sse('campaigns', {
                'campaigns_consolidated': resultado['campaigns_consolidated'],
            })

            yield _sse('adsets', {
                'adsets_consolidated': resultado['adsets_consolidated'],
            })

            yield _sse('ads', {
                'ads_consolidated': resultado['ads_consolidated'],
            })
            yield ": keepalive\n\n"

            yield _sse('instagram_posts', {
                'instagram_posts_consolidated': resultado.get('instagram_posts_consolidated', []),
            })
            yield ": keepalive\n\n"

            yield _sse('timeline', {
                'by_date':              resultado.get('by_date', []),
                'by_date_per_campaign': resultado.get('by_date_per_campaign', {}),
                'by_date_per_adset':    resultado.get('by_date_per_adset', {}),
                'by_date_per_ad':       resultado.get('by_date_per_ad', {}),
            })

            yield _sse('done', {
                'meta': {
                    'fb_ads_count': len(fb_ads),
                    'google_ads_count': google_ads_count,
                    'google_ads_enabled': google_ads_enabled,
                    'mqls_count':   resultado['total_mqls'],
                    'wons_count':   resultado['total_wons'],
                    'elapsed_sec':  elapsed,
                    'date_preset':  date_preset,
                    'timestamp':    datetime.now(_BR_TZ).isoformat(),
                }
            })

        except Exception as e:
            import traceback
            traceback.print_exc()
            yield _sse('error', {'message': str(e)})

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'X-Accel-Buffering': 'no', 'Cache-Control': 'no-cache'}
    )


@cruzamento_bp.route('/api/cruzamento/filters', methods=['GET', 'POST'])
def api_cruzamento_filters():
    """
    GET: retorna lista atual de padrões excluídos.
    POST: salva nova lista. Body: {"patterns": ["[TAG1]", "[TAG2]"]}.
    """
    from app import obter_token
    if not obter_token():
        return jsonify({'success': False, 'error': 'Não autenticado'}), 401

    if request.method == 'GET':
        return jsonify({
            'success': True,
            'patterns': load_excluded_patterns(),
            'defaults': DEFAULT_EXCLUDED_PATTERNS,
            'instagram_post_prefix': INSTAGRAM_POST_PREFIX,
        })

    # POST
    body = request.get_json(silent=True) or {}
    patterns = body.get('patterns', [])
    if not isinstance(patterns, list):
        return jsonify({'success': False, 'error': 'patterns deve ser uma lista'}), 400

    ok, saved = save_excluded_patterns(patterns)
    if not ok:
        return jsonify({'success': False, 'error': 'Falha ao salvar filtros'}), 500
    return jsonify({'success': True, 'patterns': saved})


@cruzamento_bp.route('/api/cruzamento/action-types')
def api_action_types():
    """
    Endpoint de diagnóstico: lista todos os action_types únicos encontrados
    nos insights da conta, junto com o total de cada um.
    Útil para descobrir o nome exato de eventos personalizados (ex: TypeformSubmit).
    """
    from app import obter_token
    token = obter_token()
    if not token:
        return jsonify({'success': False, 'error': 'Não autenticado'}), 401

    date_preset = request.args.get('date_preset', 'last_30_days')
    since = request.args.get('since')
    until = request.args.get('until')

    try:
        # Converte preset interno (last_30_days) em datas reais (Meta API exige last_30d ou time_range)
        since_d, until_d = preset_to_dates(date_preset, since, until)
        if since_d is None or until_d is None:
            # Fallback seguro: últimos 30 dias
            from datetime import date as _d
            until_d = _d.today()
            since_d = until_d - timedelta(days=29)

        base_url = f"https://graph.facebook.com/v22.0/{AD_ACCOUNT_ID}/insights"
        params = {
            'access_token': token,
            'level': 'ad',
            'fields': 'ad_id,actions',
            'limit': 500,
            'time_increment': 1,
            'time_range': json.dumps({'since': str(since_d), 'until': str(until_d)}),
        }

        totals = {}
        url = base_url
        while url:
            resp = requests.get(url, params=params if url == base_url else None, timeout=30)
            resp.raise_for_status()
            body = resp.json()

            for item in body.get('data', []):
                for a in item.get('actions', []):
                    at = a.get('action_type', '')
                    val = int(float(a.get('value', 0) or 0))
                    totals[at] = totals.get(at, 0) + val

            url = body.get('paging', {}).get('next')
            params = None

        # Ordena por valor desc e formata
        sorted_types = sorted(totals.items(), key=lambda x: -x[1])

        return jsonify({
            'success': True,
            'date_preset': date_preset,
            'action_types': [
                {'action_type': at, 'total': v,
                 'typeform_match': 'typeform' in at.lower()}
                for at, v in sorted_types
            ],
            'typeform_action_type_env': TYPEFORM_ACTION_TYPE or '(auto-detect)',
            'hint': 'Procure o action_type com typeform_match=true e configure TYPEFORM_ACTION_TYPE no VPS se necessário.'
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
