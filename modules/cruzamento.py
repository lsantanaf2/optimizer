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
                'date_start':         item.get('date_start', ''),
            })

        # Paginação cursor
        paging = body.get('paging', {})
        next_url = paging.get('next')
        url = next_url if next_url else None
        params = None  # next_url já tem params embutidos

    return ads

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

# ── Processamento: Duplo Join em Memória ──────────────────────────────────────
def processar_cruzamento(fb_ads, mqls_rows, wons_rows, mqls_all=None):
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

    # Guarda contagem de MQLs dentro do período (para total_mqls no retorno)
    mqls_in_period_count = len(leads_enriquecidos)

    # ── Recuperar wons do período cujo MQL foi criado ANTES do período ─────────
    # Wons filtradas por "Data de fechamento" podem referenciar um MQL
    # criado fora do filtro → não aparecem em leads_enriquecidos acima,
    # mas devem ser atribuídos à sua campanha/adset/ad para consistência.
    if mqls_all:
        leads_ids_in_period = {l['deal_id'] for l in leads_enriquecidos}
        mqls_all_by_deal = {
            _norm(row.get('Deal ID', '')): row
            for row in mqls_all
            if _norm(row.get('Deal ID', ''))
        }
        for deal_id, won_data in wons_idx.items():
            if deal_id not in leads_ids_in_period:
                mql_row = mqls_all_by_deal.get(deal_id)
                if mql_row:
                    produto = _norm(mql_row.get('Produto indicado', ''))
                    is_a = _is_produto_a(produto)
                    leads_enriquecidos.append({
                        'deal_id':      deal_id,
                        'produto':      produto,
                        'is_a':         is_a,
                        'utm_campaign': _norm(mql_row.get('utm_campaign', '')),
                        'utm_content':  _norm(mql_row.get('utm_content', '')),
                        'utm_medium':   _norm(mql_row.get('utm_medium', '')),
                        'utm_source':   _norm(mql_row.get('utm_source', '')),
                        'utm_term':     _norm(mql_row.get('utm_term', '')),
                        'vendeu':       True,
                        'valor_venda':  won_data['valor'],
                    })

    # ── Passo 2: Indexar leads ─────────────────
    leads_by_term = {}
    leads_by_content = {}
    leads_by_campaign = {}

    for lead in leads_enriquecidos:
        ut = lead['utm_term']
        if ut and ut != 'null':
            leads_by_term.setdefault(ut, []).append(lead)
            
        uc = lead['utm_content']
        if uc and uc != 'null':
            leads_by_content.setdefault(uc, []).append(lead)
            
        ucamp = lead['utm_campaign']
        if ucamp and ucamp != 'null':
            leads_by_campaign.setdefault(ucamp, []).append(lead)

    # ── Passo 2: Consolidar FB Ads, AdSets e Campaigns ────────────────────────────────
    # IDs são armazenados para join fallback (UTM pode conter ID em vez de nome)
    fb_ads_by_name = {}
    fb_adsets_by_name = {}
    fb_campaigns_by_name = {}

    for ad in fb_ads:
        name_norm     = _norm(ad.get('ad_name', ''))
        adset_norm    = _norm(ad.get('adset_name', ''))
        campaign_norm = _norm(ad.get('campaign_name', ''))

        ad_id_raw      = ad.get('ad_id', '')
        adset_id_raw   = ad.get('adset_id', '')
        campaign_id_raw= ad.get('campaign_id', '')

        # Consolidação Ad — chave composta (campaign, adset, nome) para preservar
        # o mesmo criativo em adsets diferentes como entradas separadas.
        # O matching de leads continua usando apenas name_norm (utm_term).
        if name_norm:
            ad_key = f"{campaign_norm}|||{adset_norm}|||{name_norm}"
            if ad_key not in fb_ads_by_name:
                fb_ads_by_name[ad_key] = {
                    'ad_name':            ad.get('ad_name', 'Desconhecido'),
                    'campaign_name':      ad.get('campaign_name', ''),
                    'adset_name':         ad.get('adset_name', ''),
                    'ad_status':          ad.get('ad_status', 'UNKNOWN'),
                    'ad_ids':             set(),
                    '_name_norm':         name_norm,
                    'spend':              0.0,
                    'impressions':        0,
                    'clicks':             0,
                    'link_clicks':        0,
                    'landing_page_views': 0,
                    'typeform_submits':   0,
                }
            if ad_id_raw:
                fb_ads_by_name[ad_key]['ad_ids'].add(ad_id_raw)
            fb_ads_by_name[ad_key]['spend']               += ad.get('spend', 0.0)
            fb_ads_by_name[ad_key]['impressions']         += ad.get('impressions', 0)
            fb_ads_by_name[ad_key]['clicks']              += ad.get('clicks', 0)
            fb_ads_by_name[ad_key]['link_clicks']         += ad.get('link_clicks', 0)
            fb_ads_by_name[ad_key]['landing_page_views']  += ad.get('landing_page_views', 0)
            fb_ads_by_name[ad_key]['typeform_submits']    += ad.get('typeform_submits', 0)

        # Consolidação AdSet
        if adset_norm:
            if adset_norm not in fb_adsets_by_name:
                fb_adsets_by_name[adset_norm] = {
                    'adset_name':         ad.get('adset_name', 'Desconhecido'),
                    'campaign_name':      ad.get('campaign_name', ''),
                    'adset_id':           adset_id_raw,
                    'ad_status':          ad.get('ad_status', 'PAUSED'),
                    'spend':              0.0,
                    'impressions':        0,
                    'clicks':             0,
                    'link_clicks':        0,
                    'landing_page_views': 0,
                    'typeform_submits':   0,
                }
            if ad.get('ad_status') == 'ACTIVE':
                fb_adsets_by_name[adset_norm]['ad_status'] = 'ACTIVE'
            fb_adsets_by_name[adset_norm]['spend']               += ad.get('spend', 0.0)
            fb_adsets_by_name[adset_norm]['impressions']         += ad.get('impressions', 0)
            fb_adsets_by_name[adset_norm]['clicks']              += ad.get('clicks', 0)
            fb_adsets_by_name[adset_norm]['link_clicks']         += ad.get('link_clicks', 0)
            fb_adsets_by_name[adset_norm]['landing_page_views']  += ad.get('landing_page_views', 0)
            fb_adsets_by_name[adset_norm]['typeform_submits']    += ad.get('typeform_submits', 0)

        # Consolidação Campaign
        if campaign_norm:
            if campaign_norm not in fb_campaigns_by_name:
                fb_campaigns_by_name[campaign_norm] = {
                    'campaign_name':      ad.get('campaign_name', 'Desconhecida'),
                    'campaign_id':        campaign_id_raw,
                    'ad_status':          ad.get('ad_status', 'PAUSED'),
                    'spend':              0.0,
                    'impressions':        0,
                    'clicks':             0,
                    'link_clicks':        0,
                    'landing_page_views': 0,
                    'typeform_submits':   0,
                }
            if ad.get('ad_status') == 'ACTIVE':
                fb_campaigns_by_name[campaign_norm]['ad_status'] = 'ACTIVE'
            fb_campaigns_by_name[campaign_norm]['spend']               += ad.get('spend', 0.0)
            fb_campaigns_by_name[campaign_norm]['impressions']         += ad.get('impressions', 0)
            fb_campaigns_by_name[campaign_norm]['clicks']              += ad.get('clicks', 0)
            fb_campaigns_by_name[campaign_norm]['link_clicks']         += ad.get('link_clicks', 0)
            fb_campaigns_by_name[campaign_norm]['landing_page_views']  += ad.get('landing_page_views', 0)
            fb_campaigns_by_name[campaign_norm]['typeform_submits']    += ad.get('typeform_submits', 0)

    def _join_leads(index, name_key, *id_keys):
        """
        Junta leads pelo nome e também por IDs alternativos (fallback).
        Evita duplicatas por deal_id — resolve o caso em que o UTM foi
        configurado com o ID da entidade em vez do nome.
        """
        combined = list(index.get(name_key, []))
        seen = {l['deal_id'] for l in combined}
        for ik in id_keys:
            if ik:
                for l in index.get(ik, []):
                    if l['deal_id'] not in seen:
                        combined.append(l)
                        seen.add(l['deal_id'])
        return combined

    # ── Passo 3: Cruzar FB com Leads ───────────────────────────
    ads_consolidated       = []
    adsets_consolidated    = []
    campaigns_consolidated = []
    matched_deal_ids_ads       = set()
    matched_deal_ids_adsets    = set()
    matched_deal_ids_campaigns = set()

    # ── 3.1: Cruzar Ads (utm_term ↔ ad_name ou ad_id, refinado por utm_content → adset)
    # Agrupa compound entries por name_norm para distribuir leads sem duplicar
    from collections import defaultdict as _defaultdict
    _ads_by_name_norm = _defaultdict(list)
    for _ak, _ad in fb_ads_by_name.items():
        _ads_by_name_norm[_ad['_name_norm']].append(_ad)

    for _nn, _entries in _ads_by_name_norm.items():
        # Coleta todos os ad_ids do grupo para matching por ID
        _all_ids = set()
        for _e in _entries:
            _all_ids.update(_e['ad_ids'])
        _all_leads = _join_leads(leads_by_term, _nn, *_all_ids)

        # Fase 1: atribui leads com utm_content === adset_norm
        _entry_leads = {id(_e): [] for _e in _entries}
        _assigned = set()
        for _e in _entries:
            _as_norm = _norm(_e.get('adset_name', ''))
            for _l in _all_leads:
                if _l['deal_id'] not in _assigned and _l.get('utm_content', '') == _as_norm:
                    _entry_leads[id(_e)].append(_l)
                    _assigned.add(_l['deal_id'])

        # Fase 2: leads sem utm_content matchando → vão para a entry de maior spend
        _unassigned = [_l for _l in _all_leads if _l['deal_id'] not in _assigned]
        if _unassigned:
            _best = max(_entries, key=lambda e: e.get('spend', 0))
            _entry_leads[id(_best)].extend(_unassigned)
            _assigned.update(_l['deal_id'] for _l in _unassigned)

        # Fase 3: consolida cada entry
        for _e in _entries:
            _my_leads = _entry_leads[id(_e)]
            metrics = _calc_metrics(_e, _my_leads)
            ads_consolidated.append({
                'ad_name':       _e['ad_name'],
                'campaign_name': _e['campaign_name'],
                'adset_name':    _e['adset_name'],
                'ad_status':     _e['ad_status'],
                **metrics
            })
            for _l in _my_leads:
                matched_deal_ids_ads.add(_l['deal_id'])

    # ── 3.2: Cruzar AdSets (utm_content ↔ adset_name ou adset_id)
    for name_norm, adset_data in fb_adsets_by_name.items():
        m_leads = _join_leads(leads_by_content, name_norm, adset_data.get('adset_id', ''))
        metrics = _calc_metrics(adset_data, m_leads)
        adsets_consolidated.append({
            'adset_name':    adset_data['adset_name'],
            'campaign_name': adset_data['campaign_name'],
            'ad_status':     adset_data['ad_status'],
            **metrics
        })
        for lead in m_leads:
            matched_deal_ids_adsets.add(lead['deal_id'])

    # ── 3.3: Cruzar Campaigns (utm_campaign ↔ campaign_name ou campaign_id)
    for name_norm, camp_data in fb_campaigns_by_name.items():
        m_leads = _join_leads(leads_by_campaign, name_norm, camp_data.get('campaign_id', ''))
        metrics = _calc_metrics(camp_data, m_leads)
        campaigns_consolidated.append({
            'campaign_name': camp_data['campaign_name'],
            'ad_status':     camp_data['ad_status'],
            **metrics
        })
        for lead in m_leads:
            matched_deal_ids_campaigns.add(lead['deal_id'])

    organicos_ads = [l for l in leads_enriquecidos if l['deal_id'] not in matched_deal_ids_ads]
    organicos_adsets = [l for l in leads_enriquecidos if l['deal_id'] not in matched_deal_ids_adsets]
    organicos_campaigns = [l for l in leads_enriquecidos if l['deal_id'] not in matched_deal_ids_campaigns]
    
    organico_metrics = _calc_organic_metrics(organicos_ads) # Keep backward compatibility

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

    # ── Breakdown diário por entidade (pré-indexado O(n)) ──────────────────────
    def _build_entity_series(spend_map, mqls_map):
        all_keys = sorted(set(list(spend_map.keys()) + list(mqls_map.keys())))
        result = []
        for dk in all_keys:
            sp = round(spend_map.get(dk, 0.0), 2)
            mq = mqls_map.get(dk, 0)
            result.append({
                'date':  dk,
                'mqls':  mq,
                'spend': sp,
                'cpl':   round(sp / mq, 2) if mq > 0 and sp > 0 else None,
            })
        return result

    # Pré-indexar fb_ads por (campaign, date), (adset, date), (ad+adset, date) — O(n) único
    _spend_by_camp_date = {}
    _spend_by_adset_date = {}
    _spend_by_ad_adset_date = {}
    for ad in fb_ads:
        d = ad.get('date_start', '')
        if not d:
            continue
        sp = ad.get('spend', 0.0)
        cn = _norm(ad.get('campaign_name', ''))
        an = _norm(ad.get('adset_name', ''))
        adn = _norm(ad.get('ad_name', ''))

        k1 = (cn, d)
        _spend_by_camp_date[k1] = _spend_by_camp_date.get(k1, 0.0) + sp
        k2 = (an, d)
        _spend_by_adset_date[k2] = _spend_by_adset_date.get(k2, 0.0) + sp
        k3 = (adn, an, d)
        _spend_by_ad_adset_date[k3] = _spend_by_ad_adset_date.get(k3, 0.0) + sp

    # Pré-indexar mqls por utm_campaign/content/term → date — O(n) único
    _mqls_by_camp_date = {}
    _mqls_by_content_date = {}
    _mqls_by_term_date = {}
    for row in mqls_rows:
        d = _parse_date_br(row.get('Data do preenchimento', ''))
        if not d:
            continue
        dk = d.strftime('%Y-%m-%d')
        uc = _norm(row.get('utm_campaign', ''))
        if uc:
            k = (uc, dk)
            _mqls_by_camp_date[k] = _mqls_by_camp_date.get(k, 0) + 1
        ucont = _norm(row.get('utm_content', ''))
        if ucont:
            k = (ucont, dk)
            _mqls_by_content_date[k] = _mqls_by_content_date.get(k, 0) + 1
        ut = _norm(row.get('utm_term', ''))
        if ut:
            k = (ut, dk)
            _mqls_by_term_date[k] = _mqls_by_term_date.get(k, 0) + 1

    # Helpers para extrair séries de datas dos índices pré-construídos
    def _extract_spend_2key(idx, key_prefix):
        return {d: v for (k, d), v in idx.items() if k == key_prefix}

    def _extract_spend_3key(idx, k1, k2):
        return {d: v for (a, b, d), v in idx.items() if a == k1 and b == k2}

    def _extract_mqls_2key(idx, key_prefix):
        return {d: v for (k, d), v in idx.items() if k == key_prefix}

    # Por campanha — O(campaigns)
    by_date_per_campaign = {}
    for camp_norm, camp_data in fb_campaigns_by_name.items():
        ds = _extract_spend_2key(_spend_by_camp_date, camp_norm)
        dm = _extract_mqls_2key(_mqls_by_camp_date, camp_norm)
        by_date_per_campaign[camp_data['campaign_name']] = _build_entity_series(ds, dm)

    # Por conjunto — O(adsets)
    by_date_per_adset = {}
    for adset_norm, adset_data in fb_adsets_by_name.items():
        ds = _extract_spend_2key(_spend_by_adset_date, adset_norm)
        dm = _extract_mqls_2key(_mqls_by_content_date, adset_norm)
        by_date_per_adset[adset_data['adset_name']] = _build_entity_series(ds, dm)

    # Por anúncio — O(ads)
    by_date_per_ad = {}
    for _ad_key, ad_data_item in fb_ads_by_name.items():
        ad_name_norm = ad_data_item['_name_norm']
        adset_name_item = _norm(ad_data_item.get('adset_name', ''))
        ds = _extract_spend_3key(_spend_by_ad_adset_date, ad_name_norm, adset_name_item)
        dm = _extract_mqls_2key(_mqls_by_term_date, ad_name_norm)
        by_date_per_ad[ad_data_item['ad_name']] = _build_entity_series(ds, dm)

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

    return {
        'ads_consolidated': ads_consolidated,
        'adsets_consolidated': adsets_consolidated,
        'campaigns_consolidated': campaigns_consolidated,
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

            # Fetch paralelo com keepalive — envia heartbeat a cada 3s
            # para evitar que proxy/browser cortem a conexão por inatividade
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                fb_future     = executor.submit(fetch_fb_insights, AD_ACCOUNT_ID, token, date_preset, since, until)
                sheets_future = executor.submit(fetch_sheets_data, SPREADSHEET_ID)
                status_future = executor.submit(fetch_ads_status, AD_ACCOUNT_ID, token)

                futures = [fb_future, sheets_future, status_future]
                while not all(f.done() for f in futures):
                    yield ": keepalive\n\n"
                    time.sleep(3)

                fb_ads                       = fb_future.result()
                mqls_rows_all, wons_rows_all = sheets_future.result()
                status_map                   = status_future.result()

            yield _sse('status', {'message': f'Processando {len(fb_ads)} registros...'})
            yield ": keepalive\n\n"

            for ad in fb_ads:
                ad['ad_status'] = status_map.get(ad.get('ad_id'), 'UNKNOWN')

            since_d, until_d = preset_to_dates(date_preset, since, until)
            mqls_rows = filter_rows_by_date(mqls_rows_all, 'Data do preenchimento', since_d, until_d)
            wons_rows = filter_rows_by_date(wons_rows_all, 'Data de fechamento', since_d, until_d)

            resultado = processar_cruzamento(fb_ads, mqls_rows, wons_rows, mqls_all=mqls_rows_all)
            elapsed = round(time.time() - t0, 2)

            # Envia cada seção como evento SSE separado
            yield _sse('kpis', {
                'ads_consolidated': resultado['ads_consolidated'],
                'total_mqls':       resultado['total_mqls'],
                'fat_total_sheets': resultado['fat_total_sheets'],
            })

            yield _sse('funnel', {'funnel': resultado.get('funnel')})

            yield _sse('charts', {
                'by_produto': resultado.get('by_produto', {}),
                'by_date':    resultado.get('by_date', []),
            })

            yield _sse('campaigns', {
                'campaigns_consolidated': resultado['campaigns_consolidated'],
            })

            yield _sse('adsets', {
                'adsets_consolidated': resultado['adsets_consolidated'],
            })

            yield _sse('ads', {
                'ads_consolidated': resultado['ads_consolidated'],
            })

            yield _sse('timeline', {
                'by_date':              resultado.get('by_date', []),
                'by_date_per_campaign': resultado.get('by_date_per_campaign', {}),
                'by_date_per_adset':    resultado.get('by_date_per_adset', {}),
                'by_date_per_ad':       resultado.get('by_date_per_ad', {}),
            })

            yield _sse('done', {
                'meta': {
                    'fb_ads_count': len(fb_ads),
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
