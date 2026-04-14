"""
Google Ads Integration — Módulo para buscar dados de campanhas do Google Ads.

Fluxo:
  1. Usuário conecta via OAuth 2.0 (consent screen Google)
  2. Tokens (access + refresh) são salvos no banco (saved_assets['google_ads'])
  3. Na página de cruzamento, dados do Google Ads são buscados e SOMADOS
     aos dados do Meta Ads antes do processamento

Requisitos de configuração (env vars):
  - GOOGLE_ADS_CLIENT_ID       — Client ID do OAuth (Google Cloud Console)
  - GOOGLE_ADS_CLIENT_SECRET   — Client Secret do OAuth
  - GOOGLE_ADS_DEVELOPER_TOKEN — Developer Token (Google Ads API Center no MCC)
  - GOOGLE_ADS_LOGIN_CUSTOMER_ID — ID do MCC (Manager Account), sem hífens

Se qualquer uma dessas vars estiver ausente, o módulo opera em modo OFF
(todas as funções retornam gracefully sem erro).
"""

import os
import json
import time
import requests
from datetime import datetime

# ── Configuração ─────────────────────────────────────────────────────────────
GOOGLE_ADS_CLIENT_ID       = os.getenv('GOOGLE_ADS_CLIENT_ID', '')
GOOGLE_ADS_CLIENT_SECRET   = os.getenv('GOOGLE_ADS_CLIENT_SECRET', '')
GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN', '')
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID', '')

# Redirect URI para o OAuth callback
GOOGLE_ADS_REDIRECT_URI = os.getenv(
    'GOOGLE_ADS_REDIRECT_URI',
    'https://optimizer.xn--trfego-qta.com/callback/google-ads'
)

# Scopes necessários para ler dados do Google Ads
GOOGLE_ADS_SCOPES = 'https://www.googleapis.com/auth/adwords'

# API version
GOOGLE_ADS_API_VERSION = 'v18'


def is_google_ads_configured():
    """Retorna True se todas as variáveis de ambiente necessárias estão configuradas."""
    return bool(
        GOOGLE_ADS_CLIENT_ID
        and GOOGLE_ADS_CLIENT_SECRET
        and GOOGLE_ADS_DEVELOPER_TOKEN
    )


def get_google_ads_auth_url(state=''):
    """
    Gera URL de autorização OAuth para o usuário conectar Google Ads.
    Retorna a URL ou None se não configurado.
    """
    if not is_google_ads_configured():
        return None

    params = {
        'client_id': GOOGLE_ADS_CLIENT_ID,
        'redirect_uri': GOOGLE_ADS_REDIRECT_URI,
        'scope': GOOGLE_ADS_SCOPES,
        'response_type': 'code',
        'access_type': 'offline',     # Para receber refresh_token
        'prompt': 'consent',           # Forçar consent para sempre receber refresh_token
    }
    if state:
        params['state'] = state

    from urllib.parse import urlencode
    return f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"


def exchange_google_ads_code(code):
    """
    Troca authorization code por access_token + refresh_token.
    Retorna dict com tokens ou None em caso de erro.
    """
    if not is_google_ads_configured():
        return None

    resp = requests.post(
        'https://oauth2.googleapis.com/token',
        data={
            'code': code,
            'client_id': GOOGLE_ADS_CLIENT_ID,
            'client_secret': GOOGLE_ADS_CLIENT_SECRET,
            'redirect_uri': GOOGLE_ADS_REDIRECT_URI,
            'grant_type': 'authorization_code',
        },
        timeout=15,
    )

    if resp.status_code != 200:
        print(f"[google_ads] Erro ao trocar code: {resp.status_code} {resp.text}", flush=True)
        return {'error': True, 'status': resp.status_code, 'body': resp.text,
                'redirect_uri_usado': GOOGLE_ADS_REDIRECT_URI}

    data = resp.json()
    return {
        'access_token':  data.get('access_token'),
        'refresh_token': data.get('refresh_token'),
        'expires_at':    time.time() + data.get('expires_in', 3600),
        'token_type':    data.get('token_type', 'Bearer'),
    }


def refresh_google_ads_token(refresh_token):
    """
    Renova o access_token usando o refresh_token.
    Retorna dict atualizado ou None em caso de erro.
    """
    if not is_google_ads_configured() or not refresh_token:
        return None

    resp = requests.post(
        'https://oauth2.googleapis.com/token',
        data={
            'client_id': GOOGLE_ADS_CLIENT_ID,
            'client_secret': GOOGLE_ADS_CLIENT_SECRET,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token',
        },
        timeout=15,
    )

    if resp.status_code != 200:
        print(f"[google_ads] Erro ao refresh token: {resp.status_code} {resp.text}")
        return None

    data = resp.json()
    return {
        'access_token':  data.get('access_token'),
        'refresh_token': refresh_token,  # Google não retorna novo refresh_token no refresh
        'expires_at':    time.time() + data.get('expires_in', 3600),
        'token_type':    data.get('token_type', 'Bearer'),
    }


def _get_valid_token(google_ads_config):
    """
    Garante que temos um access_token válido.
    google_ads_config: dict com access_token, refresh_token, expires_at.
    Retorna (access_token, updated_config) ou (None, None) se falhar.
    """
    if not google_ads_config:
        return None, None

    access_token = google_ads_config.get('access_token')
    refresh_token = google_ads_config.get('refresh_token')
    expires_at = google_ads_config.get('expires_at', 0)

    # Se token ainda válido (com margem de 60s), usa direto
    if access_token and time.time() < expires_at - 60:
        return access_token, google_ads_config

    # Precisa renovar
    if not refresh_token:
        print("[google_ads] Sem refresh_token, não é possível renovar.")
        return None, None

    new_config = refresh_google_ads_token(refresh_token)
    if not new_config:
        return None, None

    return new_config['access_token'], new_config


def list_accessible_customers(access_token):
    """
    Lista TODOS os customer IDs que o usuário autenticado tem acesso direto.
    NÃO envia login-customer-id para retornar todas as contas (não só as do MCC).
    Retorna lista de customer IDs (strings sem hífens, ex: '1234567890').
    """
    if not access_token:
        return []

    # IMPORTANTE: Para listAccessibleCustomers, NÃO mandar login-customer-id.
    # Esse endpoint retorna todas as contas que o OAuth user tem acesso direto.
    headers = {
        'Authorization': f'Bearer {access_token}',
        'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
    }

    url = f'https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers:listAccessibleCustomers'

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            print(f"[google_ads] Erro ao listar customers: {resp.status_code} {resp.text}")
            return []
        data = resp.json()
        # Retorna ['customers/1234567890', ...] → extrair IDs
        return [r.split('/')[-1] for r in data.get('resourceNames', [])]
    except Exception as e:
        print(f"[google_ads] Erro ao listar customers: {e}")
        return []


def fetch_google_ads_insights(access_token, customer_id, since_str, until_str):
    """
    Busca insights de campanhas do Google Ads via REST API (GAQL).

    Retorna lista de dicts no MESMO formato que fetch_fb_insights() retorna:
    {
        'campaign_id', 'campaign_name', 'adset_id', 'adset_name',
        'ad_id', 'ad_name', 'spend', 'impressions', 'clicks',
        'link_clicks', 'landing_page_views', 'typeform_submits',
        'instagram_follows', 'date_start', 'source': 'google'
    }

    Mapeamento Google → Meta:
      - Campaign → Campaign
      - Ad Group → AdSet
      - Ad Group Ad → Ad
      - cost_micros / 1_000_000 → spend
      - clicks → clicks + link_clicks
      - conversions → typeform_submits (proxy — ambos são conversões)
    """
    if not access_token or not customer_id:
        return []

    headers = {
        'Authorization': f'Bearer {access_token}',
        'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
        'Content-Type': 'application/json',
    }
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        headers['login-customer-id'] = GOOGLE_ADS_LOGIN_CUSTOMER_ID

    # GAQL query — busca a nível de ad_group_ad (equivalente a ad-level no Meta)
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            ad_group_ad.ad.id,
            ad_group_ad.ad.name,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            segments.date
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{since_str}' AND '{until_str}'
          AND campaign.status != 'REMOVED'
          AND ad_group.status != 'REMOVED'
          AND ad_group_ad.status != 'REMOVED'
    """

    url = f'https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/{customer_id}/googleAds:searchStream'

    try:
        resp = requests.post(
            url,
            headers=headers,
            json={'query': query.strip()},
            timeout=60,
        )

        if resp.status_code != 200:
            error_msg = resp.text[:500]
            print(f"[google_ads] Erro na query ({resp.status_code}): {error_msg}")
            return []

        results = resp.json()
        ads = []

        # searchStream retorna array de batches
        for batch in results:
            for row in batch.get('results', []):
                campaign = row.get('campaign', {})
                ad_group = row.get('adGroup', {})
                ad_group_ad = row.get('adGroupAd', {}).get('ad', {})
                metrics = row.get('metrics', {})
                segments = row.get('segments', {})

                spend = float(metrics.get('costMicros', 0) or 0) / 1_000_000
                impressions = int(metrics.get('impressions', 0) or 0)
                clicks = int(metrics.get('clicks', 0) or 0)
                conversions = float(metrics.get('conversions', 0) or 0)

                ads.append({
                    'campaign_id':        str(campaign.get('id', '')),
                    'campaign_name':      f"[Google] {campaign.get('name', '')}",
                    'adset_id':           str(ad_group.get('id', '')),
                    'adset_name':         ad_group.get('name', ''),
                    'ad_id':              f"g_{ad_group_ad.get('id', '')}",
                    'ad_name':            ad_group_ad.get('name', ''),
                    'spend':              spend,
                    'impressions':        impressions,
                    'clicks':             clicks,
                    'link_clicks':        clicks,  # Google não diferencia; usa clicks
                    'landing_page_views': 0,       # Não disponível direto no Google Ads
                    'typeform_submits':   int(conversions),
                    'instagram_follows':  0,
                    'date_start':         segments.get('date', ''),
                    'source':             'google',
                })

        print(f"[google_ads] Buscados {len(ads)} registros do Google Ads (customer {customer_id})")
        return ads

    except requests.exceptions.Timeout:
        print(f"[google_ads] Timeout ao buscar insights (customer {customer_id})")
        return []
    except Exception as e:
        print(f"[google_ads] Erro ao buscar insights: {e}")
        return []


def fetch_google_ads_campaigns(access_token, customer_id):
    """
    Lista campanhas (ativas e pausadas) de um customer_id.
    Retorna lista de dicts: {id, name, status, channel_type, start_date, end_date}.
    """
    if not access_token or not customer_id:
        return []

    headers = {
        'Authorization': f'Bearer {access_token}',
        'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
        'Content-Type': 'application/json',
    }
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        headers['login-customer-id'] = GOOGLE_ADS_LOGIN_CUSTOMER_ID

    query = """
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            campaign.start_date,
            campaign.end_date
        FROM campaign
        WHERE campaign.status != 'REMOVED'
        ORDER BY campaign.status, campaign.name
    """

    url = f'https://googleads.googleapis.com/{GOOGLE_ADS_API_VERSION}/customers/{customer_id}/googleAds:searchStream'

    try:
        resp = requests.post(url, headers=headers, json={'query': query.strip()}, timeout=30)
        if resp.status_code != 200:
            print(f"[google_ads] Erro ao listar campanhas ({resp.status_code}): {resp.text[:500]}", flush=True)
            return []

        results = resp.json()
        campaigns = []
        for batch in results:
            for row in batch.get('results', []):
                c = row.get('campaign', {})
                campaigns.append({
                    'id':           str(c.get('id', '')),
                    'name':         c.get('name', ''),
                    'status':       c.get('status', ''),
                    'channel_type': c.get('advertisingChannelType', ''),
                    'start_date':   c.get('startDate', ''),
                    'end_date':     c.get('endDate', ''),
                })
        return campaigns
    except Exception as e:
        print(f"[google_ads] Erro ao listar campanhas: {e}", flush=True)
        return []


def get_google_ads_config_from_db(user_id, meta_account_id):
    """
    Lê configuração do Google Ads salva no banco (saved_assets['google_ads']).
    Retorna dict com tokens e customer_id ou None.
    """
    try:
        from modules.account_settings import get_settings_for_setup
        settings = get_settings_for_setup(user_id, meta_account_id)
        assets = settings.get('saved_assets', {})
        ga_config = assets.get('google_ads')
        if ga_config and ga_config.get('refresh_token') and ga_config.get('customer_id'):
            return ga_config
    except Exception as e:
        print(f"[google_ads] Erro ao ler config do banco: {e}")
    return None


def save_google_ads_config(user_id, meta_account_id, config):
    """
    Salva configuração do Google Ads no banco (saved_assets['google_ads']).
    config: dict com access_token, refresh_token, expires_at, customer_id.
    """
    try:
        from modules.account_settings import get_or_create_imported_account, _db_ok
        from modules.database import fetch_one, execute

        if not _db_ok():
            return False

        imported_id = get_or_create_imported_account(user_id, meta_account_id)
        if not imported_id:
            return False

        row = fetch_one(
            "SELECT saved_assets FROM ad_account_settings WHERE ad_account_id = %s",
            (imported_id,)
        )
        assets = (row['saved_assets'] or {}) if row else {}
        if isinstance(assets, str):
            assets = json.loads(assets)

        assets['google_ads'] = config

        execute(
            "UPDATE ad_account_settings SET saved_assets = %s WHERE ad_account_id = %s",
            (json.dumps(assets, ensure_ascii=False), imported_id)
        )
        return True
    except Exception as e:
        print(f"[google_ads] Erro ao salvar config: {e}")
        return False
