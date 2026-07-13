"""
Blueprint /dash — Dashboards multi-tenant com token anti-enum.

Acesso público: GET /dash/<slug>?t=<token>
Admin:          GET /dash  (listagem, requer login do sistema)

APIs tenant-aware (reutilizam connectors de cruzamento.py):
  GET  /api/dash/<slug>/data               — SSE principal
  GET  /api/dash/<slug>/filtros            — lista padrões excluídos
  POST /api/dash/<slug>/filtros            — salva padrões excluídos
  GET  /api/dash/<slug>/google-ads-sheets  — Google Ads via planilha pública
  GET  /api/dash/<slug>/consolidado        — visão consolidada multi-canal
"""

import os
import csv
import io
import json
import time
import logging
import threading
import concurrent.futures
from datetime import datetime, timedelta, date as _date
from zoneinfo import ZoneInfo

import requests as _req
from flask import (
    Blueprint, jsonify, render_template, request,
    Response, stream_with_context, session, abort, redirect, url_for,
)

from modules.meta_client import GRAPH_BASE
from modules.dashboard_clients import (
    get_client_by_token, list_clients,
    get_excluded_patterns, save_excluded_patterns as db_save_excluded,
)

logger = logging.getLogger(__name__)
_BR_TZ = ZoneInfo('America/Sao_Paulo')

dash_bp = Blueprint('dash', __name__)


# ─────────────────────────────────────────────────────────────────────────────
# Auth helpers
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_client(slug: str, token: str):
    """Valida token e retorna config do cliente, ou None."""
    if not slug or not token:
        return None
    client = get_client_by_token(token)
    if client and client.get('slug') == slug:
        return client
    return None


def _require_client(slug: str):
    """
    Extrai ?t=<token> do request e valida.
    Retorna client dict ou aborta com 403.
    Rate limit ANTES da validação: também frena tentativas de enumeração de token.
    """
    from modules.rate_limiter import check_rate_limit
    check_rate_limit(f'dash-api:{slug}')
    token = request.args.get('t', '').strip()
    if not token:
        abort(403, 'Token obrigatório (?t=<token>)')
    client = _resolve_client(slug, token)
    if not client:
        abort(403, 'Link inválido ou desabilitado')
    return client


# ─────────────────────────────────────────────────────────────────────────────
# Google Ads via planilha pública — versão genérica (não-VINCI)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_num(s):
    if not s:
        return 0.0
    s = str(s).strip().replace('%', '')
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0


def fetch_client_google_ads_daily(sheet_id, sheet_gid, filter_keyword, since_dt=None, until_dt=None):
    """
    Lê planilha pública Google Ads e retorna dict { 'YYYY-MM-DD': {'spend': float, 'clicks': int} }.
    Versão genérica de cruzamento.fetch_vinci_daily — aceita sheet_id/gid/keyword dinâmicos.
    Retorna {} silenciosamente em caso de erro.
    """
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={sheet_gid}'
    try:
        resp = _req.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f'[dash] fetch_client_google_ads_daily ({sheet_id}) falhou: {e}')
        return {}

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
                    elif any(x in kl for x in ('cost', 'custo', 'spend', 'gasto')):
                        headers_map['cost'] = k
                    elif 'click' in kl or 'clique' in kl:
                        headers_map['clicks'] = k

            date_col     = headers_map.get('date', 'Date (Segment)')
            campaign_col = headers_map.get('campaign', 'Campaign Name')
            cost_col     = headers_map.get('cost', 'Cost')
            clicks_col   = headers_map.get('clicks', 'Clicks')

            if filter_keyword:
                name = row.get(campaign_col, '').strip()
                if filter_keyword.upper() not in name.upper():
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
        logger.warning(f'[dash] parse falhou: {e}')
        return {}

    return result


def fetch_client_sheet_campaigns(sheet_id, sheet_gid, filter_keyword, since_dt=None, until_dt=None):
    """
    Versão genérica de _fetch_vinci_sheet em app.py.
    Retorna {'campaigns': [...], 'totals': {...}, 'error': str|None}
    """
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={sheet_gid}'
    try:
        resp = _req.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        return {'campaigns': [], 'totals': {'spend': 0.0, 'conversions': 0.0, 'clicks': 0},
                'error': f'Erro ao buscar planilha: {e}'}

    campaigns = {}
    headers_map = {}
    try:
        reader = csv.DictReader(io.StringIO(resp.text))
        for row in reader:
            if not headers_map:
                for k in row.keys():
                    kl = k.lower()
                    if 'date' in kl or 'data' in kl:
                        headers_map['date'] = k
                    elif 'campaign' in kl or 'campanha' in kl:
                        headers_map['campaign'] = k
                    elif any(x in kl for x in ('cost', 'custo', 'spend', 'gasto')):
                        headers_map['cost'] = k
                    elif any(x in kl for x in ('conversion', 'conversao', 'conversão')):
                        headers_map['conversions'] = k
                    elif 'click' in kl or 'clique' in kl:
                        headers_map['clicks'] = k
                    elif 'ctr' in kl:
                        headers_map['ctr'] = k

            date_col     = headers_map.get('date', 'Date (Segment)')
            campaign_col = headers_map.get('campaign', 'Campaign Name')
            cost_col     = headers_map.get('cost', 'Cost')
            conv_col     = headers_map.get('conversions', 'Conversions')
            clicks_col   = headers_map.get('clicks', 'Clicks')
            ctr_col      = headers_map.get('ctr', 'CTR')

            name = row.get(campaign_col, '').strip()
            if filter_keyword and filter_keyword.upper() not in name.upper():
                continue
            if not name:
                continue

            date_str = row.get(date_col, '').strip()
            if (since_dt or until_dt) and date_str:
                try:
                    row_dt = datetime.strptime(date_str, '%d/%m/%Y').date()
                    if since_dt and row_dt < since_dt:
                        continue
                    if until_dt and row_dt > until_dt:
                        continue
                except ValueError:
                    pass

            spend       = _parse_num(row.get(cost_col, '0'))
            conversions = _parse_num(row.get(conv_col, '0'))
            clicks      = int(_parse_num(row.get(clicks_col, '0')))
            ctr_raw     = _parse_num(row.get(ctr_col, '0'))

            if name not in campaigns:
                campaigns[name] = {'campaign_name': name, 'spend': 0.0, 'conversions': 0.0,
                                   'clicks': 0, 'ctr_sum': 0.0, 'days': 0}
            campaigns[name]['spend']       += spend
            campaigns[name]['conversions'] += conversions
            campaigns[name]['clicks']      += clicks
            campaigns[name]['ctr_sum']     += ctr_raw
            campaigns[name]['days']        += 1
    except Exception as e:
        return {'campaigns': [], 'totals': {'spend': 0.0, 'conversions': 0.0, 'clicks': 0},
                'error': f'Erro ao parsear planilha: {e}'}

    result = []
    total_spend = total_conv = 0.0
    total_clicks = 0
    for c in campaigns.values():
        days = c['days'] or 1
        cpc  = c['spend'] / c['clicks']      if c['clicks'] > 0      else 0
        cpa  = c['spend'] / c['conversions'] if c['conversions'] > 0 else 0
        ctr  = c['ctr_sum'] / days
        result.append({
            'campaign_name': c['campaign_name'],
            'spend':         round(c['spend'], 2),
            'conversions':   round(c['conversions'], 2),
            'clicks':        c['clicks'],
            'ctr':           round(ctr, 2),
            'cpc':           round(cpc, 2),
            'cpa':           round(cpa, 2),
        })
        total_spend  += c['spend']
        total_conv   += c['conversions']
        total_clicks += c['clicks']

    result.sort(key=lambda x: x['spend'], reverse=True)
    return {
        'campaigns': result,
        'totals': {
            'spend':       round(total_spend, 2),
            'conversions': round(total_conv, 2),
            'clicks':      total_clicks,
        },
        'error': None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Meta Ads only — fetch diário (sem dependência de planilha)
# ─────────────────────────────────────────────────────────────────────────────

def _meta_api_get(url, params, *, timeout=30):
    """Wrapper para GET na Meta API — delega ao client central com throttle
    global, monitor de usage headers e backoff (conformidade 7.e.i.2)."""
    from modules.meta_client import meta_get
    return meta_get(url, params, timeout=timeout)


def _sum_action_value(actions, action_type):
    """Soma `value` de actions com action_type específico."""
    if not action_type:
        return 0
    return sum(
        int(float(a.get('value', 0) or 0))
        for a in (actions or []) if a.get('action_type') == action_type
    )


def _sum_action_money(action_values, action_type):
    """Soma `value` monetário de action_values com action_type específico."""
    if not action_type:
        return 0.0
    return sum(
        float(a.get('value', 0) or 0)
        for a in (action_values or []) if a.get('action_type') == action_type
    )


def _previous_period(since_str, until_str):
    """
    Dado um período (YYYY-MM-DD strings), retorna o período anterior equivalente
    de mesma duração imediatamente antes.

    Ex: ('2026-05-01', '2026-05-21') → ('2026-04-10', '2026-04-30')
    """
    if not since_str or not until_str:
        return None, None
    try:
        s = datetime.strptime(since_str, '%Y-%m-%d').date()
        u = datetime.strptime(until_str, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return None, None
    duration = (u - s).days
    new_until = s - timedelta(days=1)
    new_since = new_until - timedelta(days=duration)
    return str(new_since), str(new_until)


def fetch_meta_ads_daily(account_id, access_token, conversion_event,
                         date_preset='last_7_days', since=None, until=None):
    """
    Busca insights agregados por DATA para um account_id.
    Usa `level='ad'` (mesmo padrão de cruzamento.fetch_fb_insights — testado em prod)
    e agrega por date_start no Python.

    Retorna lista de dicts ordenada por data:
        { date, spend, impressions, clicks, lpv, conversions, revenue_real }
    Onde revenue_real é o valor reportado pelo Pixel via action_values[conversion_event].
    """
    from modules.cruzamento import preset_to_dates
    from modules.meta_cache import get_or_fetch, ttl_for_period

    # Resolve o período ANTES para servir de chave de cache
    if not (since and until):
        since_d, until_d = preset_to_dates(date_preset)
        if since_d and until_d:
            since, until = str(since_d), str(until_d)

    cache_key = ('dash_daily', account_id, conversion_event, since, until, date_preset)
    return get_or_fetch(cache_key, ttl_for_period(until),
                        lambda: _fetch_meta_ads_daily_live(
                            account_id, access_token, conversion_event,
                            date_preset, since, until))


def _fetch_meta_ads_daily_live(account_id, access_token, conversion_event,
                               date_preset, since, until):
    """Fetch real (sem cache) — chamado apenas em cache miss."""
    base_url = f'{GRAPH_BASE}/{account_id}/insights'
    params = {
        'access_token':   access_token,
        'level':          'ad',
        'fields':         'spend,impressions,clicks,inline_link_clicks,actions,action_values,date_start',
        'limit':          500,
        'time_increment': 1,
    }

    if since and until:
        params['time_range'] = json.dumps({'since': since, 'until': until}, separators=(',', ':'))
    else:
        params['date_preset'] = 'last_30d'

    # meta_get_insights_rows: paginação + fragmentação automática de períodos
    # longos (> 90 dias) em blocos — recomendação oficial da Meta
    from modules.meta_client import meta_get_insights_rows
    rows_raw = meta_get_insights_rows(base_url, params)

    by_day = {}
    for item in rows_raw:
        date = item.get('date_start', '')
        if not date:
            continue
        actions       = item.get('actions') or []
        action_values = item.get('action_values') or []

        entry = by_day.setdefault(date, {
            'date': date, 'spend': 0.0, 'impressions': 0, 'clicks': 0,
            'lpv': 0, 'conversions': 0, 'revenue_real': 0.0,
        })
        entry['spend']        += float(item.get('spend', 0) or 0)
        entry['impressions']  += int(item.get('impressions', 0) or 0)
        entry['clicks']       += int(item.get('inline_link_clicks', 0) or 0)
        entry['lpv']          += _sum_action_value(actions, 'landing_page_view')
        entry['conversions']  += _sum_action_value(actions, conversion_event)
        entry['revenue_real'] += _sum_action_money(action_values, conversion_event)

    rows = list(by_day.values())
    for r in rows:
        r['spend']        = round(r['spend'], 2)
        r['revenue_real'] = round(r['revenue_real'], 2)
    rows.sort(key=lambda r: r['date'])
    return rows


def fetch_meta_ads_top(account_id, access_token, conversion_event,
                       since=None, until=None, date_preset='last_30d', limit=5):
    """
    Busca insights agregados por AD para retornar o top N ads por número de
    conversões. Usado pela seção 'Top criativos' do dashboard.

    Retorna lista ordenada (desc por conversions) com:
        { ad_id, ad_name, campaign_name, spend, impressions, clicks,
          conversions, revenue_real, roas, cpa }
    """
    from modules.cruzamento import preset_to_dates
    from modules.meta_cache import get_or_fetch, ttl_for_period

    # Resolve o período ANTES para servir de chave de cache
    if not (since and until):
        since_d, until_d = preset_to_dates(date_preset)
        if since_d and until_d:
            since, until = str(since_d), str(until_d)

    cache_key = ('dash_top', account_id, conversion_event, since, until, date_preset, limit)
    return get_or_fetch(cache_key, ttl_for_period(until),
                        lambda: _fetch_meta_ads_top_live(
                            account_id, access_token, conversion_event,
                            since, until, limit))


def _fetch_meta_ads_top_live(account_id, access_token, conversion_event,
                             since, until, limit):
    """Fetch real (sem cache) — chamado apenas em cache miss."""
    base_url = f'{GRAPH_BASE}/{account_id}/insights'
    params = {
        'access_token': access_token,
        'level':        'ad',
        'fields':       ('ad_id,ad_name,campaign_id,campaign_name,'
                         'spend,impressions,clicks,inline_link_clicks,'
                         'actions,action_values'),
        'limit':        500,
        # SEM time_increment — agrega o período todo por ad
    }
    if since and until:
        params['time_range'] = json.dumps({'since': since, 'until': until}, separators=(',', ':'))
    else:
        params['date_preset'] = 'last_30d'

    # Paginação + fragmentação de períodos longos (a agregação por ad_id abaixo
    # soma corretamente linhas vindas de blocos temporais diferentes)
    from modules.meta_client import meta_get_insights_rows
    by_ad = {}
    for item in meta_get_insights_rows(base_url, params):
        ad_id = item.get('ad_id') or ''
        if not ad_id:
            continue
        actions       = item.get('actions') or []
        action_values = item.get('action_values') or []

        entry = by_ad.setdefault(ad_id, {
            'ad_id': ad_id,
            'ad_name':       item.get('ad_name', ''),
            'campaign_name': item.get('campaign_name', ''),
            'spend': 0.0, 'impressions': 0, 'clicks': 0,
            'conversions': 0, 'revenue_real': 0.0,
        })
        entry['spend']        += float(item.get('spend', 0) or 0)
        entry['impressions']  += int(item.get('impressions', 0) or 0)
        entry['clicks']       += int(item.get('inline_link_clicks', 0) or 0)
        entry['conversions']  += _sum_action_value(actions, conversion_event)
        entry['revenue_real'] += _sum_action_money(action_values, conversion_event)

    rows = list(by_ad.values())
    # Filtra ads sem nenhuma venda (não interessam para "top criativos")
    rows = [r for r in rows if r['conversions'] > 0]
    for r in rows:
        r['spend']        = round(r['spend'], 2)
        r['revenue_real'] = round(r['revenue_real'], 2)
        r['roas']         = round(r['revenue_real'] / r['spend'], 2) if r['spend'] > 0 else 0.0
        r['cpa']          = round(r['spend'] / r['conversions'], 2) if r['conversions'] > 0 else 0.0
    rows.sort(key=lambda r: r['conversions'], reverse=True)
    return rows[:limit]


# ─────────────────────────────────────────────────────────────────────────────
# Rotas
# ─────────────────────────────────────────────────────────────────────────────

@dash_bp.route('/dash')
def dash_list():
    """Admin: lista todos os clientes configurados. Requer login do sistema."""
    from app import obter_token
    token = obter_token()
    if not token:
        return redirect(url_for('pagina_login'))
    clients = list_clients()
    return render_template('dash_list.html', clients=clients)


@dash_bp.route('/dash/<slug>')
def dash_view(slug):
    """
    View pública do dashboard de um cliente.
    Requer ?t=<public_link_token>.
    """
    from modules.rate_limiter import check_rate_limit
    check_rate_limit(f'dash-view:{slug}')

    token = request.args.get('t', '').strip()
    if not token:
        return render_template('dash_error.html',
                               message='Link inválido: parâmetro de acesso ausente.'), 403

    client = _resolve_client(slug, token)
    if not client:
        return render_template('dash_error.html',
                               message='Link inválido ou expirado. Solicite um novo link ao seu gestor.'), 403

    display_name  = client.get('display_name') or client.get('name') or slug.title()
    locked_period = client.get('locked_period') or ''

    # Clientes sem planilha de MQLs → dashboard Meta-only simplificado
    if not client.get('mqls_spreadsheet_id'):
        ticket = float(client.get('ticket_value') or 0)
        return render_template(
            'dash_meta.html',
            client_name=display_name,
            locked_period=locked_period,
            api_base=f'/api/dash/{slug}',
            dash_token=token,
            slug=slug,
            ticket_value=ticket,
        )

    # Clientes com planilha → dashboard completo (cruzamento)
    return render_template(
        'cruzamento.html',
        client_mode=True,
        client_name=display_name,
        locked_period=locked_period,
        api_base=f'/api/dash/{slug}',
        dash_token=token,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SSE — dados do dashboard
# ─────────────────────────────────────────────────────────────────────────────

def _resilient_sse_response(slug, endpoint, pkey, generate_fn=None):
    """Envolve um generator SSE com snapshot 'última leitura boa'.

    - Carga bem-sucedida (termina em stage 'done') → eventos salvos no banco
    - Falha ao vivo (stage 'error') → substitui pelo replay do último snapshot
    - generate_fn=None (ex: sem token Meta) → só replay; se não houver
      snapshot, emite o erro padrão

    O dashboard é um link público de cliente: NUNCA deve mostrar tela de erro
    se um dado antigo puder ser exibido com aviso de defasagem.
    """
    from modules.dash_snapshot import save_snapshot, load_snapshot, replay

    def _error_chunk(msg):
        return f"data: {json.dumps({'stage': 'error', 'message': msg})}\n\n"

    def wrapped():
        if generate_fn is None:
            events, at = load_snapshot(slug, endpoint, pkey)
            if events:
                yield from replay(events, at)
            else:
                yield _error_chunk('Sistema não autenticado. Contate o administrador.')
            return

        collected = []
        for chunk in generate_fn():
            is_data = isinstance(chunk, str) and chunk.startswith('data:')
            if is_data and '"stage": "error"' in chunk:
                # Falha ao vivo → tenta servir o último snapshot bom
                events, at = load_snapshot(slug, endpoint, pkey)
                if events:
                    logger.warning(f'[dash:{slug}] fonte falhou — servindo snapshot de {at}')
                    yield from replay(events, at)
                else:
                    yield chunk
                return
            if is_data:
                collected.append(chunk)
            yield chunk

        # Stream completou sem erro → persiste se terminou em 'done'
        if collected and '"stage": "done"' in collected[-1]:
            save_snapshot(slug, endpoint, pkey, collected)

    return Response(
        stream_with_context(wrapped()),
        mimetype='text/event-stream',
        headers={'X-Accel-Buffering': 'no', 'Cache-Control': 'no-cache'},
    )


@dash_bp.route('/api/dash/<slug>/data')
def api_dash_data(slug):
    """
    Endpoint SSE tenant-aware.
    Mesmo protocolo de /api/cruzamento/data mas config vem do banco (dashboard_clients).
    """
    client = _require_client(slug)

    # ── Config do cliente ────────────────────────────────────────────────────
    meta_account_id      = client['meta_ad_account_id']
    spreadsheet_id       = client.get('mqls_spreadsheet_id') or ''
    typeform_action_type = client.get('typeform_action_type') or 'offsite_conversion.fb_pixel_custom'
    google_sheet_id      = client.get('google_ads_sheet_id') or ''
    google_sheet_gid     = client.get('google_ads_sheet_gid') or ''
    google_filter_kw     = client.get('google_ads_filter_keyword') or ''
    google_customer_id   = client.get('google_ads_customer_id') or ''
    google_user_id       = client.get('google_ads_user_id')

    # Token Meta: sessão → token.json → banco (fallback durável pós-deploy)
    from app import obter_token
    meta_token = obter_token()

    date_preset = request.args.get('date_preset', 'last_7_days')
    since       = request.args.get('since')
    until       = request.args.get('until')

    from modules.dash_snapshot import period_key
    pkey = period_key(date_preset, since, until)

    if not meta_token:
        # Sem token: serve o último snapshot bom (com aviso) em vez de 503
        return _resilient_sse_response(slug, 'data', pkey, None)

    # Importa helpers do cruzamento (reutiliza lógica de processamento)
    from modules.cruzamento import (
        fetch_fb_insights, fetch_sheets_data, fetch_ads_status,
        fetch_campaigns_status, fetch_adsets_status, processar_cruzamento,
        preset_to_dates, filter_rows_by_date, _norm,
    )

    def _sse(stage, payload):
        return f"data: {json.dumps({'stage': stage, **payload})}\n\n"

    def generate():
        try:
            t0 = time.time()
            display = client.get('display_name') or slug

            yield _sse('status', {'message': f'Buscando Meta Ads para {display}...'})

            # ── Resolve Google Ads: OAuth ou planilha ────────────────────────
            google_ads_enabled = False
            google_ads_oauth   = False
            google_ads_sheets  = False
            ga_token           = None
            ga_config          = None

            if google_customer_id:
                try:
                    from modules.google_ads import (
                        is_google_ads_configured, get_google_ads_config_from_db,
                        fetch_google_ads_insights, _get_valid_token, save_google_ads_config,
                    )
                    if is_google_ads_configured() and google_user_id:
                        ga_config = get_google_ads_config_from_db(str(google_user_id), '')
                        if ga_config:
                            ga_token, ga_config_updated = _get_valid_token(ga_config)
                            if ga_token:
                                google_ads_oauth   = True
                                google_ads_enabled = True
                                if (ga_config_updated and
                                        ga_config_updated.get('access_token') != ga_config.get('access_token')):
                                    save_google_ads_config(str(google_user_id), '', ga_config_updated)
                except Exception as e:
                    logger.warning(f'[dash:{slug}] Google Ads OAuth init: {e}')

            # Planilha como fallback
            if not google_ads_oauth and google_sheet_id and google_sheet_gid:
                google_ads_sheets  = True
                google_ads_enabled = True

            since_d, until_d = preset_to_dates(date_preset, since, until)

            # ── Fetch paralelo ───────────────────────────────────────────────
            with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
                fb_future     = executor.submit(
                    fetch_fb_insights, meta_account_id, meta_token, date_preset, since, until)
                sheets_future = (executor.submit(fetch_sheets_data, spreadsheet_id)
                                 if spreadsheet_id else None)
                status_future       = executor.submit(fetch_ads_status,      meta_account_id, meta_token)
                camp_status_future  = executor.submit(fetch_campaigns_status, meta_account_id, meta_token)
                adset_status_future = executor.submit(fetch_adsets_status,    meta_account_id, meta_token)

                google_ads_future  = None
                gads_daily_future  = None

                if google_ads_oauth:
                    ga_since = str(since_d) if since_d else str(_date.today() - timedelta(days=29))
                    ga_until = str(until_d) if until_d else str(_date.today())
                    google_ads_future = executor.submit(
                        fetch_google_ads_insights, ga_token, ga_config['customer_id'],
                        ga_since, ga_until,
                    )
                    yield _sse('status', {'message': f'Buscando Meta + Google Ads + Sheets para {display}...'})
                elif google_ads_sheets:
                    gads_daily_future = executor.submit(
                        fetch_client_google_ads_daily,
                        google_sheet_id, google_sheet_gid, google_filter_kw, since_d, until_d,
                    )

                futures = [fb_future, status_future, camp_status_future, adset_status_future]
                if sheets_future:
                    futures.append(sheets_future)
                if google_ads_future:
                    futures.append(google_ads_future)
                if gads_daily_future:
                    futures.append(gads_daily_future)

                while not all(f.done() for f in futures):
                    yield ': keepalive\n\n'
                    time.sleep(3)

                fb_ads           = fb_future.result()
                status_map       = status_future.result()
                camp_status_map  = camp_status_future.result()
                adset_status_map = adset_status_future.result()

                mqls_rows_all, wons_rows_all = [], []
                if sheets_future:
                    try:
                        mqls_rows_all, wons_rows_all = sheets_future.result()
                    except Exception as e:
                        logger.warning(f'[dash:{slug}] Sheets fetch falhou: {e}')

                google_ads_count = 0
                if google_ads_future:
                    try:
                        ga_data = google_ads_future.result()
                        if ga_data:
                            google_ads_count = len(ga_data)
                            fb_ads.extend(ga_data)
                    except Exception as e:
                        logger.warning(f'[dash:{slug}] Google Ads OAuth fetch falhou: {e}')

                gads_daily = {}
                if gads_daily_future:
                    try:
                        gads_daily = gads_daily_future.result() or {}
                    except Exception as e:
                        logger.warning(f'[dash:{slug}] Google Ads planilha fetch falhou: {e}')

            yield _sse('status', {'message': f'Processando {len(fb_ads)} registros...'})
            yield ': keepalive\n\n'

            # Enriquece status
            for ad in fb_ads:
                ad['ad_status']       = status_map.get(ad.get('ad_id'), 'UNKNOWN')
                ad['campaign_status'] = camp_status_map.get(ad.get('campaign_id'), 'UNKNOWN')
                ad['adset_status']    = adset_status_map.get(ad.get('adset_id'), 'UNKNOWN')

            mqls_rows = filter_rows_by_date(mqls_rows_all, 'Data do preenchimento', since_d, until_d)
            wons_rows = filter_rows_by_date(wons_rows_all, 'Data de fechamento', since_d, until_d)

            # Exclui leads Google dos totais Meta
            _GOOGLE_SRC = {'adwords', 'google'}
            _google_deal_ids = {
                _norm(r.get('Deal ID', ''))
                for r in mqls_rows_all
                if _norm(r.get('utm_source', '')) in _GOOGLE_SRC
            }
            mqls_rows = [r for r in mqls_rows if _norm(r.get('utm_source', '')) not in _GOOGLE_SRC]
            wons_rows = [r for r in wons_rows if _norm(r.get('Deal ID', '')) not in _google_deal_ids]

            # Padrões excluídos do banco (por cliente)
            excluded_patterns = get_excluded_patterns(slug)

            # Processa (typeform_action_type via override thread-local seguro aqui pois
            # fetch_fb_insights já terminou — o override afeta apenas processar_cruzamento
            # que não usa TYPEFORM_ACTION_TYPE; a resolução de typeform_submits já foi feita
            # durante o fetch. TODO: passar typeform_action_type como parâmetro em futura refatoração.)
            _resultado_box = [None, None]
            def _process():
                try:
                    _resultado_box[0] = processar_cruzamento(
                        fb_ads, mqls_rows, wons_rows,
                        mqls_all=mqls_rows_all,
                        excluded_patterns=excluded_patterns,
                    )
                except Exception as e:
                    _resultado_box[1] = e

            proc_thread = threading.Thread(target=_process, daemon=True)
            proc_thread.start()
            while proc_thread.is_alive():
                yield ': keepalive\n\n'
                proc_thread.join(timeout=3)

            if _resultado_box[1]:
                raise _resultado_box[1]

            resultado = _resultado_box[0]
            elapsed   = round(time.time() - t0, 2)

            # Emite eventos SSE (mesmo protocolo do cruzamento)
            yield _sse('kpis', {
                'ads_consolidated': resultado['ads_consolidated'],
                'total_mqls':       resultado['total_mqls'],
                'fat_total_sheets': resultado['fat_total_sheets'],
            })
            yield ': keepalive\n\n'

            yield _sse('funnel', {'funnel': resultado.get('funnel')})

            # Merge Google Ads planilha no daily funnel
            daily_funnel = resultado.get('daily_funnel', [])
            if gads_daily:
                existing_dates = {e['date'] for e in daily_funnel}
                for entry in daily_funnel:
                    gd = gads_daily.get(entry['date'])
                    if gd:
                        entry['spend']       = round(entry.get('spend', 0.0) + gd.get('spend', 0.0), 2)
                        entry['link_clicks'] = entry.get('link_clicks', 0) + gd.get('clicks', 0)
                        entry['has_google']  = True
                for dk, gd in gads_daily.items():
                    if dk not in existing_dates:
                        daily_funnel.append({
                            'date': dk, 'spend': round(gd.get('spend', 0.0), 2),
                            'impressions': 0, 'link_clicks': gd.get('clicks', 0),
                            'lpv': 0, 'typeform': 0, 'mqls': 0,
                            'ctr': None, 'connect_rate': None,
                            'taxa_lead': None, 'taxa_mql': None, 'has_google': True,
                        })
                daily_funnel.sort(key=lambda e: e['date'])

            yield _sse('panel', {
                'daily_funnel':  daily_funnel,
                'google_merged': bool(gads_daily or google_ads_oauth),
            })
            yield ': keepalive\n\n'

            yield _sse('charts', {
                'by_produto': resultado.get('by_produto', {}),
                'by_date':    resultado.get('by_date', []),
            })
            yield ': keepalive\n\n'

            yield _sse('campaigns', {'campaigns_consolidated': resultado['campaigns_consolidated']})
            yield _sse('adsets',    {'adsets_consolidated':    resultado['adsets_consolidated']})
            yield _sse('ads',       {'ads_consolidated':       resultado['ads_consolidated']})
            yield ': keepalive\n\n'

            yield _sse('instagram_posts', {
                'instagram_posts_consolidated': resultado.get('instagram_posts_consolidated', []),
            })
            yield ': keepalive\n\n'

            yield _sse('timeline', {
                'by_date':              resultado.get('by_date', []),
                'by_date_per_campaign': resultado.get('by_date_per_campaign', {}),
                'by_date_per_adset':    resultado.get('by_date_per_adset', {}),
                'by_date_per_ad':       resultado.get('by_date_per_ad', {}),
            })

            yield _sse('done', {
                'meta': {
                    'fb_ads_count':       len(fb_ads),
                    'google_ads_count':   google_ads_count,
                    'google_ads_enabled': google_ads_enabled,
                    'mqls_count':         resultado['total_mqls'],
                    'wons_count':         resultado['total_wons'],
                    'elapsed_sec':        elapsed,
                    'date_preset':        date_preset,
                    'timestamp':          datetime.now(_BR_TZ).isoformat(),
                }
            })

        except Exception as e:
            import traceback
            traceback.print_exc()
            yield _sse('error', {'message': str(e)})

    return _resilient_sse_response(slug, 'data', pkey, generate)


# ─────────────────────────────────────────────────────────────────────────────
# SSE Meta-only (clientes sem planilha de MQLs)
# ─────────────────────────────────────────────────────────────────────────────

@dash_bp.route('/api/dash/<slug>/meta-only')
def api_dash_meta_only(slug):
    """
    Endpoint SSE para dashboards Meta-only (sem planilha de MQLs/Wons).
    Retorna dados diários + totais do período.
    """
    client = _require_client(slug)

    meta_account_id  = client['meta_ad_account_id']
    conversion_event = client.get('typeform_action_type') or 'offsite_conversion.fb_pixel_custom'
    ticket_value     = float(client.get('ticket_value') or 0)

    from app import obter_token
    meta_token = obter_token()

    date_preset = request.args.get('date_preset', 'last_7_days')
    since       = request.args.get('since')
    until       = request.args.get('until')

    from modules.dash_snapshot import period_key
    pkey = period_key(date_preset, since, until)

    if not meta_token:
        # Sem token: serve o último snapshot bom (com aviso) em vez de 503
        return _resilient_sse_response(slug, 'meta-only', pkey, None)

    def _sse(stage, payload):
        return f"data: {json.dumps({'stage': stage, **payload})}\n\n"

    def _aggregate_totals(rows):
        """Recebe a lista diária e devolve dict de totais + métricas derivadas."""
        t_spend   = sum(r['spend']        for r in rows)
        t_imp     = sum(r['impressions']  for r in rows)
        t_clicks  = sum(r['clicks']       for r in rows)
        t_lpv     = sum(r['lpv']          for r in rows)
        t_conv    = sum(r['conversions']  for r in rows)
        t_rev_re  = sum(r['revenue_real'] for r in rows)
        t_rev_est = t_conv * ticket_value
        # Faturamento "efetivo": prioriza Pixel, cai pro estimado se vier 0
        t_rev_eff = t_rev_re if t_rev_re > 0 else t_rev_est

        return {
            'spend':         round(t_spend, 2),
            'impressions':   t_imp,
            'clicks':        t_clicks,
            'lpv':           t_lpv,
            'conversions':   t_conv,
            'revenue_real':  round(t_rev_re, 2),
            'revenue_est':   round(t_rev_est, 2),
            'revenue':       round(t_rev_eff, 2),   # campo principal
            'ticket':        ticket_value,
            'aov':           round(t_rev_re / t_conv, 2) if (t_rev_re > 0 and t_conv > 0) else 0.0,
            'roas':          round(t_rev_eff / t_spend, 2) if t_spend > 0 else 0.0,
            'profit':        round(t_rev_eff - t_spend, 2),
            'cac':           round(t_spend / t_conv, 2) if t_conv > 0 else 0.0,
            'cpm':           round(t_spend / t_imp * 1000, 2) if t_imp > 0 else 0.0,
            'cpc_link':      round(t_spend / t_clicks, 2) if t_clicks > 0 else 0.0,
            'ctr':           round(t_clicks / t_imp * 100, 2) if t_imp > 0 else 0.0,
            'connect_rate':  round(t_lpv / t_clicks * 100, 2) if t_clicks > 0 else 0.0,
        }

    def _deltas(curr, prev):
        """Calcula delta percentual para cada KPI comparado ao período anterior."""
        out = {}
        # Para CAC/CPM/CPC e profit a interpretação de delta é específica:
        # - 'inverse' significa que valor menor é melhor (CAC, CPM, CPC)
        # - 'neutral' usa valor absoluto sem polaridade definida
        for key in ('spend', 'impressions', 'clicks', 'lpv', 'conversions',
                    'revenue', 'revenue_real', 'roas', 'aov', 'profit',
                    'cac', 'cpm', 'cpc_link', 'ctr', 'connect_rate'):
            c, p = curr.get(key, 0) or 0, prev.get(key, 0) or 0
            if p == 0:
                out[key] = None  # sem base de comparação
            else:
                out[key] = round((c - p) / p * 100, 1)
        return out

    def generate():
        try:
            t0      = time.time()
            display = client.get('display_name') or slug

            yield _sse('status', {'message': f'Buscando Meta Ads para {display}...'})

            # Período atual
            rows = fetch_meta_ads_daily(
                meta_account_id, meta_token, conversion_event,
                date_preset, since, until,
            )

            # Resolve período anterior — só calculável quando temos since/until concretos
            from modules.cruzamento import preset_to_dates as _p2d
            curr_since, curr_until = since, until
            if not (curr_since and curr_until):
                _sd, _ud = _p2d(date_preset)
                if _sd and _ud:
                    curr_since, curr_until = str(_sd), str(_ud)
            prev_since, prev_until = _previous_period(curr_since, curr_until)

            yield _sse('status', {'message': f'Processando {len(rows)} dias + período anterior + top criativos...'})

            # Fetch paralelo: período anterior + top ads do período atual
            prev_rows = []
            top_ads   = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
                fut_prev = (ex.submit(
                    fetch_meta_ads_daily, meta_account_id, meta_token, conversion_event,
                    date_preset, prev_since, prev_until,
                ) if prev_since and prev_until else None)
                fut_top  = ex.submit(
                    fetch_meta_ads_top, meta_account_id, meta_token, conversion_event,
                    curr_since, curr_until, date_preset, 5,
                )
                if fut_prev:
                    try:    prev_rows = fut_prev.result()
                    except Exception as e:
                        logger.warning(f'[dash:{slug}] período anterior falhou: {e}')
                try:    top_ads = fut_top.result()
                except Exception as e:
                    logger.warning(f'[dash:{slug}] top ads falhou: {e}')

            totals      = _aggregate_totals(rows)
            prev_totals = _aggregate_totals(prev_rows)
            deltas      = _deltas(totals, prev_totals)

            # Métricas derivadas por dia (mantém compat com tabela atual)
            daily = []
            for r in rows:
                sp, cl, im, lp, cv = (r['spend'], r['clicks'], r['impressions'],
                                      r['lpv'], r['conversions'])
                rev = r['revenue_real'] if r['revenue_real'] > 0 else round(cv * ticket_value, 2)
                daily.append({
                    'date':         r['date'],
                    'spend':        sp,
                    'impressions':  im,
                    'clicks':       cl,
                    'lpv':          lp,
                    'conversions':  cv,
                    'ctr':          round(cl / im * 100, 2) if im > 0 else 0.0,
                    'connect_rate': round(lp / cl * 100, 2) if cl > 0 else 0.0,
                    'revenue':      rev,
                    'roas':         round(rev / sp, 2) if sp > 0 else 0.0,
                })

            elapsed = round(time.time() - t0, 2)

            yield _sse('kpis', {
                'totals':      totals,
                'prev_totals': prev_totals,
                'deltas':      deltas,
                'period':      {'since': curr_since, 'until': curr_until},
                'prev_period': {'since': prev_since, 'until': prev_until},
            })

            yield _sse('daily',   {'rows': daily})
            yield _sse('top_ads', {'rows': top_ads, 'ticket': ticket_value})

            yield _sse('done', {
                'meta': {
                    'days':        len(rows),
                    'elapsed_sec': elapsed,
                    'date_preset': date_preset,
                    'timestamp':   datetime.now(_BR_TZ).isoformat(),
                }
            })

        except Exception as e:
            import traceback
            traceback.print_exc()
            yield _sse('error', {'message': str(e)})

    return _resilient_sse_response(slug, 'meta-only', pkey, generate)


# ─────────────────────────────────────────────────────────────────────────────
# Debug: lista todos os action_types disponíveis no período (descoberta de
# `conversion_event` correto pra cada cliente). Retorna HTML com tabela.
# ─────────────────────────────────────────────────────────────────────────────

@dash_bp.route('/api/dash/<slug>/debug-actions')
def api_dash_debug_actions(slug):
    """
    Lista TODOS os action_types retornados pela Meta para a conta do cliente
    no período, agregados (total_value, total_count, dias_com_evento).
    Destaca tipos relevantes (purchase/lead/conversion/typeform) para facilitar
    identificação do `conversion_event` correto.

    Uso: /api/dash/<slug>/debug-actions?t=<token>&date_preset=this_month
    """
    client = _require_client(slug)
    meta_account_id = client['meta_ad_account_id']
    current_event   = client.get('typeform_action_type') or '—'

    from app import obter_token
    meta_token = obter_token()
    if not meta_token:
        return 'Sistema não autenticado.', 503

    from modules.cruzamento import preset_to_dates
    date_preset = request.args.get('date_preset', 'last_30d')
    since       = request.args.get('since')
    until       = request.args.get('until')

    base_url = f'{GRAPH_BASE}/{meta_account_id}/insights'
    params = {
        'access_token':   meta_token,
        'level':          'ad',
        'fields':         'actions,date_start',
        'limit':          500,
        'time_increment': 1,
    }
    if since and until:
        params['time_range'] = json.dumps({'since': since, 'until': until}, separators=(',', ':'))
    else:
        since_d, until_d = preset_to_dates(date_preset)
        if since_d and until_d:
            params['time_range'] = json.dumps({'since': str(since_d), 'until': str(until_d)}, separators=(',', ':'))

    # Agrega action_types
    by_type = {}  # action_type -> {'value': float, 'days': set}
    url = base_url
    try:
        while url:
            body = _meta_api_get(url, params if url == base_url else None)
            for item in body.get('data', []):
                date = item.get('date_start', '')
                for a in (item.get('actions') or []):
                    at = a.get('action_type', '')
                    if not at:
                        continue
                    entry = by_type.setdefault(at, {'value': 0.0, 'days': set()})
                    entry['value'] += float(a.get('value', 0) or 0)
                    if date:
                        entry['days'].add(date)

            url = body.get('paging', {}).get('next')
            params = None
    except Exception as e:
        return f'<pre style="color:#f87171;padding:24px;font-family:monospace">Erro: {e}</pre>', 500

    # Sort: relevantes primeiro (purchase/lead/conversion/typeform), depois por valor
    KEYWORDS_RELEVANT = ('purchase', 'lead', 'conversion', 'typeform', 'submit', 'complete_registration', 'subscribe')
    def _score(at):
        s = 0
        atl = at.lower()
        for kw in KEYWORDS_RELEVANT:
            if kw in atl:
                s += 100
        return s
    rows = sorted(
        [{'action_type': k, **v, 'days': len(v['days'])} for k, v in by_type.items()],
        key=lambda r: (_score(r['action_type']), r['value']),
        reverse=True,
    )

    # Renderiza HTML
    rows_html = ''
    if rows:
        for r in rows:
            highlight = any(kw in r['action_type'].lower() for kw in KEYWORDS_RELEVANT)
            row_style = 'background:#1e293b' if highlight else ''
            is_current = r['action_type'] == current_event
            current_badge = '<span style="background:#22c55e;color:#000;padding:2px 8px;border-radius:4px;font-size:.7rem;font-weight:700;margin-left:8px">EM USO</span>' if is_current else ''
            rows_html += (
                f'<tr style="{row_style}">'
                f'<td style="padding:8px 14px;font-family:monospace;font-size:.85rem;color:#e2e8f0">{r["action_type"]}{current_badge}</td>'
                f'<td style="padding:8px 14px;text-align:right;font-weight:600;color:{"#22c55e" if highlight else "#94a3b8"}">{int(r["value"])}</td>'
                f'<td style="padding:8px 14px;text-align:right;color:#94a3b8">{r["days"]}</td>'
                f'</tr>'
            )
    else:
        rows_html = '<tr><td colspan="3" style="padding:24px;text-align:center;color:#64748b">Nenhuma ação retornada pela Meta no período.</td></tr>'

    period_label = (f'{since} → {until}' if since and until else date_preset)
    html = f'''<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8"><title>Debug Actions — {slug}</title>
<style>
  body {{ background:#0f1117; color:#e2e8f0; font-family:-apple-system, sans-serif; padding:24px; margin:0; }}
  h1 {{ font-size:1.4rem; margin-bottom:8px; color:#f1f5f9; }}
  .sub {{ color:#94a3b8; font-size:.9rem; margin-bottom:20px; }}
  .info {{ background:#1a1d27; border:1px solid #252836; border-radius:8px;
          padding:12px 16px; margin-bottom:16px; font-size:.85rem; color:#cbd5e1; }}
  .info code {{ background:#0f1117; padding:2px 6px; border-radius:3px; color:#a5b4fc; font-family:monospace; }}
  table {{ width:100%; border-collapse:collapse; background:#1a1d27; border:1px solid #252836; border-radius:8px; overflow:hidden; }}
  th {{ background:#131621; padding:10px 14px; text-align:left;
        font-size:.75rem; text-transform:uppercase; color:#64748b; letter-spacing:.05em; }}
  th:nth-child(2), th:nth-child(3) {{ text-align:right; }}
  td {{ border-bottom:1px solid #252836; }}
  tr:last-child td {{ border-bottom:none; }}
  .legend {{ margin-top:14px; color:#64748b; font-size:.8rem; }}
  .filters {{ display:flex; gap:8px; margin-bottom:16px; flex-wrap:wrap; }}
  .filters a {{ background:#1a1d27; border:1px solid #252836; padding:5px 12px;
                border-radius:6px; color:#cbd5e1; text-decoration:none; font-size:.82rem; }}
  .filters a:hover {{ border-color:#6366f1; color:#6366f1; }}
</style>
</head><body>
<h1>🔍 Debug — Action Types disponíveis</h1>
<div class="sub">Cliente: <b>{slug}</b> | Período: <b>{period_label}</b></div>

<div class="info">
  <b>conversion_event atual:</b> <code>{current_event}</code><br>
  Linhas destacadas (em verde) contêm palavras-chave de conversão. Identifique qual <code>action_type</code> bate com o seu volume real de vendas e me passe o nome — vou atualizar o banco.
</div>

<div class="filters">
  <a href="?t={request.args.get('t','')}&date_preset=last_7d">7 dias</a>
  <a href="?t={request.args.get('t','')}&date_preset=last_30d">30 dias</a>
  <a href="?t={request.args.get('t','')}&date_preset=this_month">Este mês</a>
  <a href="?t={request.args.get('t','')}&date_preset=last_month">Mês passado</a>
  <a href="?t={request.args.get('t','')}&date_preset=maximum">Tempo todo</a>
</div>

<table>
  <thead><tr><th>Action Type</th><th>Total (somado)</th><th>Dias c/ evento</th></tr></thead>
  <tbody>{rows_html}</tbody>
</table>

<div class="legend">Total = soma do campo <code>value</code> em todos os ads do período. Tipos relevantes (purchase/lead/conversion/typeform/submit/subscribe) aparecem no topo destacados.</div>
</body></html>'''
    return html


# ─────────────────────────────────────────────────────────────────────────────
# Filtros
# ─────────────────────────────────────────────────────────────────────────────

@dash_bp.route('/api/dash/<slug>/filtros', methods=['GET', 'POST'])
def api_dash_filtros(slug):
    """GET: lista padrões excluídos. POST: salva nova lista."""
    _require_client(slug)

    if request.method == 'GET':
        return jsonify({'success': True, 'patterns': get_excluded_patterns(slug)})

    body = request.get_json(silent=True) or {}
    patterns = body.get('patterns', [])
    if not isinstance(patterns, list):
        return jsonify({'success': False, 'error': 'patterns deve ser uma lista'}), 400

    ok, saved = db_save_excluded(slug, patterns)
    if not ok:
        return jsonify({'success': False, 'error': 'Falha ao salvar filtros'}), 500
    return jsonify({'success': True, 'patterns': saved})


# ─────────────────────────────────────────────────────────────────────────────
# Google Ads via planilha
# ─────────────────────────────────────────────────────────────────────────────

@dash_bp.route('/api/dash/<slug>/google-ads-sheets')
def api_dash_google_ads_sheets(slug):
    """
    Aba Google Ads: dados da planilha pública configurada para o cliente.
    Mesmo formato de resposta de /api/cruzamento/google-ads-sheets.
    """
    client = _require_client(slug)

    sheet_id  = client.get('google_ads_sheet_id') or ''
    sheet_gid = client.get('google_ads_sheet_gid') or ''
    filter_kw = client.get('google_ads_filter_keyword') or ''

    if not sheet_id or not sheet_gid:
        return jsonify({'campaigns': [], 'totals': {'spend': 0, 'conversions': 0, 'clicks': 0},
                        'count': 0, 'daily': [],
                        'error': 'Google Ads via planilha não configurado para este cliente'}), 200

    since_str = request.args.get('since', '')
    until_str = request.args.get('until', '')
    try:
        since_dt = datetime.strptime(since_str, '%Y-%m-%d').date() if since_str else None
        until_dt = datetime.strptime(until_str, '%Y-%m-%d').date() if until_str else None
    except ValueError:
        since_dt = until_dt = None

    # Campanhas agregadas
    sheet_data = fetch_client_sheet_campaigns(sheet_id, sheet_gid, filter_kw, since_dt, until_dt)

    # Enriquecer com MQLs/NC da planilha de MQLs (utm_source google/adwords)
    spreadsheet_id = client.get('mqls_spreadsheet_id') or ''
    mqls_by_campaign = {}
    mqls_by_day      = {}
    total_mqls = total_nc = total_track = 0

    if spreadsheet_id and not sheet_data.get('error'):
        try:
            from modules.cruzamento import (
                fetch_sheets_data, filter_rows_by_date,
                _norm, _is_produto_a, _parse_date_br,
            )
            mqls_all, _ = fetch_sheets_data(spreadsheet_id)
            mqls_filtered = filter_rows_by_date(mqls_all, 'Data do preenchimento', since_dt, until_dt)
            GOOGLE_SRC = {'adwords', 'google'}
            for r in mqls_filtered:
                if _norm(r.get('utm_source', '')) not in GOOGLE_SRC:
                    continue
                key = _norm(r.get('utm_campaign', ''))
                if key:
                    bucket = mqls_by_campaign.setdefault(key, {'mqls': 0, 'nc': 0})
                    bucket['mqls'] += 1
                d_obj = _parse_date_br(r.get('Data do preenchimento', ''))
                if d_obj:
                    day_key = d_obj.strftime('%Y-%m-%d')
                    dbucket = mqls_by_day.setdefault(day_key, {'mqls': 0, 'nc': 0, 'track': 0})
                    dbucket['mqls'] += 1
                is_nc = _is_produto_a(r.get('Produto indicado', ''))
                total_mqls += 1
                if is_nc:
                    total_nc += 1
                    if key:
                        mqls_by_campaign[key]['nc'] = mqls_by_campaign[key].get('nc', 0) + 1
                    if d_obj:
                        mqls_by_day[day_key]['nc'] += 1
                else:
                    total_track += 1
                    if d_obj:
                        mqls_by_day[day_key]['track'] += 1
        except Exception as e:
            logger.warning(f'[dash:{slug}] MQL enrichment for google-ads-sheets falhou: {e}')

    # Mescla MQLs nas campanhas
    for c in sheet_data.get('campaigns', []):
        key    = c.get('campaign_name', '').strip().lower()
        bucket = mqls_by_campaign.get(key, {'mqls': 0, 'nc': 0})
        mqls   = bucket['mqls']
        nc     = bucket['nc']
        spend  = c.get('spend', 0.0) or 0.0
        c['mqls']     = mqls
        c['nc']       = nc
        c['pct_nc']   = round(nc / mqls * 100, 1) if mqls > 0 else 0.0
        c['cpa']      = round(spend / mqls, 2)     if mqls > 0 else 0.0
        c['custo_nc'] = round(spend / nc, 2)        if nc > 0   else 0.0

    sheet_data['totals']['mqls']   = total_mqls
    sheet_data['totals']['nc']     = total_nc
    sheet_data['totals']['track']  = total_track
    sheet_data['totals']['pct_nc'] = round(total_nc / total_mqls * 100, 1) if total_mqls > 0 else 0.0

    # Série diária
    gads_daily = fetch_client_google_ads_daily(sheet_id, sheet_gid, filter_kw, since_dt, until_dt)
    all_dates  = set(mqls_by_day.keys()) | set(gads_daily.keys())
    daily_list = []
    for dk in sorted(all_dates):
        mq = mqls_by_day.get(dk, {'mqls': 0, 'nc': 0, 'track': 0})
        vd = gads_daily.get(dk, {'spend': 0.0, 'clicks': 0})
        sp = round(vd.get('spend', 0.0), 2)
        mq_n, nc_n, tr_n = mq['mqls'], mq['nc'], mq['track']
        daily_list.append({
            'date':   dk,  'mqls':  mq_n, 'nc':    nc_n,
            'track':  tr_n, 'spend': sp,   'clicks': vd.get('clicks', 0),
            'pct_nc': round(nc_n / mq_n * 100, 1) if mq_n > 0 else 0.0,
            'cpl':    round(sp / mq_n, 2)          if mq_n > 0 else 0.0,
        })

    return jsonify({
        'campaigns': sheet_data.get('campaigns', []),
        'totals':    sheet_data.get('totals', {}),
        'count':     len(sheet_data.get('campaigns', [])),
        'filter':    filter_kw or slug.upper(),
        'daily':     daily_list,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Consolidado multi-canal
# ─────────────────────────────────────────────────────────────────────────────

@dash_bp.route('/api/dash/<slug>/consolidado')
def api_dash_consolidado(slug):
    """
    Aba Consolidado: une gastos Meta Ads + Google Ads (planilha) + MQLs/Wons da planilha.
    Mesmo protocolo de /api/cruzamento/consolidado mas config vem do banco.
    """
    client = _require_client(slug)

    meta_account_id = client['meta_ad_account_id']
    spreadsheet_id  = client.get('mqls_spreadsheet_id') or ''
    sheet_id        = client.get('google_ads_sheet_id') or ''
    sheet_gid       = client.get('google_ads_sheet_gid') or ''
    filter_kw       = client.get('google_ads_filter_keyword') or ''

    from app import obter_token
    meta_token = obter_token()
    if not meta_token:
        return jsonify({'success': False, 'error': 'Sistema não autenticado'}), 503

    date_preset = request.args.get('date_preset', 'last_7_days')
    since       = request.args.get('since')
    until       = request.args.get('until')

    from modules.cruzamento import (
        fetch_fb_insights, fetch_sheets_data,
        filter_rows_by_date, preset_to_dates,
        _matches_excluded, _is_instagram_post, _norm, _parse_valor,
    )

    since_d, until_d = preset_to_dates(date_preset, since, until)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            fb_fut     = ex.submit(fetch_fb_insights, meta_account_id, meta_token, date_preset, since, until)
            sheets_fut = ex.submit(fetch_sheets_data, spreadsheet_id) if spreadsheet_id else None
            gads_fut   = ex.submit(
                fetch_client_sheet_campaigns, sheet_id, sheet_gid, filter_kw, since_d, until_d
            ) if (sheet_id and sheet_gid) else None

            fb_ads = fb_fut.result()
            mqls_all, wons_all = (sheets_fut.result() if sheets_fut else ([], []))
            gads   = (gads_fut.result() if gads_fut else {'totals': {'spend': 0.0, 'conversions': 0.0}, 'error': None})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

    # Facebook spend (respeita filtros do cliente)
    excluded = get_excluded_patterns(slug)
    fb_spend = 0.0
    for ad in fb_ads:
        cn = ad.get('campaign_name', '')
        if _matches_excluded(cn, excluded) or _is_instagram_post(cn):
            continue
        fb_spend += float(ad.get('spend', 0) or 0)

    google_spend = float((gads.get('totals') or {}).get('spend', 0.0))

    # MQLs/Wons do período
    mqls_rows = filter_rows_by_date(mqls_all, 'Data do preenchimento', since_d, until_d)
    wons_rows = filter_rows_by_date(wons_all, 'Data de fechamento',    since_d, until_d)

    wons_idx = {
        _norm(r.get('Deal ID', '')): {'valor': _parse_valor(r.get('Valor', 0))}
        for r in wons_rows if _norm(r.get('Deal ID', ''))
    }

    def _bucket(utm_src):
        s = (utm_src or '').strip().lower()
        if s == 'facebook':    return 'facebook'
        if s in ('adwords', 'google'): return 'google'
        return 'outros'

    def _empty(): return {'mqls': 0, 'wons': 0, 'receita': 0.0}
    buckets = {'facebook': _empty(), 'google': _empty(), 'outros': _empty()}

    for row in mqls_rows:
        buckets[_bucket(row.get('utm_source', ''))]['mqls'] += 1

    mqls_all_by_deal = {
        _norm(r.get('Deal ID', '')): r for r in mqls_all if _norm(r.get('Deal ID', ''))
    }
    for row in wons_rows:
        deal_id = _norm(row.get('Deal ID', ''))
        valor   = _parse_valor(row.get('Valor', 0))
        mql_row = mqls_all_by_deal.get(deal_id)
        b = _bucket(mql_row.get('utm_source', '') if mql_row else '')
        buckets[b]['wons']    += 1
        buckets[b]['receita'] += valor

    def _metrics(bucket, spend):
        m, w, r = bucket['mqls'], bucket['wons'], bucket['receita']
        return {
            'spend':     round(spend, 2),
            'mqls':      m, 'wons': w, 'receita': round(r, 2),
            'cpm':       round(spend / m, 2)   if m > 0     else 0.0,
            'cpa':       round(spend / w, 2)   if w > 0     else 0.0,
            'ticket':    round(r / w, 2)       if w > 0     else 0.0,
            'conv_rate': round(100 * w / m, 2) if m > 0     else 0.0,
            'roas':      round(r / spend, 2)   if spend > 0 else 0.0,
        }

    per_source = {
        'facebook': _metrics(buckets['facebook'], fb_spend),
        'google':   _metrics(buckets['google'],   google_spend),
        'outros':   _metrics(buckets['outros'],   0.0),
    }

    total_spend   = fb_spend + google_spend
    total_mqls    = sum(b['mqls']    for b in buckets.values())
    total_wons    = sum(b['wons']    for b in buckets.values())
    total_receita = sum(b['receita'] for b in buckets.values())

    consolidado = {
        'spend':     round(total_spend, 2),
        'mqls':      total_mqls, 'wons': total_wons,
        'receita':   round(total_receita, 2),
        'cpm':       round(total_spend / total_mqls, 2)     if total_mqls > 0 else 0.0,
        'cpa':       round(total_spend / total_wons, 2)     if total_wons > 0 else 0.0,
        'ticket':    round(total_receita / total_wons, 2)   if total_wons > 0 else 0.0,
        'conv_rate': round(100 * total_wons / total_mqls, 2) if total_mqls > 0 else 0.0,
        'roas':      round(total_receita / total_spend, 2)  if total_spend > 0 else 0.0,
    }

    return jsonify({
        'success':     True,
        'period':      {'since': str(since_d) if since_d else None,
                        'until': str(until_d) if until_d else None,
                        'preset': date_preset},
        'consolidado': consolidado,
        'por_fonte':   per_source,
        'google_conversions_sheet': float((gads.get('totals') or {}).get('conversions', 0.0)),
        'vinci_error': gads.get('error'),
    })
