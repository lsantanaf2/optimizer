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
    """
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

def fetch_meta_ads_daily(account_id, access_token, conversion_event,
                         date_preset='last_7_days', since=None, until=None):
    """
    Busca insights agregados por DATA para um account_id.
    Aceita conversion_event como parâmetro (não usa global do cruzamento).
    Retorna lista de dicts ordenada por data:
        { date, spend, impressions, clicks, lpv, conversions }
    """
    from modules.cruzamento import preset_to_dates

    base_url = f'https://graph.facebook.com/v22.0/{account_id}/insights'
    params = {
        'access_token': access_token,
        'level':        'account',
        'fields':       'spend,impressions,inline_link_clicks,landing_page_views,actions,date_start',
        'limit':        90,
        'time_increment': 1,
    }

    if since and until:
        params['time_range'] = json.dumps({'since': since, 'until': until})
    else:
        since_d, until_d = preset_to_dates(date_preset)
        if since_d and until_d:
            params['time_range'] = json.dumps({'since': str(since_d), 'until': str(until_d)})
        else:
            params['date_preset'] = 'last_30d'

    rows = []
    url = base_url
    while url:
        resp = _req.get(url, params=params if url == base_url else None, timeout=30)
        resp.raise_for_status()
        body = resp.json()

        for item in body.get('data', []):
            actions = item.get('actions', [])

            def _conv(_a=actions):
                if not conversion_event:
                    return 0
                return sum(
                    int(float(a.get('value', 0) or 0))
                    for a in _a if a.get('action_type') == conversion_event
                )

            rows.append({
                'date':        item.get('date_start', ''),
                'spend':       round(float(item.get('spend', 0) or 0), 2),
                'impressions': int(item.get('impressions', 0) or 0),
                'clicks':      int(item.get('inline_link_clicks', 0) or 0),
                'lpv':         int(item.get('landing_page_views', 0) or 0),
                'conversions': _conv(),
            })

        url = body.get('paging', {}).get('next')
        params = None

    rows.sort(key=lambda r: r['date'])
    return rows


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

    # Token Meta: usa token persistido do sistema (token.json)
    from app import obter_token
    meta_token = obter_token()
    if not meta_token:
        return jsonify({'success': False,
                        'error': 'Sistema não autenticado. Contate o administrador.'}), 503

    date_preset = request.args.get('date_preset', 'last_7_days')
    since       = request.args.get('since')
    until       = request.args.get('until')

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

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'X-Accel-Buffering': 'no', 'Cache-Control': 'no-cache'},
    )


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
    if not meta_token:
        return jsonify({'success': False,
                        'error': 'Sistema não autenticado. Contate o administrador.'}), 503

    date_preset = request.args.get('date_preset', 'last_7_days')
    since       = request.args.get('since')
    until       = request.args.get('until')

    def _sse(stage, payload):
        return f"data: {json.dumps({'stage': stage, **payload})}\n\n"

    def generate():
        try:
            t0      = time.time()
            display = client.get('display_name') or slug

            yield _sse('status', {'message': f'Buscando Meta Ads para {display}...'})

            rows = fetch_meta_ads_daily(
                meta_account_id, meta_token, conversion_event,
                date_preset, since, until,
            )

            yield _sse('status', {'message': f'Processando {len(rows)} dias...'})

            # Totais
            total_spend       = round(sum(r['spend']       for r in rows), 2)
            total_impressions = sum(r['impressions'] for r in rows)
            total_clicks      = sum(r['clicks']      for r in rows)
            total_lpv         = sum(r['lpv']         for r in rows)
            total_conv        = sum(r['conversions'] for r in rows)
            total_revenue     = round(total_conv * ticket_value, 2)
            roas              = round(total_revenue / total_spend, 2) if total_spend > 0 else 0.0

            # Métricas derivadas por dia
            daily = []
            for r in rows:
                sp, cl, im, lp, cv = (
                    r['spend'], r['clicks'], r['impressions'],
                    r['lpv'], r['conversions'],
                )
                daily.append({
                    'date':         r['date'],
                    'spend':        sp,
                    'impressions':  im,
                    'clicks':       cl,
                    'lpv':          lp,
                    'conversions':  cv,
                    'ctr':          round(cl / im * 100, 2) if im > 0 else 0.0,
                    'connect_rate': round(lp / cl * 100, 2) if cl > 0 else 0.0,
                    'revenue':      round(cv * ticket_value, 2),
                    'roas':         round((cv * ticket_value) / sp, 2) if sp > 0 else 0.0,
                })

            elapsed = round(time.time() - t0, 2)

            yield _sse('kpis', {
                'totals': {
                    'spend':       total_spend,
                    'impressions': total_impressions,
                    'clicks':      total_clicks,
                    'lpv':         total_lpv,
                    'conversions': total_conv,
                    'revenue':     total_revenue,
                    'roas':        roas,
                    'ticket':      ticket_value,
                    'ctr':         round(total_clicks / total_impressions * 100, 2) if total_impressions > 0 else 0.0,
                    'connect_rate': round(total_lpv / total_clicks * 100, 2) if total_clicks > 0 else 0.0,
                }
            })

            yield _sse('daily', {'rows': daily})

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

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'X-Accel-Buffering': 'no', 'Cache-Control': 'no-cache'},
    )


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
