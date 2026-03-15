"""
Página de Anúncios — Insights consolidados por ad_name em tempo real.

Fluxo:
  1. Fetch paralelo: insights por anúncio + effective_status de cada ad
  2. Merge por ad_id → enriquece cada insight com effective_status
  3. Retorna lista raw; frontend faz consolidação por ad_name via reduce()
"""

import os
import json
import concurrent.futures
import requests

from flask import Blueprint, jsonify, render_template, request, session, redirect, url_for

# ── Blueprint ──────────────────────────────────────────────────────────────────
anuncios_bp = Blueprint('anuncios', __name__)

APP_ID     = os.getenv('APP_ID')
APP_SECRET = os.getenv('APP_SECRET')
BASE_URL   = 'https://graph.facebook.com/v22.0'


# ── Helper: paginação automática ──────────────────────────────────────────────
def _paginate(url, params, timeout=30):
    results = []
    next_url = url
    cur_params = dict(params)
    while next_url:
        resp = requests.get(next_url, params=cur_params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get('data', []))
        paging = data.get('paging', {})
        cursors = paging.get('cursors', {})
        after = cursors.get('after')
        if after:
            cur_params = dict(params)
            cur_params['after'] = after
            next_url = url
        elif 'next' in paging:
            next_url = paging['next']
            cur_params = {}
        else:
            break
    return results


def _action_value(action_list, action_type):
    """Extrai o valor de uma action pelo action_type."""
    if not action_list:
        return 0.0
    for a in action_list:
        if a.get('action_type') == action_type:
            return float(a.get('value', 0))
    return 0.0


# ── Rotas ─────────────────────────────────────────────────────────────────────
@anuncios_bp.route('/account/<account_id>/anuncios')
def anuncios_page(account_id):
    from app import obter_token
    token = obter_token()
    if not token:
        return redirect(url_for('pagina_login'))
    session['account_id'] = account_id
    return render_template('anuncios.html', account_id=account_id)


@anuncios_bp.route('/api/account/<account_id>/anuncios-data')
def api_anuncios_data(account_id):
    from app import obter_token
    token = obter_token()
    if not token:
        return jsonify({'success': False, 'error': 'Não autenticado'}), 401

    since       = request.args.get('since')
    until       = request.args.get('until')
    date_preset = request.args.get('date_preset', 'last_7d')

    try:
        # ── 1. Insights ────────────────────────────────────────────────────────
        def fetch_insights():
            fields = (
                'ad_id,ad_name,spend,impressions,clicks,ctr,'
                'actions,'
                'video_3_sec_watched_actions,'
                'video_p75_watched_actions'
            )
            params = {
                'level': 'ad',
                'fields': fields,
                'limit': 500,
                'access_token': token,
            }
            if since and until:
                params['time_range'] = json.dumps({'since': since, 'until': until})
            else:
                params['date_preset'] = date_preset
            return _paginate(f'{BASE_URL}/{account_id}/insights', params)

        # ── 2. Effective status de todos os ads ────────────────────────────────
        def fetch_effective_status():
            params = {
                'fields': 'id,effective_status',
                'limit': 500,
                'access_token': token,
            }
            return _paginate(f'{BASE_URL}/{account_id}/ads', params)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            f_ins = ex.submit(fetch_insights)
            f_eff = ex.submit(fetch_effective_status)
            insights    = f_ins.result()
            ads_status  = f_eff.result()

        # ── 3. Mapa ad_id → effective_status ──────────────────────────────────
        status_map = {ad['id']: ad.get('effective_status', 'UNKNOWN') for ad in ads_status}

        # ── 4. Normaliza cada linha de insight ─────────────────────────────────
        result = []
        for item in insights:
            ad_id = item.get('ad_id', '')

            purchases = _action_value(item.get('actions'), 'purchase')
            if purchases == 0:
                purchases = _action_value(item.get('actions'), 'omni_purchase')

            video_3s  = _action_value(item.get('video_3_sec_watched_actions'), 'video_view')
            video_p75 = _action_value(item.get('video_p75_watched_actions'),   'video_view')

            result.append({
                'ad_id':            ad_id,
                'ad_name':          item.get('ad_name', ''),
                'spend':            float(item.get('spend', 0) or 0),
                'impressions':      int(item.get('impressions', 0) or 0),
                'clicks':           int(item.get('clicks', 0) or 0),
                'ctr':              float(item.get('ctr', 0) or 0),
                'purchases':        purchases,
                'video_3s':         video_3s,
                'video_p75':        video_p75,
                'effective_status': status_map.get(ad_id, 'UNKNOWN'),
            })

        return jsonify({'success': True, 'data': result})

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f'❌ [anuncios] Erro: {tb}')
        return jsonify({'success': False, 'error': str(e), 'traceback': tb}), 500
