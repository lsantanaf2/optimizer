"""
Página de Anúncios — Insights consolidados por ad_name em tempo real.

Pipeline:
  1. Insights (level=ad, período) → apenas ads com gasto no período
  2. Parallel: batch-fetch effective_status + video insights
  3. Backend retorna lista raw; frontend consolida por ad_name (pivot)

Nota: o endpoint /insights não aceita ad{effective_status} como campo,
por isso o status é buscado via GET /v22.0?ids=...&fields=effective_status
em paralelo com o request de vídeo.
"""

import json
import concurrent.futures
import requests

from flask import Blueprint, jsonify, render_template, request, session, redirect, url_for

anuncios_bp = Blueprint('anuncios', __name__)

BASE_URL = 'https://graph.facebook.com/v22.0'


# ── Helpers ────────────────────────────────────────────────────────────────────

def _paginate(url, params, timeout=45):
    """Itera paginação cursor da Meta API e retorna lista flat."""
    results = []
    next_url = url
    cur_params = dict(params)
    while next_url:
        resp = requests.get(next_url, params=cur_params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get('data', []))
        paging  = data.get('paging', {})
        cursors = paging.get('cursors', {})
        after   = cursors.get('after')
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
    """Extrai valor numérico de um action_type dentro de uma lista de actions."""
    if not action_list:
        return 0.0
    for a in action_list:
        if a.get('action_type') == action_type:
            return float(a.get('value', 0))
    return 0.0


def _purchases(actions):
    """
    Extrai compras tentando todos os action_types conhecidos, em ordem de prioridade.
    Compatível com pixel (offsite), Facebook/IG Shop (onsite) e app (purchase).
    """
    return (
        _action_value(actions, 'offsite_conversion.fb_pixel_purchase') or
        _action_value(actions, 'onsite_conversion.purchase')           or
        _action_value(actions, 'purchase')                             or
        _action_value(actions, 'omni_purchase')
    )


def _batch_effective_status(ad_ids, token, chunk_size=50):
    """
    Busca effective_status dos ad_ids via GET /v22.0?ids=id1,id2,...
    Retorna dict {ad_id: effective_status}.
    """
    status_map = {}
    chunks = [ad_ids[i:i+chunk_size] for i in range(0, len(ad_ids), chunk_size)]

    def fetch_chunk(chunk):
        resp = requests.get(
            BASE_URL,
            params={
                'ids':          ','.join(chunk),
                'fields':       'id,effective_status',
                'access_token': token,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(chunks) or 1)) as ex:
        futures = [ex.submit(fetch_chunk, c) for c in chunks]
        for f in concurrent.futures.as_completed(futures):
            try:
                for ad_id, obj in f.result().items():
                    status_map[ad_id] = obj.get('effective_status', 'UNKNOWN')
            except Exception as e:
                print(f'⚠️ [anuncios] batch status chunk falhou: {e}')

    return status_map


# ── Rotas ──────────────────────────────────────────────────────────────────────

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

    def _date_params():
        if since and until:
            return {'time_range': json.dumps({'since': since, 'until': until}, separators=(',', ':'))}
        return {'date_preset': date_preset}

    try:
        # ── PASSO 1: Insights core ─────────────────────────────────────────────
        core_params = {
            'level':        'ad',
            'fields':       'ad_id,ad_name,spend,impressions,clicks,ctr,actions',
            'limit':        500,
            'access_token': token,
            **_date_params(),
        }
        insights = _paginate(f'{BASE_URL}/{account_id}/insights', core_params)
        print(f'ℹ️ [anuncios] {len(insights)} linhas de insight no período')

        if not insights:
            return jsonify({'success': True, 'data': []})

        ad_ids = list({item['ad_id'] for item in insights if item.get('ad_id')})

        # ── PASSO 2: Parallel — effective_status + video ───────────────────────
        def fetch_video():
            video_params = {
                'level':        'ad',
                'fields':       'ad_id,video_play_actions,video_p75_watched_actions',
                'limit':        500,
                'access_token': token,
                **_date_params(),
            }
            try:
                return _paginate(f'{BASE_URL}/{account_id}/insights', video_params)
            except Exception as e:
                print(f'⚠️ [anuncios] video insights indisponível: {e}')
                return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            f_status = ex.submit(_batch_effective_status, ad_ids, token)
            f_video  = ex.submit(fetch_video)
            status_map = f_status.result()
            video_rows = f_video.result()

        # ── PASSO 3: Mapa de vídeo por ad_id ──────────────────────────────────
        video_map = {}
        for v in video_rows:
            vid = v.get('ad_id', '')
            if vid:
                video_map[vid] = {
                    'video_3s':  _action_value(v.get('video_play_actions'),        'video_view'),
                    'video_p75': _action_value(v.get('video_p75_watched_actions'), 'video_view'),
                }

        # ── PASSO 4: Monta resultado normalizado ───────────────────────────────
        result = []
        for item in insights:
            ad_id   = item.get('ad_id', '')
            actions = item.get('actions') or []
            vid     = video_map.get(ad_id, {})

            result.append({
                'ad_id':            ad_id,
                'ad_name':          item.get('ad_name', ''),
                'spend':            float(item.get('spend', 0) or 0),
                'impressions':      int(item.get('impressions', 0) or 0),
                'clicks':           int(item.get('clicks', 0) or 0),
                'ctr':              float(item.get('ctr', 0) or 0),
                'purchases':        _purchases(actions),
                'video_3s':         vid.get('video_3s',  0.0),
                'video_p75':        vid.get('video_p75', 0.0),
                'effective_status': status_map.get(ad_id, 'UNKNOWN'),
            })

        print(f'✅ [anuncios] retornando {len(result)} registros')
        return jsonify({'success': True, 'data': result})

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f'❌ [anuncios] Erro: {tb}')
        return jsonify({'success': False, 'error': str(e), 'traceback': tb}), 500
