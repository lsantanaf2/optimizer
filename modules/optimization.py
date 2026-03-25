from flask import Blueprint, jsonify, render_template, request, session, redirect, url_for
import os



optimization_bp = Blueprint('optimization', __name__)

APP_ID = os.getenv('APP_ID')
APP_SECRET = os.getenv('APP_SECRET')

def _get_date_params():
    """Extrai parâmetros de data da request: date_preset OU since/until."""
    since = request.args.get('since')
    until = request.args.get('until')
    date_preset = request.args.get('date_preset', 'today')
    return date_preset, since, until

@optimization_bp.route('/account/<account_id>/otimizar')
def otimizar_campanhas(account_id):
    from app import obter_token
    token = obter_token()
    if not token:
        return redirect(url_for('pagina_login'))
    session['account_id'] = account_id
    return render_template('optimizer.html', account_id=account_id)

# ======================== DATA ENDPOINTS ========================

@optimization_bp.route('/api/account/<account_id>/campaigns')
def api_campaigns(account_id):
    from app import obter_token
    from meta_api import MetaUploader

    token = obter_token()
    if not token:
        return jsonify({"success": False, "error": "Não autenticado"}), 401

    date_preset, since, until = _get_date_params()

    try:
        uploader = MetaUploader(account_id, token, APP_ID, APP_SECRET)
        data = uploader.get_campaigns_list(date_preset=date_preset, since=since, until=until)
        return jsonify({"success": True, "data": data})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@optimization_bp.route('/api/account/<account_id>/adsets')
def api_adsets(account_id):
    from app import obter_token
    from meta_api import MetaUploader

    token = obter_token()
    if not token:
        return jsonify({"success": False, "error": "Não autenticado"}), 401

    campaign_ids = request.args.get('campaign_ids', '')
    ids_list = [cid.strip() for cid in campaign_ids.split(',') if cid.strip()] if campaign_ids else []
    date_preset, since, until = _get_date_params()

    try:
        uploader = MetaUploader(account_id, token, APP_ID, APP_SECRET)
        if ids_list:
            data = uploader.get_adsets_list(ids_list, date_preset=date_preset, since=since, until=until)
        else:
            # Sem filtro: buscar todas as campanhas ativas primeiro, depois adsets
            camps = uploader.get_campaigns_list(date_preset=date_preset, since=since, until=until)
            camp_ids = [c['id'] for c in camps]
            data = uploader.get_adsets_list(camp_ids, date_preset=date_preset, since=since, until=until) if camp_ids else []
        return jsonify({"success": True, "data": data})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@optimization_bp.route('/api/account/<account_id>/ads')
def api_ads(account_id):
    from app import obter_token
    from meta_api import MetaUploader

    token = obter_token()
    if not token:
        return jsonify({"success": False, "error": "Não autenticado"}), 401

    adset_ids = request.args.get('adset_ids', '')
    ids_list = [aid.strip() for aid in adset_ids.split(',') if aid.strip()] if adset_ids else []
    date_preset, since, until = _get_date_params()

    try:
        uploader = MetaUploader(account_id, token, APP_ID, APP_SECRET)
        if ids_list:
            data = uploader.get_ads_list(ids_list, date_preset=date_preset, since=since, until=until)
        else:
            # Sem filtro: buscar campanhas -> adsets -> ads
            camps = uploader.get_campaigns_list(date_preset=date_preset, since=since, until=until)
            camp_ids = [c['id'] for c in camps]
            if camp_ids:
                adsets = uploader.get_adsets_list(camp_ids, date_preset=date_preset, since=since, until=until)
                adset_id_list = [a['id'] for a in adsets]
                data = uploader.get_ads_list(adset_id_list, date_preset=date_preset, since=since, until=until) if adset_id_list else []
            else:
                data = []
        return jsonify({"success": True, "data": data})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

# ======================== ACTION ENDPOINTS ========================

@optimization_bp.route('/api/account/<account_id>/entity/status', methods=['POST'])
def api_entity_status(account_id):
    from app import obter_token
    from meta_api import MetaUploader

    token = obter_token()
    if not token:
        return jsonify({"success": False, "error": "Não autenticado"}), 401

    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "JSON inválido"}), 400

    entity_id = data.get('entity_id')
    entity_type = data.get('entity_type', 'unknown')
    new_status = data.get('status')

    if not entity_id or not new_status:
        return jsonify({"success": False, "error": "entity_id e status são obrigatórios"}), 400

    try:
        uploader = MetaUploader(account_id, token, APP_ID, APP_SECRET)
        result = uploader.update_entity_status(entity_id, entity_type, new_status)
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@optimization_bp.route('/api/account/<account_id>/entity/budget', methods=['POST'])
def api_entity_budget(account_id):
    from app import obter_token
    from meta_api import MetaUploader

    token = obter_token()
    if not token:
        return jsonify({"success": False, "error": "Não autenticado"}), 401

    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "JSON inválido"}), 400

    entity_id = data.get('entity_id')
    entity_type = data.get('entity_type', 'unknown')
    daily_budget = data.get('daily_budget')

    if not entity_id or daily_budget is None:
        return jsonify({"success": False, "error": "entity_id e daily_budget são obrigatórios"}), 400

    try:
        uploader = MetaUploader(account_id, token, APP_ID, APP_SECRET)
        result = uploader.update_budget(entity_id, entity_type, daily_budget)
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

# ======================== TURBINADA ========================

@optimization_bp.route('/account/<account_id>/turbinada')
def turbinada_page(account_id):
    from app import obter_token
    token = obter_token()
    if not token:
        return redirect(url_for('pagina_login'))
    session['account_id'] = account_id

    # Buscar CAC máximo salvo no banco
    cac_max = 150  # fallback padrão
    user_id = session.get('user_id')
    if user_id:
        try:
            from modules.account_settings import get_settings_for_setup
            settings = get_settings_for_setup(user_id, account_id)
            if settings.get('cac_target_value'):
                cac_max = settings['cac_target_value']
        except Exception:
            pass

    return render_template('turbinada.html', account_id=account_id, cac_max=cac_max)

@optimization_bp.route('/api/account/<account_id>/turbinada/<level>')
def api_turbinada(account_id, level):
    from app import obter_token
    from meta_api import MetaUploader

    token = obter_token()
    if not token:
        return jsonify({"success": False, "error": "Não autenticado"}), 401

    if level not in ('campaign', 'adset', 'ad'):
        return jsonify({"success": False, "error": f"Nível inválido: {level}"}), 400

    if not APP_ID or not APP_SECRET:
        return jsonify({"success": False, "error": "APP_ID ou APP_SECRET não configurados"}), 500

    parent_ids = request.args.get('parent_ids', '')
    parent_list = [pid.strip() for pid in parent_ids.split(',') if pid.strip()] if parent_ids else None
    parent_type = request.args.get('parent_type', None)  # 'campaign' ou 'adset'

    # status_filter=ACTIVE para trazer só ativas (padrão selecionado na UI)
    status_filter = request.args.get('status_filter', None)  # ex: "ACTIVE"

    # Períodos dinâmicos enviados pelo frontend como JSON
    periods_json = request.args.get('periods', None)
    periods_dict = None
    if periods_json:
        try:
            import json
            periods_dict = json.loads(periods_json)
        except Exception:
            periods_dict = None

    try:
        uploader = MetaUploader(account_id, token, APP_ID, APP_SECRET)
        data = uploader.get_turbinada_data(level=level, parent_ids=parent_list, parent_type=parent_type, status_filter=status_filter, periods=periods_dict)
        return jsonify({"success": True, "data": data})
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"❌ [turbinada] Erro: {tb}")
        return jsonify({"success": False, "error": str(e), "traceback": tb}), 500


# ======================== VISUALIZATION MODES (Squad 5) ========================

@optimization_bp.route('/api/viz-modes', methods=['GET'])
def api_get_viz_modes():
    """Retorna os modos de visualização salvos pelo usuário."""
    from modules.account_settings import get_viz_modes
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Não autenticado'}), 401
    modes = get_viz_modes(user_id)
    return jsonify({'modes': modes})


@optimization_bp.route('/api/viz-modes', methods=['POST'])
def api_save_viz_mode():
    """Salva ou atualiza um modo de visualização."""
    from modules.account_settings import save_viz_mode
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Não autenticado'}), 401

    data = request.get_json() or {}
    mode_name = data.get('mode_name', '').strip()
    periods = data.get('periods', {})
    is_default = data.get('is_default', False)
    mode_id = data.get('mode_id')

    if not mode_name:
        return jsonify({'error': 'Nome do modo é obrigatório'}), 400
    if not periods.get('columns'):
        return jsonify({'error': 'Períodos inválidos'}), 400

    saved_id = save_viz_mode(user_id, mode_name, periods, is_default, mode_id)
    if saved_id:
        return jsonify({'success': True, 'mode_id': saved_id})
    return jsonify({'error': 'Banco de dados indisponível'}), 503


@optimization_bp.route('/api/viz-modes/<mode_id>', methods=['DELETE'])
def api_delete_viz_mode(mode_id):
    """Remove um modo de visualização."""
    from modules.account_settings import delete_viz_mode
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Não autenticado'}), 401
    delete_viz_mode(user_id, mode_id)
    return jsonify({'success': True})
