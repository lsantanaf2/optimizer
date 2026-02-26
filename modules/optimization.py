from flask import Blueprint, jsonify, render_template, request, session, redirect, url_for
import os



optimization_bp = Blueprint('optimization', __name__)

APP_ID = os.getenv('APP_ID')
APP_SECRET = os.getenv('APP_SECRET')

@optimization_bp.route('/account/<account_id>/otimizar')
def otimizar_campanhas(account_id):
    """
    Página principal do módulo de Otimização.
    """
    from app import obter_token
    token = obter_token()
    if not token:
        return redirect(url_for('pagina_login'))
    
    session['account_id'] = account_id
    return render_template('optimizer.html', account_id=account_id)

@optimization_bp.route('/api/account/<account_id>/insights')
def api_insights(account_id):
    """
    Retorna dados de performance das campanhas.
    """
    from app import obter_token
    from meta_api import MetaUploader
    
    token = obter_token()
    if not token:
        return jsonify({"success": False, "error": "Não autenticado"}), 401

    date_preset = request.args.get('date_preset', 'today')

    try:
        uploader = MetaUploader(account_id, token, APP_ID, APP_SECRET)
        insights_data = uploader.get_campaign_insights(date_preset=date_preset)
        return jsonify({"success": True, "data": insights_data})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@optimization_bp.route('/api/account/<account_id>/campaign-tree')
def api_campaign_tree(account_id):
    """
    Retorna hierarquia completa: Campanhas → Ad Sets → Ads.
    """
    from app import obter_token
    from meta_api import MetaUploader

    token = obter_token()
    if not token:
        return jsonify({"success": False, "error": "Não autenticado"}), 401

    date_preset = request.args.get('date_preset', 'today')

    try:
        uploader = MetaUploader(account_id, token, APP_ID, APP_SECRET)
        tree = uploader.get_campaign_tree(date_preset=date_preset)
        return jsonify({"success": True, "data": tree})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@optimization_bp.route('/api/account/<account_id>/entity/status', methods=['POST'])
def api_entity_status(account_id):
    """
    Altera o status (PAUSED/ACTIVE) de uma entidade (campaign, adset, ad).
    Body JSON: {entity_id, entity_type, status}
    """
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
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@optimization_bp.route('/api/account/<account_id>/entity/budget', methods=['POST'])
def api_entity_budget(account_id):
    """
    Altera o orçamento diário de uma campanha ou adset.
    Body JSON: {entity_id, entity_type, daily_budget}
    """
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
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

