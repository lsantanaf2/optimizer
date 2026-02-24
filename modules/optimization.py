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
