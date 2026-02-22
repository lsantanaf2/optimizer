from flask import Blueprint, jsonify, render_template

from app import obter_token, inicializar_api

optimization_bp = Blueprint('optimization', __name__)

@optimization_bp.route('/optimize/<account_id>')
def otimizar_campanhas(account_id):
    """
    Página principal do módulo de Otimização.
    """
    token = obter_token()
    if not token:
        return jsonify({"error": "Não autenticado"}), 401
    
    return render_template('optimizer.html', account_id=account_id)

@optimization_bp.route('/api/insights/<account_id>')
def api_insights(account_id):
    """
    Retorna dados de performance das campanhas.
    """
    token = obter_token()
    if not token:
        return jsonify({"error": "Não autenticado"}), 401

    uploader = inicializar_api(token)
    if not uploader:
        return jsonify({"error": "Falha na inicialização da API"}), 500

    try:
        uploader.account_id = account_id
        insights_data = uploader.get_campaign_insights(date_preset='today')
        return jsonify({"status": "success", "data": insights_data})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
