from flask import Blueprint, request, redirect, session, url_for
from urllib.parse import quote
import os

# Create Blueprint
auth_bp = Blueprint('auth', __name__)

APP_ID = os.getenv('APP_ID')
REDIRECT_URI = os.getenv('REDIRECT_URI', 'http://localhost:5000/callback')

from app import limpar_token, salvar_token

@auth_bp.route('/login')
def pagina_login():
    scopes = "ads_management,ads_read,read_insights,pages_read_engagement,pages_show_list,pages_manage_ads,pages_manage_metadata,instagram_basic,instagram_manage_insights,business_management,leads_retrieval"
    url = (
        f"https://www.facebook.com/v18.0/dialog/oauth?"
        f"client_id={APP_ID}&redirect_uri={quote(REDIRECT_URI)}&scope={quote(scopes)}"
    )
    return redirect(url)

@auth_bp.route('/callback')
def callback():
    code = request.args.get('code')
    if not code:
        return "Erro: Código de autorização não retornado pelo Facebook.", 400

    import requests
    from app import APP_SECRET
    token_url = (
        f"https://graph.facebook.com/v18.0/oauth/access_token?"
        f"client_id={APP_ID}&redirect_uri={REDIRECT_URI}&"
        f"client_secret={APP_SECRET}&code={code}"
    )
    
    response = requests.get(token_url)
    data = response.json()
    
    if "access_token" in data:
        session['access_token'] = data["access_token"]
        salvar_token(data["access_token"])
        return redirect(url_for('index'))
    return f"Erro ao obter token: {data}", 400

@auth_bp.route('/logout')
def logout():
    limpar_token()
    return redirect(url_for('pagina_login'))
