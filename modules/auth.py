"""
Módulo de autenticação — Login email/senha + OAuth Meta.
Fluxo: Login/Registro → Conectar Meta (se primeiro acesso) → Dashboard
"""

import os
import hashlib
import secrets
import logging
from functools import wraps
from flask import Blueprint, request, redirect, session, url_for, render_template
from urllib.parse import quote

from modules.database import fetch_one, execute_returning, execute

logger = logging.getLogger(__name__)

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')

APP_ID = os.getenv('APP_ID')
APP_SECRET = os.getenv('APP_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI', 'http://localhost:5000/callback')


# --- Helpers de senha ---

def hash_password(password):
    """Hash com salt usando SHA-256. Simples e sem dependência extra."""
    salt = secrets.token_hex(16)
    hashed = hashlib.sha256(f"{salt}{password}".encode()).hexdigest()
    return f"{salt}:{hashed}"


def verify_password(password, stored_hash):
    """Verifica senha contra hash armazenado."""
    salt, hashed = stored_hash.split(':')
    return hashlib.sha256(f"{salt}{password}".encode()).hexdigest() == hashed


# --- Decorator de autenticação ---

def login_required(f):
    """Decorator que exige login do app (email/senha)."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user_id'):
            return redirect(url_for('auth.login_page'))
        return f(*args, **kwargs)
    return decorated


def meta_required(f):
    """Decorator que exige login do app + Meta conectado."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user_id'):
            return redirect(url_for('auth.login_page'))
        if not session.get('access_token'):
            return redirect(url_for('auth.connect_meta_page'))
        return f(*args, **kwargs)
    return decorated


# --- Rotas de Login/Registro ---

@auth_bp.route('/login', methods=['GET'])
def login_page():
    if session.get('user_id'):
        return redirect(url_for('index'))
    return render_template('auth/login.html')


@auth_bp.route('/login', methods=['POST'])
def login_submit():
    email = request.form.get('email', '').strip().lower()
    password = request.form.get('password', '')

    if not email or not password:
        return render_template('auth/login.html', error='Preencha todos os campos.')

    user = fetch_one("SELECT id, email, password_hash FROM app_users WHERE email = %s", (email,))

    if not user or not verify_password(password, user['password_hash']):
        return render_template('auth/login.html', error='Email ou senha incorretos.')

    # Login OK — setar sessão
    session['user_id'] = str(user['id'])
    session['user_email'] = user['email']

    # Verificar se tem Meta token no banco
    token_row = fetch_one(
        "SELECT access_token FROM user_meta_tokens WHERE user_id = %s",
        (user['id'],)
    )
    if token_row:
        session['access_token'] = token_row['access_token']
        return redirect(url_for('index'))

    # Sem Meta conectado — redirecionar para conectar
    return redirect(url_for('auth.connect_meta_page'))


@auth_bp.route('/register', methods=['GET'])
def register_page():
    if session.get('user_id'):
        return redirect(url_for('index'))
    return render_template('auth/register.html')


@auth_bp.route('/register', methods=['POST'])
def register_submit():
    email = request.form.get('email', '').strip().lower()
    password = request.form.get('password', '')
    password_confirm = request.form.get('password_confirm', '')

    if not email or not password:
        return render_template('auth/register.html', error='Preencha todos os campos.')

    if password != password_confirm:
        return render_template('auth/register.html', error='As senhas não coincidem.')

    if len(password) < 6:
        return render_template('auth/register.html', error='A senha deve ter pelo menos 6 caracteres.')

    # Verificar se email já existe
    existing = fetch_one("SELECT id FROM app_users WHERE email = %s", (email,))
    if existing:
        return render_template('auth/register.html', error='Este email já está cadastrado.')

    # Criar usuário
    new_user = execute_returning(
        "INSERT INTO app_users (email, password_hash) VALUES (%s, %s) RETURNING id, email",
        (email, hash_password(password))
    )

    # Login automático após registro
    session['user_id'] = str(new_user['id'])
    session['user_email'] = new_user['email']

    # Redirecionar para conectar Meta
    return redirect(url_for('auth.connect_meta_page'))


# --- Conectar Meta (OAuth) ---

@auth_bp.route('/connect-meta', methods=['GET'])
@login_required
def connect_meta_page():
    scopes = 'public_profile,email,ads_read,ads_management,pages_show_list,pages_read_engagement,instagram_basic,read_insights,pages_manage_ads'
    encoded_uri = quote(REDIRECT_URI)
    auth_url = (
        f"https://www.facebook.com/v22.0/dialog/oauth?"
        f"client_id={APP_ID}&redirect_uri={encoded_uri}&scope={scopes}"
    )
    return render_template('auth/connect_meta.html', auth_url=auth_url)


@auth_bp.route('/callback', endpoint='meta_callback')
def meta_callback():
    """Callback do OAuth Meta — salva token no banco vinculado ao user."""
    import requests as req

    code = request.args.get('code')
    if not code:
        return "Erro: Código de autorização não recebido.", 400

    user_id = session.get('user_id')
    if not user_id:
        return redirect(url_for('auth.login_page'))

    encoded_uri = quote(REDIRECT_URI)
    token_url = (
        f"https://graph.facebook.com/v22.0/oauth/access_token?"
        f"client_id={APP_ID}&redirect_uri={encoded_uri}&"
        f"client_secret={APP_SECRET}&code={code}"
    )

    response = req.get(token_url).json()
    access_token = response.get('access_token')

    if not access_token:
        logger.error(f"Meta OAuth error: {response}")
        return render_template('auth/connect_meta.html',
                               error=f"Erro ao obter token: {response.get('error', {}).get('message', 'Desconhecido')}",
                               auth_url=url_for('auth.connect_meta_page'))

    # Buscar meta_user_id
    me = req.get(f"https://graph.facebook.com/v22.0/me?access_token={access_token}").json()
    meta_user_id = me.get('id', 'unknown')

    # Upsert token no banco
    existing = fetch_one("SELECT id FROM user_meta_tokens WHERE user_id = %s", (user_id,))
    if existing:
        execute(
            """UPDATE user_meta_tokens
               SET access_token = %s, meta_user_id = %s, updated_at = NOW()
               WHERE user_id = %s""",
            (access_token, meta_user_id, user_id)
        )
    else:
        execute(
            """INSERT INTO user_meta_tokens (user_id, meta_user_id, access_token)
               VALUES (%s, %s, %s)""",
            (user_id, meta_user_id, access_token)
        )

    # Setar na sessão
    session['access_token'] = access_token

    # Compatibilidade: salvar token.json para funções legadas
    import json
    try:
        with open('token.json', 'w') as f:
            json.dump({'access_token': access_token}, f)
    except Exception:
        pass

    # Inicializar API Meta
    from facebook_business.api import FacebookAdsApi
    FacebookAdsApi.init(APP_ID, APP_SECRET, access_token)

    return redirect(url_for('index'))


# --- Logout ---

@auth_bp.route('/logout')
def logout():
    session.clear()
    # Limpar token.json legado
    if os.path.exists('token.json'):
        try:
            os.remove('token.json')
        except Exception:
            pass
    return redirect(url_for('auth.login_page'))
