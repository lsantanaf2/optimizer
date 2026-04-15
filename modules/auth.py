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
from modules.token_crypto import encrypt_token, decrypt_token, is_encrypted

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

    # Verificar se tem Meta token no banco (decifra se necessário)
    token_row = fetch_one(
        "SELECT access_token FROM user_meta_tokens WHERE user_id = %s",
        (user['id'],)
    )
    if token_row:
        stored = token_row['access_token']
        # Re-criptografa tokens legados em plaintext encontrados no banco
        if not is_encrypted(stored):
            try:
                execute(
                    "UPDATE user_meta_tokens SET access_token = %s WHERE user_id = %s",
                    (encrypt_token(stored), user['id'])
                )
            except Exception as e:
                logger.warning(f"Falha ao re-criptografar token legado: {e}")
        try:
            session['access_token'] = decrypt_token(stored)
            return redirect(url_for('index'))
        except Exception as e:
            # Token cifrado com APP_SECRET antigo ou corrompido → remove e manda reconectar
            logger.warning(f"Token Meta corrompido/incompatível para user {user['id']}: {e}. Removendo e pedindo reconexão.")
            try:
                execute("DELETE FROM user_meta_tokens WHERE user_id = %s", (user['id'],))
            except Exception as ee:
                logger.error(f"Falha ao remover token corrompido: {ee}")
            return redirect(url_for('auth.connect_meta_page'))

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
    scopes = 'public_profile,email,ads_read,ads_management,pages_show_list,instagram_basic,read_insights,pages_manage_ads,leads_retrieval'
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

    # ── Trocar short-lived (~1-2h) por long-lived token (~60 dias) ──
    expires_at = None
    try:
        ll_url = (
            f"https://graph.facebook.com/v22.0/oauth/access_token?"
            f"grant_type=fb_exchange_token&client_id={APP_ID}&"
            f"client_secret={APP_SECRET}&fb_exchange_token={access_token}"
        )
        ll_resp = req.get(ll_url, timeout=15).json()
        if ll_resp.get('access_token'):
            access_token = ll_resp['access_token']
            expires_in = ll_resp.get('expires_in', 5184000)  # default 60 dias
            from datetime import datetime, timedelta
            expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
            logger.info(f"Token long-lived obtido. Expira em {expires_in // 86400} dias ({expires_at.isoformat()})")
        else:
            logger.warning(f"Falha ao trocar por long-lived token: {ll_resp}. Usando short-lived.")
    except Exception as e:
        logger.warning(f"Erro ao trocar token: {e}. Usando short-lived.")

    # Buscar meta_user_id
    me = req.get(f"https://graph.facebook.com/v22.0/me?access_token={access_token}").json()
    meta_user_id = me.get('id', 'unknown')

    # Upsert token no banco (criptografado em repouso)
    encrypted = encrypt_token(access_token)
    existing = fetch_one("SELECT id FROM user_meta_tokens WHERE user_id = %s", (user_id,))
    if existing:
        execute(
            """UPDATE user_meta_tokens
               SET access_token = %s, meta_user_id = %s, updated_at = NOW(),
                   expires_at = %s
               WHERE user_id = %s""",
            (encrypted, meta_user_id, expires_at, user_id)
        )
    else:
        execute(
            """INSERT INTO user_meta_tokens (user_id, meta_user_id, access_token, expires_at)
               VALUES (%s, %s, %s, %s)""",
            (user_id, meta_user_id, encrypted, expires_at)
        )

    # Setar na sessão (plaintext — nunca armazenamos o token cifrado na sessão)
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


# --- Minha Conta ---

@auth_bp.route('/account/profile', methods=['GET'])
@login_required
def account_profile():
    """Página Minha Conta — perfil, senha, conexão Meta."""
    user_id = session['user_id']

    # Dados do usuário
    user = fetch_one(
        "SELECT email, plan, created_at FROM app_users WHERE id = %s",
        (user_id,)
    )
    if not user:
        return redirect(url_for('auth.login_page'))

    plan_labels = {
        'free': 'Plano Gratuito',
        'pro': 'Plano Profissional',
        'enterprise': 'Plano Enterprise'
    }

    # Dados Meta
    meta_row = fetch_one(
        "SELECT meta_user_id, updated_at, expires_at FROM user_meta_tokens WHERE user_id = %s",
        (user_id,)
    )

    # URL de re-autenticação Meta
    scopes = 'public_profile,email,ads_read,ads_management,pages_show_list,instagram_basic,read_insights,pages_manage_ads,leads_retrieval'
    encoded_uri = quote(REDIRECT_URI)
    reauth_url = (
        f"https://www.facebook.com/v22.0/dialog/oauth?"
        f"client_id={APP_ID}&redirect_uri={encoded_uri}&scope={scopes}"
        f"&auth_type=reauthorize"
    )

    created_str = user['created_at'].strftime('%d/%m/%Y') if user.get('created_at') else '—'

    # Calcular dias restantes do token
    meta_expires_str = None
    meta_expires_days = None
    if meta_row and meta_row.get('expires_at'):
        from datetime import datetime
        meta_expires_str = meta_row['expires_at'].strftime('%d/%m/%Y %H:%M')
        delta = meta_row['expires_at'].replace(tzinfo=None) - datetime.utcnow()
        meta_expires_days = max(0, delta.days)

    return render_template('account/profile.html',
        user_email=user['email'],
        plan=user.get('plan', 'free'),
        plan_label=plan_labels.get(user.get('plan', 'free'), 'Plano Gratuito'),
        created_at=created_str,
        meta_connected=bool(meta_row),
        meta_user_id=meta_row['meta_user_id'] if meta_row else None,
        meta_updated_at=meta_row['updated_at'].strftime('%d/%m/%Y %H:%M') if meta_row and meta_row.get('updated_at') else None,
        meta_expires_at=meta_expires_str,
        meta_expires_days=meta_expires_days,
        reauth_url=reauth_url
    )


@auth_bp.route('/change-password', methods=['POST'])
@login_required
def change_password():
    """Altera senha do usuário logado."""
    import json as _json
    data = request.get_json() or {}
    current_pwd = data.get('current_password', '')
    new_pwd = data.get('new_password', '')

    if not current_pwd or not new_pwd:
        return {'error': 'Preencha todos os campos.'}, 400

    if len(new_pwd) < 6:
        return {'error': 'A nova senha deve ter pelo menos 6 caracteres.'}, 400

    user = fetch_one(
        "SELECT password_hash FROM app_users WHERE id = %s",
        (session['user_id'],)
    )
    if not user or not verify_password(current_pwd, user['password_hash']):
        return {'error': 'Senha atual incorreta.'}, 400

    execute(
        "UPDATE app_users SET password_hash = %s, updated_at = NOW() WHERE id = %s",
        (hash_password(new_pwd), session['user_id'])
    )
    return {'success': True}


@auth_bp.route('/disconnect-meta', methods=['POST'])
@login_required
def disconnect_meta():
    """Remove token Meta do banco e da sessão."""
    user_id = session['user_id']
    execute("DELETE FROM user_meta_tokens WHERE user_id = %s", (user_id,))
    session.pop('access_token', None)
    session.pop('account_id', None)
    session.pop('account_name', None)

    # Limpar token.json legado
    if os.path.exists('token.json'):
        try:
            os.remove('token.json')
        except Exception:
            pass

    return {'success': True}


# --- Presets da Conta de Anúncios ---

@auth_bp.route('/account/presets', methods=['GET'])
@login_required
def account_presets():
    """Página de gestão de presets por conta de anúncios."""
    from modules.account_settings import list_imported_accounts, get_or_create_imported_account
    user_id = session['user_id']
    accounts = list_imported_accounts(user_id)

    # Tentar preencher nomes faltantes via Meta API
    access_token = session.get('access_token')
    if access_token:
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.adaccount import AdAccount
        try:
            FacebookAdsApi.init(APP_ID, APP_SECRET, access_token)
            for acc in accounts:
                if not acc.get('account_name') or acc['account_name'] == acc['meta_account_id']:
                    try:
                        meta_acc = AdAccount(f"act_{acc['meta_account_id']}")
                        info = meta_acc.api_get(fields=['name'])
                        real_name = info.get('name')
                        if real_name:
                            acc['account_name'] = real_name
                            get_or_create_imported_account(user_id, acc['meta_account_id'], real_name)
                    except Exception:
                        pass
        except Exception:
            pass

    return render_template('account/presets.html', accounts=accounts)


# --- Histórico de Uploads ---

@auth_bp.route('/account/history', methods=['GET'])
@login_required
def account_history():
    """Página de histórico de uploads do usuário."""
    from modules.account_settings import get_upload_history
    user_id = session['user_id']
    history = get_upload_history(user_id, limit=100)
    return render_template('account/history.html', history=history)


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
