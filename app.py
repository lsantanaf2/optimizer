import os
import json
import time
import requests as req_lib
import tempfile
import queue
import threading
from datetime import datetime
from flask import (
    Flask, request, redirect, session, render_template,
    jsonify, Response, stream_with_context, url_for, copy_current_request_context
)
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.business import Business
from meta_api import MetaUploader, GeoComplianceError

load_dotenv('notepad.env')

APP_ID = os.getenv('APP_ID')
APP_SECRET = os.getenv('APP_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI', 'http://localhost:5000/callback')
TOKEN_FILE = 'token.json'

# ── Validação de variáveis obrigatórias ────────────────────────────────────────
_missing = [k for k, v in {'APP_ID': APP_ID, 'APP_SECRET': APP_SECRET}.items() if not v]
if _missing:
    print(f"🚨 ATENÇÃO: Variáveis de ambiente AUSENTES: {', '.join(_missing)}")
    print("   O login via Facebook NÃO funcionará até que estas variáveis sejam definidas.")
    print("   Use: docker run ... -e APP_ID=<valor> -e APP_SECRET=<valor> ...")

app = Flask(__name__, static_folder='static')
app.secret_key = os.getenv('FLASK_SECRET_KEY') or os.urandom(32)

from modules.optimization import optimization_bp
app.register_blueprint(optimization_bp)

from modules.cruzamento import cruzamento_bp
app.register_blueprint(cruzamento_bp)

from modules.anuncios import anuncios_bp
app.register_blueprint(anuncios_bp)

from modules.instagram_downloader import instagram_dl_bp
app.register_blueprint(instagram_dl_bp)

from modules.auth import auth_bp, login_required, meta_required
app.register_blueprint(auth_bp)

from modules.database import init_db, close_db
from modules.account_settings import (
    get_or_create_imported_account,
    save_upload_assets,
    save_upload_history,
)

import atexit
atexit.register(close_db)

VERSION = "v2.6.2"

@app.before_request
def ensure_db():
    """Inicializa pool de DB no primeiro request de cada worker."""
    init_db()

@app.context_processor
def inject_version():
    return dict(version=VERSION)

# --- Funções auxiliares para persistência do token ---

def salvar_token(access_token):
    """Salva o access_token em um arquivo local."""
    with open(TOKEN_FILE, 'w') as f:
        json.dump({'access_token': access_token}, f)

def carregar_token():
    """Carrega o access_token do arquivo local, se existir."""
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'r') as f:
            data = json.load(f)
            return data.get('access_token')
    return None

def obter_token():
    """Retorna o token da sessão ou do arquivo persistido."""
    token = session.get('access_token')
    if not token:
        token = carregar_token()
        if token:
            session['access_token'] = token
    return token

def inicializar_api(access_token):
    """Inicializa a API do Facebook com o token fornecido."""
    FacebookAdsApi.init(APP_ID, APP_SECRET, access_token)

def limpar_token():
    """Remove o token da sessão e do arquivo."""
    session.pop('access_token', None)
    if os.path.exists(TOKEN_FILE):
        os.remove(TOKEN_FILE)

# --- Rotas ---
ACCOUNTS_CACHE = {} # Memory Cache { token: { 'time': 1234, 'accounts': [...] } }

@app.route('/')
@meta_required
def index():
    token = obter_token()
    if token:
        try:
            inicializar_api(token)
            if not session.get('account_id'):
                return redirect(url_for('listar_contas'))
            return redirect(url_for('listar_campanhas', account_id=session.get('account_id')))
        except Exception:
            limpar_token()
            return redirect(url_for('auth.connect_meta_page'))
    return redirect(url_for('auth.connect_meta_page'))

# Rotas legadas redirecionam para novo auth
@app.route('/login')
def pagina_login():
    return redirect(url_for('auth.login_page'))

@app.route('/callback')
def callback():
    # Delega para o handler do auth, sem redirect (preserva query params do Facebook)
    from modules.auth import meta_callback
    return meta_callback()

@app.route('/logout')
def logout():
    return redirect(url_for('auth.logout'))

@app.route('/set_account/<account_id>')
def set_account(account_id):
    """Define a conta globalmente na sessão e redireciona para a página de campanhas."""
    access_token = obter_token()
    if not access_token:
        return redirect(url_for('auth.login_page'))

    if not account_id.startswith('act_'):
        account_id = f"act_{account_id}"

    session['account_id'] = account_id

    # Busca o nome da conta para exibir no Top Bar
    try:
        inicializar_api(access_token)
        conta = AdAccount(account_id)
        info = conta.api_get(fields=['name'])
        session['account_name'] = info.get('name', 'Conta de Anúncios')
    except Exception as e:
        print(f"❌ Erro ao buscar nome da conta {account_id}: {e}")
        session['account_name'] = account_id

    # Squad 1.3 — persistir conta no banco
    user_id = session.get('user_id')
    if user_id:
        get_or_create_imported_account(user_id, account_id, session['account_name'])

    # Se for chamada AJAX, retorna JSON
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return jsonify({'success': True, 'account_id': account_id, 'account_name': session['account_name']})

    next_page = request.args.get('next', 'upload')
    if next_page == 'optimize':
        return redirect(url_for('optimization.turbinada_page', account_id=account_id))
    return redirect(url_for('listar_campanhas', account_id=account_id))

@app.route('/api/accounts')
def listar_contas():
    token = obter_token()
    if not token:
        return redirect(url_for('auth.login_page'))
        
    # Check cache to prevent slow navigation (holds for 1 hour)
    if token in ACCOUNTS_CACHE:
        if time.time() - ACCOUNTS_CACHE[token]['time'] < 3600:
            return render_template('accounts.html', accounts=ACCOUNTS_CACHE[token]['accounts'])

    try:
        inicializar_api(token)
        me = User(fbid='me')
        # Fetch accounts directly from user context
        # Fields: name, id, currency, status, business info
        contas_raw = me.get_ad_accounts(
            fields=['name', 'account_id', 'currency', 'account_status', 'business_name'],
            params={'limit': 100}
        )

        contas = []
        # SDK handles pagination automatically when iterating
        for conta in contas_raw:
            # We list all accounts, let the template decide how to show non-active ones if needed
            # or filter here. The request didn't specify hiding inactive, but previous code did.
            # Let's keep showing only active (status=1) for cleaner UX, or maybe all?
            # User said "list accounts", passing extra fields. I'll stick to active for now to reduce noise,
            # but I'll store the object.
            
            # Status: 1=ACTIVE, 2=DISABLED, 3=UNSETTLED, 7=PENDING_RISK_REVIEW, etc.
            if conta.get('account_status') == 1: 
                contas.append({
                    'name': conta.get('name'),
                    'account_id': conta.get('account_id'),
                    'currency': conta.get('currency'),
                    'business_name': conta.get('business_name') or 'Conta Pessoal'
                })

        # Save to memory cache
        ACCOUNTS_CACHE[token] = {
            'time': time.time(),
            'accounts': contas
        }

        return render_template('accounts.html', accounts=contas)

    except Exception as e:
        print(f"❌ Error listing accounts: {e}")
        limpar_token()
        return redirect(url_for('index'))

@app.route('/api/accounts/saved')
def api_accounts_saved():
    """Retorna contas salvas no banco para o usuário logado (resposta imediata)."""
    from modules.database import fetch_all
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'accounts': []})
    try:
        rows = fetch_all(
            "SELECT meta_account_id, account_name FROM imported_ad_accounts WHERE user_id = %s ORDER BY pinned_order, created_at DESC",
            (user_id,)
        )
        contas = [{'account_id': r['meta_account_id'], 'name': r['account_name'] or r['meta_account_id'], 'source': 'saved'} for r in (rows or [])]
        return jsonify({'accounts': contas})
    except Exception as e:
        return jsonify({'accounts': []})


@app.route('/api/accounts/json')
def api_accounts_json():
    """Retorna lista de contas da API Meta como JSON (pode ser lento)."""
    token = obter_token()
    if not token:
        return jsonify({'error': 'Not authenticated'}), 401

    # Usar cache se disponível
    if token in ACCOUNTS_CACHE:
        if time.time() - ACCOUNTS_CACHE[token]['time'] < 3600:
            return jsonify({'accounts': ACCOUNTS_CACHE[token]['accounts']})

    try:
        inicializar_api(token)
        me = User(fbid='me')
        contas_raw = me.get_ad_accounts(
            fields=['name', 'account_id', 'currency', 'account_status', 'business_name'],
            params={'limit': 100}
        )
        contas = []
        for conta in contas_raw:
            if conta.get('account_status') == 1:
                contas.append({
                    'name': conta.get('name'),
                    'account_id': conta.get('account_id'),
                    'currency': conta.get('currency'),
                    'business_name': conta.get('business_name') or 'Conta Pessoal',
                    'source': 'meta'
                })
        ACCOUNTS_CACHE[token] = {'time': time.time(), 'accounts': contas}
        return jsonify({'accounts': contas})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/businesses')
def api_businesses():
    """Retorna lista de Business Portfolios (BMs) que o usuário tem acesso."""
    access_token = obter_token()
    if not access_token:
        return jsonify({'error': 'Não autenticado'}), 401
    
    try:
        inicializar_api(access_token)
        me = User(fbid='me')
        bms_raw = me.get_businesses(fields=['name', 'id'])
        
        bms = []
        for bm in bms_raw:
            bms.append({
                'id': bm.get('id'),
                'name': bm.get('name')
            })
        return jsonify({'businesses': bms})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/conta/<account_id>')
def listar_campanhas(account_id):
    """Renderiza a página de campanhas. Dados carregados via AJAX (lazy)."""
    if not account_id.startswith('act_'):
        account_id = f"act_{account_id}"
    access_token = obter_token()
    if not access_token:
        return redirect(url_for('index'))
    session['account_id'] = account_id
    return render_template('campaigns.html', campaigns=[], account_id=account_id)

@app.route('/api/conta/<account_id>/campanhas')
def api_campanhas(account_id):
    """API: lista campanhas da conta. Parâmetros:
       - status: 'ACTIVE' (default) ou 'PAUSED' ou 'ACTIVE,PAUSED'
       - after: cursor para paginação (só para PAUSED)
       - limit: máximo de resultados (default 50)
    """
    if not account_id.startswith('act_'):
        account_id = f"act_{account_id}"
    access_token = obter_token()
    if not access_token:
        return jsonify({'error': 'Não autenticado'}), 401
    try:
        inicializar_api(access_token)

        # Aproveita para setar o nome da conta na sessão se ainda não foi feito
        if not session.get('account_name'):
            try:
                info = AdAccount(account_id).api_get(fields=['name'])
                session['account_name'] = info.get('name', account_id)
            except Exception:
                pass

        status_filter = request.args.get('status', 'ACTIVE')
        after_cursor = request.args.get('after', '')
        limit = min(int(request.args.get('limit', 50)), 200)

        statuses = [s.strip() for s in status_filter.split(',')]

        # Usa requests direto para controlar paginação via cursor
        url = f"https://graph.facebook.com/v22.0/{account_id}/campaigns"
        params = {
            'access_token': access_token,
            'fields': 'id,name,objective,effective_status',
            'filtering': json.dumps([{'field': 'effective_status', 'operator': 'IN', 'value': statuses}]),
            'limit': limit,
        }
        if after_cursor:
            params['after'] = after_cursor

        resp = req_lib.get(url, params=params, timeout=30)
        body = resp.json()

        campanhas = []
        for c in body.get('data', []):
            campanhas.append({
                'id': c.get('id'),
                'name': c.get('name'),
                'objective': c.get('objective'),
                'status': c.get('effective_status', 'UNKNOWN')
            })

        # Cursor para próxima página
        paging = body.get('paging', {})
        next_cursor = paging.get('cursors', {}).get('after', '') if paging.get('next') else ''

        return jsonify({
            'campaigns': campanhas,
            'account_name': session.get('account_name', ''),
            'has_more': bool(next_cursor),
            'after': next_cursor,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/campanha/<campaign_id>/setup', methods=['GET'])
def setup_campanha(campaign_id):
    access_token = obter_token()
    if not access_token:
        return redirect('/')

    try:
        inicializar_api(access_token)
        account_id = session.get('account_id', '')

        campanha = Campaign(campaign_id)
        campanha_info = campanha.api_get(fields=['name'])
        campaign_name = campanha_info.get('name', campaign_id)

        adsets_data = campanha.get_ad_sets(
            fields=['id', 'name', 'effective_status']
        )
        adsets = [{'id': a.get('id'), 'name': a.get('name'), 'status': a.get('effective_status', '')} for a in adsets_data]

        return render_template(
            'setup.html',
            campaign_id=campaign_id,
            campaign_name=campaign_name,
            account_id=account_id,
            adsets=adsets
        )

    except Exception as e:
        err_str = str(e)
        if 'User request limit reached' in err_str or 'error_subcode": 2446079' in err_str or 'code": 17' in err_str:
            return (
                "<div style='font-family:sans-serif;padding:40px;max-width:600px;margin:40px auto;"
                "background:#1a1a2e;color:#e0e0e0;border-radius:12px;border:1px solid #ff6b35;'>"
                "<h2 style='color:#ff6b35'>⚠️ Limite de Requisições da Meta</h2>"
                "<p>A conta de anúncios atingiu o limite temporário de chamadas à API do Facebook.</p>"
                "<p style='color:#aaa;font-size:0.9em'>Isso não é um erro do sistema — é uma proteção automática da Meta.</p>"
                "<p><strong>O que fazer:</strong> Aguarde <strong>15 a 30 minutos</strong> e tente novamente.</p>"
                f"<a href='/campanha/{campaign_id}/setup' style='display:inline-block;margin-top:16px;"
                "padding:10px 20px;background:#ff6b35;color:#fff;border-radius:8px;text-decoration:none;'>"
                "🔄 Tentar novamente</a>&nbsp;&nbsp;"
                "<a href='/' style='display:inline-block;margin-top:16px;padding:10px 20px;"
                "background:#333;color:#fff;border-radius:8px;text-decoration:none;'>← Voltar</a>"
                "</div>"
            )
        return f"Erro ao carregar setup: {e}<br><a href='/'>Voltar</a>"

# ======================== API: IDENTITY & TRACKING ========================

@app.route('/api/conta/<account_id>/identity')
def api_identity(account_id):
    """Endpoint combinado: páginas + instagrams + pixels em 1 chamada."""
    access_token = obter_token()
    if not access_token: return jsonify({'error': 'Not authenticated'}), 401
    try:
        if not account_id.startswith('act_'): account_id = f'act_{account_id}'
        inicializar_api(access_token)
        
        uploader = MetaUploader(account_id, access_token, APP_ID, APP_SECRET)
        data = uploader.get_identity_data()
        return jsonify(data)
    except Exception as e:
        print(f"❌ Error fetching identity: {e}")
        return jsonify({'error': str(e)}), 500

# ======================== API: SAVED ASSETS / PRESETS ========================

@app.route('/api/conta/<account_id>/saved-assets')
def api_saved_assets(account_id):
    """Retorna saved_assets para o setup pré-preencher."""
    from modules.account_settings import get_settings_for_setup
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({}), 401
    return jsonify(get_settings_for_setup(user_id, account_id))


@app.route('/api/conta/<account_id>/save-asset', methods=['POST'])
def api_save_asset(account_id):
    """Salva/favorita um asset individual."""
    from modules.account_settings import save_single_asset
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Não autenticado'}), 401
    data = request.get_json() or {}
    ok = save_single_asset(
        user_id, account_id,
        asset_type=data.get('asset_type'),
        key_field=data.get('key_field'),
        value=data.get('value'),
        extra=data.get('extra')
    )
    return jsonify({'success': ok})


@app.route('/api/conta/<account_id>/remove-asset', methods=['POST'])
def api_remove_asset(account_id):
    """Remove um asset dos salvos."""
    from modules.account_settings import remove_single_asset
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Não autenticado'}), 401
    data = request.get_json() or {}
    ok = remove_single_asset(
        user_id, account_id,
        asset_type=data.get('asset_type'),
        key_field=data.get('key_field'),
        value=data.get('value')
    )
    return jsonify({'success': ok})



@app.route('/api/conta/<account_id>/save-cac', methods=['POST'])
def api_save_cac(account_id):
    """Salva o CAC ideal da conta."""
    from modules.account_settings import save_cac_target
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Não autenticado'}), 401
    data = request.get_json() or {}
    cac = data.get('cac_target_value')
    ok = save_cac_target(user_id, account_id, float(cac) if cac else None)
    return jsonify({'success': ok})


# ======================== GOOGLE ADS INTEGRATION ========================

@app.route('/api/google-ads/status')
def api_google_ads_status():
    """Retorna status da integração Google Ads para o frontend."""
    from modules.google_ads import is_google_ads_configured, get_google_ads_config_from_db
    user_id = session.get('user_id')
    account_id = session.get('account_id', '')
    configured = is_google_ads_configured()
    connected = False
    customer_id = None
    if configured and user_id:
        ga_config = get_google_ads_config_from_db(user_id, account_id)
        if ga_config and ga_config.get('refresh_token'):
            connected = True
            customer_id = ga_config.get('customer_id')
    return jsonify({
        'configured': configured,
        'connected': connected,
        'customer_id': customer_id,
    })


@app.route('/auth/google-ads')
def google_ads_auth():
    """Redireciona para consent screen do Google para conectar Google Ads."""
    from modules.google_ads import get_google_ads_auth_url, is_google_ads_configured
    if not is_google_ads_configured():
        return jsonify({'error': 'Google Ads não configurado no servidor'}), 400
    # Guarda de onde veio (opcional): /auth/google-ads?from=test → volta para /google-ads/test
    session['ga_return_to'] = request.args.get('from', '')
    url = get_google_ads_auth_url(state=session.get('account_id', ''))
    if not url:
        return jsonify({'error': 'Erro ao gerar URL de autenticação'}), 500
    return redirect(url)


@app.route('/callback/google-ads')
def google_ads_callback():
    """Callback OAuth do Google Ads — troca code por tokens e salva."""
    from modules.google_ads import (
        exchange_google_ads_code, list_accessible_customers,
        save_google_ads_config
    )

    code = request.args.get('code')
    error = request.args.get('error')
    state_account_id = request.args.get('state', '')

    if error:
        return f"Erro na autorização Google: {error}", 400
    if not code:
        return "Código de autorização não recebido", 400

    tokens = exchange_google_ads_code(code)
    if not tokens:
        return "Erro ao trocar código por tokens", 500
    if tokens.get('error'):
        return (f"<h2>Erro ao trocar código por tokens</h2>"
                f"<p><b>Status HTTP:</b> {tokens.get('status')}</p>"
                f"<p><b>Redirect URI enviado:</b> {tokens.get('redirect_uri_usado')}</p>"
                f"<p><b>Resposta do Google:</b></p>"
                f"<pre style='background:#f4f4f4;padding:12px;border-radius:6px;'>"
                f"{tokens.get('body')}</pre>"
                f"<p><a href='/cruzamento'>← voltar</a></p>"), 500

    # Listar contas acessíveis para auto-selecionar
    customers = list_accessible_customers(tokens['access_token'])

    user_id = session.get('user_id')
    account_id = state_account_id or session.get('account_id', '')

    if not user_id:
        return "Sessão expirada — faça login novamente", 401

    # Salvar config com primeiro customer (usuário pode trocar depois)
    config = {
        'access_token':  tokens['access_token'],
        'refresh_token': tokens['refresh_token'],
        'expires_at':    tokens['expires_at'],
        'customer_id':   customers[0] if customers else '',
        'all_customers': customers,
        'connected_at':  datetime.now().isoformat(),
    }
    save_google_ads_config(user_id, account_id, config)

    # Redirecionar conforme origem (test page, profile ou cruzamento)
    return_to = session.pop('ga_return_to', '') or ''
    if return_to == 'test':
        return redirect('/google-ads/test')
    if return_to == 'profile':
        return redirect('/auth/account/profile')
    return redirect('/cruzamento')


@app.route('/api/google-ads/disconnect', methods=['POST'])
def api_google_ads_disconnect():
    """Desconecta Google Ads (remove tokens do banco)."""
    from modules.google_ads import save_google_ads_config
    user_id = session.get('user_id')
    account_id = session.get('account_id', '')
    if not user_id:
        return jsonify({'error': 'Não autenticado'}), 401
    save_google_ads_config(user_id, account_id, {})
    return jsonify({'success': True})


@app.route('/api/google-ads/select-customer', methods=['POST'])
def api_google_ads_select_customer():
    """Seleciona qual customer ID (conta Google Ads) usar."""
    from modules.google_ads import get_google_ads_config_from_db, save_google_ads_config
    user_id = session.get('user_id')
    account_id = session.get('account_id', '')
    if not user_id:
        return jsonify({'error': 'Não autenticado'}), 401
    data = request.get_json() or {}
    customer_id = data.get('customer_id', '')
    if not customer_id:
        return jsonify({'error': 'customer_id obrigatório'}), 400
    ga_config = get_google_ads_config_from_db(user_id, account_id)
    if not ga_config:
        return jsonify({'error': 'Google Ads não conectado'}), 400
    ga_config['customer_id'] = customer_id
    save_google_ads_config(user_id, account_id, ga_config)
    return jsonify({'success': True, 'customer_id': customer_id})


@app.route('/google-ads/test')
def google_ads_test_page():
    """Página de teste da integração Google Ads — lista contas e campanhas."""
    user_id = session.get('user_id')
    if not user_id:
        return redirect('/login')
    return render_template('google_ads_test.html')


@app.route('/api/google-ads/customers')
def api_google_ads_customers():
    """Retorna a lista de customer IDs acessíveis + o selecionado atualmente."""
    from modules.google_ads import (
        get_google_ads_config_from_db, _get_valid_token,
        list_accessible_customers, save_google_ads_config
    )
    user_id = session.get('user_id')
    account_id = session.get('account_id', '')
    if not user_id:
        return jsonify({'error': 'Não autenticado'}), 401

    ga_config = get_google_ads_config_from_db(user_id, account_id)
    if not ga_config:
        return jsonify({'error': 'Google Ads não conectado'}), 400

    access_token, updated_config = _get_valid_token(ga_config)
    if not access_token:
        return jsonify({'error': 'Token Google Ads inválido — reconecte'}), 400

    # Se token foi renovado, salva no banco
    if updated_config and updated_config != ga_config:
        updated_config['customer_id'] = ga_config.get('customer_id', '')
        updated_config['all_customers'] = ga_config.get('all_customers', [])
        save_google_ads_config(user_id, account_id, updated_config)
        ga_config = updated_config

    customers = list_accessible_customers(access_token)
    return jsonify({
        'customers':    customers,
        'selected':     ga_config.get('customer_id', ''),
        'login_cid':    os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID', ''),
    })


@app.route('/api/google-ads/campaigns')
def api_google_ads_campaigns():
    """Lista as campanhas do customer_id selecionado (ou do passado via ?customer_id=...)."""
    from modules.google_ads import (
        get_google_ads_config_from_db, _get_valid_token,
        fetch_google_ads_campaigns, save_google_ads_config
    )
    user_id = session.get('user_id')
    account_id = session.get('account_id', '')
    if not user_id:
        return jsonify({'error': 'Não autenticado'}), 401

    ga_config = get_google_ads_config_from_db(user_id, account_id)
    if not ga_config:
        return jsonify({'error': 'Google Ads não conectado'}), 400

    access_token, updated_config = _get_valid_token(ga_config)
    if not access_token:
        return jsonify({'error': 'Token inválido — reconecte'}), 400

    if updated_config and updated_config != ga_config:
        updated_config['customer_id'] = ga_config.get('customer_id', '')
        updated_config['all_customers'] = ga_config.get('all_customers', [])
        save_google_ads_config(user_id, account_id, updated_config)
        ga_config = updated_config

    customer_id = request.args.get('customer_id') or ga_config.get('customer_id', '')
    if not customer_id:
        return jsonify({'error': 'Nenhum customer_id selecionado'}), 400

    campaigns = fetch_google_ads_campaigns(access_token, customer_id)
    return jsonify({'customer_id': customer_id, 'campaigns': campaigns, 'count': len(campaigns)})


# ======================== GOOGLE ADS — SHEETS FALLBACK ========================

def _fetch_vinci_sheet(since_dt=None, until_dt=None):
    """
    Busca e agrega dados do Google Ads (planilha pública VINCI).
    Retorna dict: {'campaigns': [...], 'totals': {'spend','conversions','clicks'}, 'error': str|None}
    Função reutilizada pelos endpoints de Google Ads Sheets e Consolidado.
    """
    import requests as _req
    import csv
    import io

    SHEET_ID  = os.getenv('GOOGLE_ADS_SHEET_ID', '1vhctrrIBQujABaD0VROW8dNHuZIqA-MIX8ESZC77tLg')
    SHEET_GID = os.getenv('GOOGLE_ADS_SHEET_GID', '2054617579')
    FILTER_KW = 'VINCI'

    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&gid={SHEET_GID}'
    try:
        resp = _req.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        return {'campaigns': [], 'totals': {'spend': 0.0, 'conversions': 0.0, 'clicks': 0},
                'error': f'Erro ao buscar Sheets: {str(e)}'}

    def parse_num(s):
        if not s:
            return 0.0
        s = s.strip().replace('%', '')
        if ',' in s and '.' in s:
            s = s.replace('.', '').replace(',', '.')
        elif ',' in s:
            s = s.replace(',', '.')
        try:
            return float(s)
        except ValueError:
            return 0.0

    reader = csv.DictReader(io.StringIO(resp.text))
    campaigns = {}
    headers_map = {}

    for row in reader:
        if not headers_map:
            for k in row.keys():
                kl = k.lower()
                if 'date' in kl or 'data' in kl:
                    headers_map['date'] = k
                elif 'campaign' in kl or 'campanha' in kl:
                    headers_map['campaign'] = k
                elif 'cost' in kl or 'custo' in kl or 'spend' in kl or 'gasto' in kl:
                    headers_map['cost'] = k
                elif 'conversion' in kl or 'conversao' in kl or 'conversão' in kl:
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
        if FILTER_KW.upper() not in name.upper():
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

        spend       = parse_num(row.get(cost_col, '0'))
        conversions = parse_num(row.get(conv_col, '0'))
        clicks      = int(parse_num(row.get(clicks_col, '0')))
        ctr_raw     = parse_num(row.get(ctr_col, '0'))

        if name not in campaigns:
            campaigns[name] = {'campaign_name': name, 'spend': 0.0, 'conversions': 0.0,
                               'clicks': 0, 'ctr_sum': 0.0, 'days': 0}
        campaigns[name]['spend']       += spend
        campaigns[name]['conversions'] += conversions
        campaigns[name]['clicks']      += clicks
        campaigns[name]['ctr_sum']     += ctr_raw
        campaigns[name]['days']        += 1

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


@app.route('/api/cruzamento/google-ads-sheets')
@login_required
def api_google_ads_sheets():
    """
    Busca dados do Google Ads via Google Sheets público (fallback sem API OAuth).
    Filtra campanhas que contenham 'VINCI' no nome.
    Query params: since=YYYY-MM-DD, until=YYYY-MM-DD
    """
    from datetime import datetime

    since_str = request.args.get('since', '')
    until_str = request.args.get('until', '')
    try:
        since_dt = datetime.strptime(since_str, '%Y-%m-%d').date() if since_str else None
        until_dt = datetime.strptime(until_str, '%Y-%m-%d').date() if until_str else None
    except ValueError:
        since_dt = until_dt = None

    data = _fetch_vinci_sheet(since_dt, until_dt)
    if data.get('error'):
        return jsonify({'error': data['error']}), 500
    return jsonify({'campaigns': data['campaigns'], 'count': len(data['campaigns']), 'filter': 'VINCI'})


@app.route('/api/cruzamento/consolidado')
@login_required
def api_cruzamento_consolidado():
    """
    Aba Consolidado: une gastos Facebook Ads + Google Ads (VINCI Sheet) + MQLs/Wons da planilha.

    Regra de atribuição (crítica — evita double counting):
      - Fonte única de MQLs e receita: planilha de MQLs/Wons
      - Segregação por utm_source (normalizado lowercase):
          'facebook'              → Facebook
          'adwords' | 'google'    → Google
          qualquer outro/vazio    → Outros
      - Spend por fonte:
          Facebook: soma de 'spend' dos fb_ads (respeita filtros de campanhas excluídas)
          Google:   soma de 'Cost' na planilha VINCI (apenas campanhas com VINCI no nome)

    Query params: date_preset=last_7_days|this_month|..., since=YYYY-MM-DD, until=YYYY-MM-DD
    """
    import concurrent.futures
    from datetime import datetime as _dt
    from modules.cruzamento import (
        AD_ACCOUNT_ID, SPREADSHEET_ID,
        fetch_fb_insights, fetch_sheets_data,
        filter_rows_by_date, preset_to_dates,
        load_excluded_patterns, _matches_excluded, _is_instagram_post,
        _norm, _parse_valor,
    )

    token = obter_token()
    if not token:
        return jsonify({'success': False, 'error': 'Não autenticado'}), 401

    date_preset = request.args.get('date_preset', 'last_7_days')
    since       = request.args.get('since')
    until       = request.args.get('until')

    since_d, until_d = preset_to_dates(date_preset, since, until)

    # Fetch paralelo — 3 fontes independentes
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            fb_fut     = ex.submit(fetch_fb_insights, AD_ACCOUNT_ID, token, date_preset, since, until)
            sheets_fut = ex.submit(fetch_sheets_data, SPREADSHEET_ID)
            vinci_fut  = ex.submit(_fetch_vinci_sheet, since_d, until_d)

            fb_ads = fb_fut.result()
            mqls_all, wons_all = sheets_fut.result()
            vinci = vinci_fut.result()
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

    # ── 1. Facebook spend (respeita filtro global de campanhas excluídas) ──
    excluded = load_excluded_patterns()
    fb_spend = 0.0
    for ad in fb_ads:
        cn = ad.get('campaign_name', '')
        if _matches_excluded(cn, excluded):
            continue
        if _is_instagram_post(cn):
            continue
        fb_spend += float(ad.get('spend', 0) or 0)

    # ── 2. Google spend (VINCI sheet) ──
    google_spend = float(vinci['totals']['spend'])
    google_conversions_sheet = float(vinci['totals']['conversions'])  # apenas exibição, não é MQL

    # ── 3. MQLs/Wons do período ──
    mqls_rows = filter_rows_by_date(mqls_all, 'Data do preenchimento', since_d, until_d)
    wons_rows = filter_rows_by_date(wons_all, 'Data de fechamento', since_d, until_d)

    # Index Wons por Deal ID (para join com MQL)
    wons_idx = {}
    for row in wons_rows:
        deal_id = _norm(row.get('Deal ID', ''))
        if deal_id:
            wons_idx[deal_id] = {'valor': _parse_valor(row.get('Valor', 0))}

    # Buckets por utm_source
    def _bucket(utm_src):
        s = (utm_src or '').strip().lower()
        if s == 'facebook':
            return 'facebook'
        if s in ('adwords', 'google'):
            return 'google'
        return 'outros'

    def _empty():
        return {'mqls': 0, 'wons': 0, 'receita': 0.0}

    buckets = {'facebook': _empty(), 'google': _empty(), 'outros': _empty()}

    # MQLs do período → contagem por bucket
    for row in mqls_rows:
        b = _bucket(row.get('utm_source', ''))
        buckets[b]['mqls'] += 1

    # Wons do período → atribuição pelo utm_source do MQL original (fonte única de verdade).
    # Se o MQL não estiver na planilha completa (raro), bucket = 'outros'.
    mqls_all_by_deal = {
        _norm(row.get('Deal ID', '')): row
        for row in mqls_all
        if _norm(row.get('Deal ID', ''))
    }
    for row in wons_rows:
        deal_id = _norm(row.get('Deal ID', ''))
        valor = _parse_valor(row.get('Valor', 0))
        mql_row = mqls_all_by_deal.get(deal_id)
        utm_src = mql_row.get('utm_source', '') if mql_row else ''
        b = _bucket(utm_src)
        buckets[b]['wons']    += 1
        buckets[b]['receita'] += valor

    # ── 4. Métricas derivadas ──
    def _metrics(bucket, spend):
        mqls    = bucket['mqls']
        wons    = bucket['wons']
        receita = bucket['receita']
        return {
            'spend':       round(spend, 2),
            'mqls':        mqls,
            'wons':        wons,
            'receita':     round(receita, 2),
            'cpm':         round(spend / mqls, 2)   if mqls > 0   else 0.0,   # custo por MQL
            'cpa':         round(spend / wons, 2)   if wons > 0   else 0.0,   # custo por venda
            'ticket':      round(receita / wons, 2) if wons > 0   else 0.0,
            'conv_rate':   round(100 * wons / mqls, 2) if mqls > 0 else 0.0,
            'roas':        round(receita / spend, 2) if spend > 0 else 0.0,
        }

    # Outros: spend = 0 (não há canal pago rastreável aqui — orgânico, direct, indicação)
    per_source = {
        'facebook': _metrics(buckets['facebook'], fb_spend),
        'google':   _metrics(buckets['google'],   google_spend),
        'outros':   _metrics(buckets['outros'],   0.0),
    }

    total_spend   = fb_spend + google_spend
    total_mqls    = sum(b['mqls'] for b in buckets.values())
    total_wons    = sum(b['wons'] for b in buckets.values())
    total_receita = sum(b['receita'] for b in buckets.values())

    consolidado = {
        'spend':     round(total_spend, 2),
        'mqls':      total_mqls,
        'wons':      total_wons,
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
        'google_conversions_sheet': round(google_conversions_sheet, 2),  # referência (não usado p/ custo/MQL)
        'vinci_error': vinci.get('error'),
    })


# ======================== GOOGLE DRIVE ========================

@app.route('/api/drive/list_folder', methods=['POST'])
def api_drive_list_folder():
    """Lista arquivos de uma pasta do Drive e faz o pareamento automático."""
    from meta_api import list_drive_folder
    data = request.json
    folder_url = data.get('url')
    if not folder_url:
        return jsonify({'error': 'URL da pasta não fornecida'}), 400

    res = list_drive_folder(folder_url)
    if 'error' in res:
        return jsonify({'error': res['error']}), 500

    files = res.get('files', [])
    
    # Lógica de Pareamento
    # Arquivos: "Anuncio1_FEED.mp4", "Anuncio1_REELS.mp4"
    pares = {} # { prefixo: {feed_url, reels_url, nome} }
    
    for f in files:
        name = f['name']
        f_id = f['id']
        direct_url = f"https://drive.google.com/uc?export=download&id={f_id}"
        
        # Tenta identificar sufixos
        clean_name = os.path.splitext(name)[0].upper()
        
        if '_FEED' in clean_name:
            prefix = clean_name.split('_FEED')[0]
            if prefix not in pares: pares[prefix] = {'nome': prefix}
            pares[prefix]['feed_url'] = direct_url
            pares[prefix]['feed_name'] = name
        elif '_REELS' in clean_name:
            prefix = clean_name.split('_REELS')[0]
            if prefix not in pares: pares[prefix] = {'nome': prefix}
            pares[prefix]['reels_url'] = direct_url
            pares[prefix]['reels_name'] = name
        elif '_STORIES' in clean_name: # Fallback para stories
            prefix = clean_name.split('_STORIES')[0]
            if prefix not in pares: pares[prefix] = {'nome': prefix}
            pares[prefix]['reels_url'] = direct_url
            pares[prefix]['reels_name'] = name

    # Converter para lista e filtrar apenas os que tem pelo menos um dos dois
    resultado = [v for v in pares.values() if 'feed_url' in v or 'reels_url' in v]
    
    return jsonify({'pares': resultado})

@app.route('/api/pagina/<page_id>/leadgen_forms')
def api_leadgen_forms(page_id):
    """Busca formulários de lead de uma página."""
    access_token = obter_token()
    if not access_token: return jsonify({'error': 'Not authenticated'}), 401
    try:
        account_id = session.get('account_id', '')
        uploader = MetaUploader(account_id, access_token, APP_ID, APP_SECRET)
        forms = uploader.get_leadgen_forms(page_id)
        return jsonify({'forms': forms})
    except Exception as e:
        print(f"❌ Error fetching lead forms: {e}")
        return jsonify({'error': str(e)}), 500

# ======================== API: HISTÓRICO ========================

@app.route('/api/campanha/<campaign_id>/historico_textos')
def historico_textos(campaign_id):
    """Busca criativos recentes da campanha e retorna URLs, UTMs, textos e títulos únicos."""
    access_token = obter_token()
    if not access_token:
        return jsonify({'error': 'Não autenticado'}), 401

    try:
        inicializar_api(access_token)

        campanha = Campaign(campaign_id)
        ads = campanha.get_ads(
            fields=['creative{effective_object_story_spec,url_tags,name,body,title,link_url,asset_feed_spec}'],
            params={'limit': 50}
        )

        urls, utms, textos, titulos = set(), set(), set(), set()

        for ad in ads:
            creative = ad.get('creative', {})

            link_url = creative.get('link_url')
            if link_url:
                if '?' in link_url:
                    base_url, query = link_url.split('?', 1)
                    urls.add(base_url)
                    utms.add(query)
                else:
                    urls.add(link_url)

            url_tags = creative.get('url_tags')
            if url_tags:
                utms.add(url_tags)

            body = creative.get('body')
            if body:
                textos.add(body)

            title = creative.get('title')
            if title:
                titulos.add(title)

            asset_feed = creative.get('asset_feed_spec', {})
            if asset_feed:
                for b in asset_feed.get('bodies', []):
                    if b.get('text'):
                        textos.add(b['text'])
                for t in asset_feed.get('titles', []):
                    if t.get('text'):
                        titulos.add(t['text'])
                for lu in asset_feed.get('link_urls', []):
                    if lu.get('website_url'):
                        u = lu['website_url']
                        if '?' in u:
                            base, q = u.split('?', 1)
                            urls.add(base)
                            utms.add(q)
                        else:
                            urls.add(u)

            story_spec = creative.get('effective_object_story_spec', {})
            if story_spec:
                ld = story_spec.get('link_data', {})
                if ld:
                    if ld.get('link'):
                        lk = ld['link']
                        if '?' in lk:
                            base, q = lk.split('?', 1)
                            urls.add(base)
                            utms.add(q)
                        else:
                            urls.add(lk)
                    if ld.get('message'):
                        textos.add(ld['message'])
                    if ld.get('name'):
                        titulos.add(ld['name'])

                vd = story_spec.get('video_data', {})
                if vd:
                    if vd.get('message'):
                        textos.add(vd['message'])
                    if vd.get('title'):
                        titulos.add(vd['title'])

        return jsonify({
            'urls': sorted(urls),
            'utms': sorted(utms),
            'textos': sorted(textos),
            'titulos': sorted(titulos),
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ======================== UPLOAD: POR ITEM ========================

@app.route('/campanha/<campaign_id>/upload', methods=['POST'])
def upload_single(campaign_id):
    """Upload de um único anúncio via SSE stream. Cada etapa emite um evento de progresso."""
    access_token = obter_token()
    if not access_token:
        return jsonify({'success': False, 'error': 'Não autenticado'}), 401

    # Extrair todos os dados do request ANTES da thread (contexto de request não é thread-safe)
    account_id = session.get('account_id', '')
    user_id = session.get('user_id')
    page_id = request.form.get('page_id')
    instagram_actor_id = request.form.get('instagram_actor_id') or None
    pixel_id = request.form.get('pixel_id') or None
    estrategia = request.form.get('estrategia')
    destino = request.form.get('destino_conjunto', '')
    adset_existente = request.form.get('adset_existente', '')
    adset_modelo = request.form.get('adset_modelo', '')
    ad_name = request.form.get('ad_name', 'Anúncio sem nome')
    ad_status = request.form.get('ad_status', 'PAUSED').upper()
    if ad_status not in ('ACTIVE', 'PAUSED'):
        ad_status = 'PAUSED'
    url_destino = request.form.get('url_destino', '')
    utm_pattern = request.form.get('utm_pattern', '')
    cta = request.form.get('cta', 'LEARN_MORE')
    textos = request.form.getlist('primary_text[]')
    titulos = request.form.getlist('headline[]')
    lead_gen_form_id = request.form.get('lead_gen_form_id') or None
    url_feed_remote = request.form.get('url_feed_remote') or None
    url_stories_remote = request.form.get('url_stories_remote') or None
    # Países a excluir da segmentação (geo-compliance bypass)
    excluded_countries_raw = request.form.get('excluded_countries', '')
    excluded_countries = [c.strip().upper() for c in excluded_countries_raw.split(',') if c.strip()] or None

    # Salvar arquivos locais em temp dir antes da thread
    temp_dir = tempfile.mkdtemp(prefix='optimizer_')
    feed_path = None
    stories_path = None

    if 'arquivo_feed' in request.files:
        f = request.files['arquivo_feed']
        if f.filename:
            safe_name = os.path.basename(f.filename)
            feed_path = os.path.join(temp_dir, safe_name)
            f.save(feed_path)
            print(f"📁 [UPLOAD LOCAL] Feed salvo: {feed_path} ({os.path.getsize(feed_path) / 1024:.0f} KB)")

    if 'arquivo_stories' in request.files:
        f = request.files['arquivo_stories']
        if f.filename:
            safe_name = os.path.basename(f.filename)
            stories_path = os.path.join(temp_dir, safe_name)
            f.save(stories_path)
            print(f"📁 [UPLOAD LOCAL] Stories salvo: {stories_path} ({os.path.getsize(stories_path) / 1024:.0f} KB)")

    if not feed_path and not stories_path and not url_feed_remote and not url_stories_remote:
        return jsonify({'success': False, 'error': 'Nenhuma mídia (arquivo ou link) enviada'}), 400

    msg_queue = queue.Queue()

    def _run_upload():
        uploader = None
        try:
            def _evt(type, **kw):
                msg_queue.put({'type': type, **kw})

            uploader = MetaUploader(account_id, access_token, APP_ID, APP_SECRET)
            uploader.set_callback(lambda msg: _evt('log', message=msg))

            _evt('progress', percent=10, stage='upload_feed',
                 message='📤 Enviando mídia feed para Meta...')

            feed_media = None
            stories_media = None

            if url_feed_remote:
                feed_media = uploader.upload_media(url=url_feed_remote)
                uploader.smart_delay()
            elif feed_path:
                feed_media = uploader.upload_media(file_path=feed_path)
                uploader.smart_delay()

            has_stories = url_stories_remote or stories_path
            if has_stories:
                _evt('progress', percent=30, stage='upload_stories',
                     message='📤 Enviando mídia stories para Meta...')
                if url_stories_remote:
                    stories_media = uploader.upload_media(url=url_stories_remote)
                    uploader.smart_delay()
                elif stories_path:
                    stories_media = uploader.upload_media(file_path=stories_path)
                    uploader.smart_delay()

            target_adset_id = adset_existente or adset_modelo
            if not target_adset_id:
                raise ValueError('Nenhum Ad Set selecionado')
            if not page_id:
                raise ValueError('Página do Facebook Obrigatória')

            actual_adset_id = target_adset_id
            if destino == 'duplicar' and estrategia == 'agrupado':
                try:
                    actual_adset_id = uploader.duplicate_adset(
                        target_adset_id,
                        excluded_countries=excluded_countries)
                except GeoComplianceError as geo_err:
                    _evt('geo_compliance_error', **{
                        'country_code': geo_err.country_code,
                        'country_name': geo_err.country_name,
                        'message': str(geo_err),
                    })
                    return
                uploader.smart_delay()

            # Aguardar processamento de vídeos
            if feed_media and feed_media.get('type') == 'video' and feed_media.get('id'):
                _evt('progress', percent=45, stage='meta_processing',
                     message='⏳ Meta processando vídeo do feed...', polling=True)
                uploader.wait_for_video_ready(feed_media['id'])
                _evt('progress', percent=60, stage='meta_processing_done',
                     message='✅ Vídeo feed pronto.')

            if stories_media and stories_media.get('type') == 'video' and stories_media.get('id'):
                _evt('progress', percent=65, stage='meta_processing_stories',
                     message='⏳ Meta processando vídeo stories...', polling=True)
                uploader.wait_for_video_ready(stories_media['id'])
                _evt('progress', percent=75, stage='meta_processing_stories_done',
                     message='✅ Vídeo stories pronto.')

            _evt('progress', percent=82, stage='create_creative',
                 message='🎨 Criando criativo...')
            creative_id = uploader.create_creative_with_placements(
                page_id=page_id,
                feed_media=feed_media,
                stories_media=stories_media,
                link_url=url_destino,
                primary_texts=textos,
                headlines=titulos,
                cta_type=cta,
                instagram_user_id=instagram_actor_id,
                url_tags=utm_pattern,
                lead_gen_form_id=lead_gen_form_id,
            )
            uploader.smart_delay()

            _evt('progress', percent=92, stage='create_ad',
                 message='📢 Criando anúncio...')
            ad_id = uploader.create_ad(actual_adset_id, creative_id, ad_name,
                                       pixel_id=pixel_id, ad_status=ad_status)

            # Cleanup temp
            try:
                if feed_path and os.path.exists(feed_path):
                    os.remove(feed_path)
                if stories_path and os.path.exists(stories_path):
                    os.remove(stories_path)
                os.rmdir(temp_dir)
            except OSError:
                pass

            # Squad 2 — salvar assets
            if user_id:
                save_upload_assets(user_id, account_id, {
                    'page_id': page_id,
                    'instagram_id': instagram_actor_id,
                    'pixel_id': pixel_id,
                    'primary_texts': textos,
                    'headlines': titulos,
                    'url': url_destino,
                    'utm': utm_pattern,
                    'cta': cta,
                })
                save_upload_history(
                    user_id, account_id,
                    campaign_name=campaign_id,
                    ad_name=ad_name,
                    strategy=estrategia,
                    success=True
                )

            _evt('done', percent=100, success=True, ad_id=ad_id,
                 message=f'✅ Anúncio "{ad_name}" criado com sucesso.',
                 logs=uploader.logs)

        except Exception as e:
            import traceback
            traceback.print_exc()
            try:
                if user_id and account_id:
                    save_upload_history(
                        user_id, account_id,
                        campaign_name=campaign_id,
                        ad_name=ad_name,
                        strategy=estrategia,
                        success=False,
                        error_message=str(e)[:500]
                    )
            except Exception:
                pass
            msg_queue.put({'type': 'error', 'message': str(e),
                           'logs': uploader.logs if uploader else []})
        finally:
            msg_queue.put(None)  # sentinel

    thread = threading.Thread(target=_run_upload, daemon=True)

    def _generate():
        thread.start()
        yield f"data: {json.dumps({'type': 'progress', 'percent': 5, 'stage': 'start', 'message': '🚀 Iniciando...'})}\n\n"
        while True:
            try:
                item = msg_queue.get(timeout=180)
            except queue.Empty:
                yield ": keepalive\n\n"
                continue
            if item is None:
                break
            yield f"data: {json.dumps(item)}\n\n"
            if item.get('type') in ('done', 'error'):
                break

    return Response(
        stream_with_context(_generate()),
        mimetype='text/event-stream',
        headers={
            'X-Accel-Buffering': 'no',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
        }
    )


@app.route('/campanha/<campaign_id>/duplicate-adset', methods=['POST'])
def duplicate_adset_route(campaign_id):
    """Duplica um Ad Set 1x. Retorna o ID do novo. Chamado ANTES do loop de criativos."""
    access_token = obter_token()
    if not access_token:
        return jsonify({'success': False, 'error': 'Não autenticado'}), 401

    try:
        account_id = session.get('account_id', '')
        adset_modelo = request.form.get('adset_modelo', '')
        adset_name = request.form.get('adset_name', '')
        adset_status = request.form.get('adset_status', 'PAUSED').upper()
        if adset_status not in ('ACTIVE', 'PAUSED'):
            adset_status = 'PAUSED'
        excluded_countries_raw = request.form.get('excluded_countries', '')
        excluded_countries = [c.strip().upper() for c in excluded_countries_raw.split(',') if c.strip()] or None

        if not adset_modelo:
            return jsonify({'success': False, 'error': 'Nenhum Ad Set modelo informado'}), 400

        uploader = MetaUploader(account_id, access_token, APP_ID, APP_SECRET)
        try:
            new_adset_id = uploader.duplicate_adset(
                adset_modelo, new_name=adset_name or None,
                adset_status=adset_status,
                excluded_countries=excluded_countries)
        except GeoComplianceError as geo_err:
            return jsonify({
                'success': False,
                'compliance_error': True,
                'country_code': geo_err.country_code,
                'country_name': geo_err.country_name,
                'error': str(geo_err),
                'logs': uploader.logs,
            }), 200  # 200 para o frontend processar o JSON normalamente

        return jsonify({
            'success': True,
            'adset_id': new_adset_id,
            'logs': uploader.logs,
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500



@app.route('/ping')
def ping_vps():
    return "🚀 BATEU NA VPS! O Docker novo está rodando o nosso código atualizado e a página subiu!!", 200

if __name__ == '__main__':
    print("Servidor rodando! Acesse http://localhost:5000 no seu navegador.")
    app.run(port=5000)
