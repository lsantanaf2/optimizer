import os
import json
import time
import tempfile
from flask import (
    Flask, request, redirect, session, render_template,
    jsonify, Response, stream_with_context, url_for
)
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.business import Business
from meta_api import MetaUploader

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
app.secret_key = 'chave-secreta-optimizer-2024'

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

VERSION = "v2.5.1"

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
    """API: lista campanhas ATIVO+PAUSADO da conta (chamada pelo frontend via AJAX)."""
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

        conta = AdAccount(account_id)
        campanhas_data = conta.get_campaigns(
            fields=['id', 'name', 'objective', 'effective_status'],
            params={'filtering': [{'field': 'effective_status', 'operator': 'IN', 'value': ['ACTIVE', 'PAUSED']}]}
        )
        campanhas = []
        for c in campanhas_data:
            campanhas.append({
                'id': c.get('id'),
                'name': c.get('name'),
                'objective': c.get('objective'),
                'status': c.get('effective_status', 'UNKNOWN')
            })
        return jsonify({'campaigns': campanhas, 'account_name': session.get('account_name', '')})
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
    """Upload de um único anúncio. Chamado pelo frontend para cada item da fila."""
    access_token = obter_token()
    if not access_token:
        return jsonify({'success': False, 'error': 'Não autenticado'}), 401

    uploader = None
    try:
        account_id = session.get('account_id', '')
        # Identity & Tracking from form
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

        # New: Links from Drive/Remote
        url_feed_remote = request.form.get('url_feed_remote') or None
        url_stories_remote = request.form.get('url_stories_remote') or None

        # Save uploaded files to temp dir
        temp_dir = tempfile.mkdtemp(prefix='optimizer_')
        feed_path = None
        stories_path = None

        if 'arquivo_feed' in request.files:
            f = request.files['arquivo_feed']
            if f.filename:
                safe_name = os.path.basename(f.filename)  # webkitdirectory pode enviar path relativo
                feed_path = os.path.join(temp_dir, safe_name)
                f.save(feed_path)
                saved_size = os.path.getsize(feed_path)
                print(f"📁 [UPLOAD LOCAL] Feed salvo: {feed_path} ({saved_size / 1024:.0f} KB)")

        if 'arquivo_stories' in request.files:
            f = request.files['arquivo_stories']
            if f.filename:
                safe_name = os.path.basename(f.filename)  # webkitdirectory pode enviar path relativo
                stories_path = os.path.join(temp_dir, safe_name)
                f.save(stories_path)
                saved_size = os.path.getsize(stories_path)
                print(f"📁 [UPLOAD LOCAL] Stories salvo: {stories_path} ({saved_size / 1024:.0f} KB)")

        if not feed_path and not stories_path and not url_feed_remote and not url_stories_remote:
            return jsonify({'success': False, 'error': 'Nenhuma mídia (arquivo ou link) enviada'}), 400

        print(f"📦 [UPLOAD LOCAL] Resumo: feed_path={feed_path}, stories_path={stories_path}, url_feed={url_feed_remote}, url_stories={url_stories_remote}")

        # Initialize uploader
        uploader = MetaUploader(account_id, access_token, APP_ID, APP_SECRET)

        # Upload media files
        feed_media = None
        stories_media = None

        # Case 1: Prioritize Remote URLs (Drive)
        if url_feed_remote:
            feed_media = uploader.upload_media(url=url_feed_remote)
            uploader.smart_delay()
        elif feed_path:
            feed_media = uploader.upload_media(file_path=feed_path)
            uploader.smart_delay()

        if url_stories_remote:
            stories_media = uploader.upload_media(url=url_stories_remote)
            uploader.smart_delay()
        elif stories_path:
            stories_media = uploader.upload_media(file_path=stories_path)
            uploader.smart_delay()

        # Determine target adset
        target_adset_id = adset_existente or adset_modelo
        if not target_adset_id:
            return jsonify({'success': False, 'error': 'Nenhum Ad Set selecionado'}), 400

        # Validation for Identity
        if not page_id:
             return jsonify({'success': False, 'error': 'Página do Facebook Obrigatória'}), 400

        # Handle strategy: duplicate adset if needed
        # NOTA: Garimpo e Novo duplicam no FRONTEND (subirTodos), não aqui.
        actual_adset_id = target_adset_id
        if destino == 'duplicar' and estrategia == 'agrupado':
            actual_adset_id = uploader.duplicate_adset(target_adset_id)
            uploader.smart_delay()

        # Build full URL (UTMs vão apenas no url_tags, não na URL do link)
        full_url = url_destino

        # Aguardar processamento dos vídeos ANTES de criar o creative
        if feed_media and feed_media.get('type') == 'video' and feed_media.get('id'):
            uploader.wait_for_video_ready(feed_media['id'])
        if stories_media and stories_media.get('type') == 'video' and stories_media.get('id'):
            uploader.wait_for_video_ready(stories_media['id'])

        # Create creative with placement rules
        try:
            creative_id = uploader.create_creative_with_placements(
                page_id=page_id,
                feed_media=feed_media,
                stories_media=stories_media,
                link_url=full_url,
                primary_texts=textos,
                headlines=titulos,
                cta_type=cta,
                instagram_user_id=instagram_actor_id,
                url_tags=utm_pattern,
                lead_gen_form_id=lead_gen_form_id,
            )
        except ValueError as e:
             return jsonify({'success': False, 'error': str(e)}), 400

        uploader.smart_delay()

        # Create ad com status configurado pelo usuário
        ad_id = uploader.create_ad(actual_adset_id, creative_id, ad_name, pixel_id=pixel_id, ad_status=ad_status)

        # Cleanup temp files
        try:
            if feed_path and os.path.exists(feed_path):
                os.remove(feed_path)
            if stories_path and os.path.exists(stories_path):
                os.remove(stories_path)
            os.rmdir(temp_dir)
        except OSError:
            pass

        # Squad 2 — salvar assets usados para pré-preencher próximos uploads
        user_id = session.get('user_id')
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
            # Squad 3 — histórico de uploads
            save_upload_history(
                user_id, account_id,
                campaign_name=campaign_id,
                ad_name=ad_name,
                strategy=estrategia,
                success=True
            )

        return jsonify({
            'success': True,
            'ad_id': ad_id,
            'message': f'Ad "{ad_name}" criado com sucesso ({ad_status})',
            'logs': uploader.logs,
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        # Squad 3 — registrar falha no histórico
        try:
            user_id = session.get('user_id')
            account_id = session.get('account_id', '')
            ad_name_err = request.form.get('ad_name', 'Desconhecido')
            if user_id and account_id:
                save_upload_history(
                    user_id, account_id,
                    campaign_name=campaign_id,
                    ad_name=ad_name_err,
                    strategy=request.form.get('estrategia'),
                    success=False,
                    error_message=str(e)[:500]
                )
        except Exception:
            pass
        return jsonify({
            'success': False,
            'error': str(e),
            'logs': uploader.logs if uploader else [],
        }), 500


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

        if not adset_modelo:
            return jsonify({'success': False, 'error': 'Nenhum Ad Set modelo informado'}), 400

        uploader = MetaUploader(account_id, access_token, APP_ID, APP_SECRET)
        new_adset_id = uploader.duplicate_adset(adset_modelo, new_name=adset_name or None, adset_status=adset_status)

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
