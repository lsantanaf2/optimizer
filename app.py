import os
import json
import time
import tempfile
import requests
from flask import (
    Flask, request, redirect, session, render_template,
    jsonify, Response, stream_with_context, url_for
)
import queue
import threading
from urllib.parse import quote
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

app = Flask(__name__, static_folder='static')
app.secret_key = 'chave-secreta-optimizer-2024'

VERSION = "v1.6.5"





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

@app.route('/')
def index():
    token = obter_token()
    if token:
        try:
            inicializar_api(token)
            return listar_contas()
        except Exception:
            limpar_token()
    return pagina_login()

def pagina_login():
    # Scopes: Basic Hygiene Package
    scopes = 'public_profile,email,ads_read,ads_management,pages_show_list,pages_read_engagement,instagram_basic,read_insights,pages_manage_ads'
    encoded_uri = quote(REDIRECT_URI)
    auth_url = (
        f"https://www.facebook.com/v22.0/dialog/oauth?"
        f"client_id={APP_ID}&redirect_uri={encoded_uri}&scope={scopes}"
    )
    print(f"DEBUG: Generating login page. REDIRECT_URI={REDIRECT_URI}")
    return render_template('login.html', auth_url=auth_url)

@app.route('/callback')
def callback():
    print(f"DEBUG: Callback received. request.url={request.url}")
    print(f"DEBUG: Using REDIRECT_URI={REDIRECT_URI}")
    
    code = request.args.get('code')
    if not code:
        return "Erro: Código de autorização não recebido."

    encoded_uri = quote(REDIRECT_URI)
    token_url = (
        f"https://graph.facebook.com/v22.0/oauth/access_token?"
        f"client_id={APP_ID}&redirect_uri={encoded_uri}&"
        f"client_secret={APP_SECRET}&code={code}"
    )

    print(f"DEBUG: Requesting token with URL: {token_url.replace(APP_SECRET or '', 'SECRET_HIDDEN')}")
    response = requests.get(token_url).json()
    access_token = response.get('access_token')

    if not access_token:
        print(f"DEBUG: Token Error: {response}")
        return f"Erro ao obter token: {response}"

    session['access_token'] = access_token
    salvar_token(access_token)
    inicializar_api(access_token)
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    limpar_token()
    return redirect(url_for('index'))

def listar_contas():
    try:
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

        return render_template('accounts.html', accounts=contas)

    except Exception as e:
        print(f"❌ Error listing accounts: {e}")
        limpar_token()
        return redirect(url_for('index'))

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
    if not account_id.startswith('act_'):
        account_id = f"act_{account_id}"
    access_token = obter_token()
    if not access_token:
        return redirect(url_for('index'))

    try:
        inicializar_api(access_token)
        session['account_id'] = account_id

        conta = AdAccount(account_id)
        campanhas_data = conta.get_campaigns(
            fields=['id', 'name', 'objective', 'effective_status'],
            params={'filtering': [{'field': 'effective_status', 'operator': 'IN', 'value': ['ACTIVE', 'PAUSED']}]}
        )

        campanhas = []
        if campanhas_data:
            for c in campanhas_data:
                campanhas.append({
                    'id': c.get('id'),
                    'name': c.get('name'),
                    'objective': c.get('objective'),
                    'status': c.get('effective_status', 'UNKNOWN')
                })

        return render_template('campaigns.html', campaigns=campanhas, account_id=account_id)

    except Exception as e:
        return f"Erro ao listar campanhas: {e}<br><a href='/'>Voltar</a>"

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
    """Upload de um único anúncio com streaming de logs em tempo real (NDJSON)."""
    access_token = obter_token()
    if not access_token:
        return jsonify({'success': False, 'error': 'Não autenticado'}), 401

    # Capturar dados necessários ANTES de entrar na thread
    account_id = session.get('account_id', '')
    form_data = request.form.to_dict(flat=False) # Para pegar listas como primary_text[]
    # request.files não pode ser acessado em thread facilmente, vamos salvar os arquivos antes
    
    # Salvar arquivos em diretório temporário antes da thread
    temp_dir = tempfile.mkdtemp(prefix='optimizer_')
    feed_path = None
    stories_path = None
    
    if 'arquivo_feed' in request.files:
        f = request.files['arquivo_feed']
        if f.filename:
            feed_path = os.path.join(temp_dir, f.filename)
            f.save(feed_path)
            
    if 'arquivo_stories' in request.files:
        f = request.files['arquivo_stories']
        if f.filename:
            stories_path = os.path.join(temp_dir, f.filename)
            f.save(stories_path)

    log_queue = queue.Queue()

    def run_upload_task():
        uploader = None
        try:
            # Reconstituir variáveis do form_data (que agora é um dict de listas)
            def get_f(key, default=None):
                val = form_data.get(key)
                return val[0] if val else default

            page_id = get_f('page_id')
            instagram_actor_id = get_f('instagram_actor_id')
            pixel_id = get_f('pixel_id')
            estrategia = get_f('estrategia')
            destino = get_f('destino_conjunto')
            adset_existente = get_f('adset_existente')
            adset_modelo = get_f('adset_modelo')
            ad_name = get_f('ad_name', 'Anúncio sem nome')
            url_destino = get_f('url_destino', '')
            utm_pattern = get_f('utm_pattern', '')
            cta = get_f('cta', 'LEARN_MORE')
            textos = form_data.get('primary_text[]', [])
            titulos = form_data.get('headline[]', [])
            lead_gen_form_id = get_f('lead_gen_form_id')
            url_feed_remote = get_f('url_feed_remote')
            url_stories_remote = get_f('url_stories_remote')

            if not feed_path and not stories_path and not url_feed_remote and not url_stories_remote:
                log_queue.put({'success': False, 'error': 'Nenhuma mídia enviada'})
                return

            # Inicializar uploader com callback na queue
            uploader = MetaUploader(account_id, access_token, APP_ID, APP_SECRET, 
                                    callback=lambda msg: log_queue.put({'type': 'log', 'msg': msg}))

            # Log de diagnóstico de rede (Architect Audit)
            uploader._log_network_info()

            # Upload mídias
            feed_media = None
            stories_media = None

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

            target_adset_id = adset_existente or adset_modelo
            actual_adset_id = target_adset_id
            
            if estrategia == 'garimpo' or destino == 'duplicar':
                actual_adset_id = uploader.duplicate_adset(target_adset_id)
                uploader.smart_delay()

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

            ad_id = uploader.create_ad(actual_adset_id, creative_id, ad_name, pixel_id=pixel_id)

            # Sucesso
            log_queue.put({
                'success': True, 
                'ad_id': ad_id, 
                'message': f'Ad "{ad_name}" criado com sucesso'
            })

        except Exception as e:
            import traceback
            traceback.print_exc()
            log_queue.put({'success': False, 'error': str(e)})
        finally:
            # Cleanup
            try:
                if feed_path and os.path.exists(feed_path): os.remove(feed_path)
                if stories_path and os.path.exists(stories_path): os.remove(stories_path)
                os.rmdir(temp_dir)
            except: pass
            log_queue.put(None) # Fim da stream

    # Iniciar thread
    threading.Thread(target=run_upload_task).start()

    def generate_ndjson():
        while True:
            item = log_queue.get()
            if item is None: break
            yield json.dumps(item) + '\n'

    return Response(
        stream_with_context(generate_ndjson()), 
        mimetype='application/x-ndjson',
        headers={
            'X-Accel-Buffering': 'no',  # Impede que o Nginx faça buffer do stream
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
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

        if not adset_modelo:
            return jsonify({'success': False, 'error': 'Nenhum Ad Set modelo informado'}), 400

        uploader = MetaUploader(account_id, access_token, APP_ID, APP_SECRET)
        new_adset_id = uploader.duplicate_adset(adset_modelo, new_name=adset_name or None)

        return jsonify({
            'success': True,
            'adset_id': new_adset_id,
            'logs': uploader.logs,
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    print("Servidor rodando! Acesse http://localhost:5000 no seu navegador.")
    app.run(port=5000)
