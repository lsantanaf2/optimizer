"""
MetaUploader — Classe utilitária para upload seguro de anúncios na Meta API.

Features:
  - Delay inteligente (1.5-3s) entre uploads
  - Rate limit monitor via header x-business-use-case-usage
  - Retry automático (até 3x) com backoff
  - Status PAUSED por padrão em todos os Ads criados
  - Asset Customization Rules para Feed vs Stories
"""

import time
import json
import random
import os
import tempfile
import requests
import re
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adimage import AdImage
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adspixel import AdsPixel


def list_drive_folder(folder_url):
    """
    Tenta listar arquivos de uma pasta pública do Google Drive sem usar API Key.
    Retorna uma lista de dicionários {'id': ..., 'name': ...}
    """
    try:
        # Extrair Folder ID
        folder_id = None
        if '/folders/' in folder_url:
            folder_id = folder_url.split('/folders/')[1].split('?')[0].split('/')[0]
        elif 'id=' in folder_url:
            folder_id = folder_url.split('id=')[1].split('&')[0]
        
        if not folder_id:
            return {'error': 'ID da pasta não encontrado na URL.'}

        # URL para pegar o JSON da pasta (hack público)
        # Nota: Este método pode ser instável se o Google mudar a estrutura.
        resp = requests.get(folder_url, headers={'User-Agent': 'Mozilla/5.0'}).text
        
        # Regex para encontrar o JSON que contém os nomes e IDs dos arquivos
        # O Google Drive injeta os dados na variável 'AF_initDataCallback' ou similar
        pattern = r'\["(?P<id>[a-zA-Z0-9_-]{20,})",\["(?P<name>[^"]+)"'
        matches = re.finditer(pattern, resp)
        
        files = []
        seen_ids = set()
        for m in matches:
            v_id = m.group('id')
            v_name = m.group('name')
            if v_id not in seen_ids and '.' in v_name: # Filtra para buscar arquivos com extensão
                files.append({'id': v_id, 'name': v_name})
                seen_ids.add(v_id)
        
        return {'files': files}
    except Exception as e:
        return {'error': str(e)}


class MetaUploader:
    """Gerencia uploads para a Meta Ads API com rate limiting, retry e delay."""

    RATE_LIMIT_THRESHOLD = 80        # Pausa se uso >= 80%
    RATE_LIMIT_PAUSE_SECONDS = 300   # 5 minutos
    DELAY_MIN = 1.5
    DELAY_MAX = 3.0
    MAX_RETRIES = 3
    RETRY_BACKOFF = 5                # Segundos entre retries

    def __init__(self, account_id, access_token, app_id, app_secret):
        self.account_id = account_id
        self.access_token = access_token
        self.app_id = app_id
        self.app_secret = app_secret
        self.logs = []
        self._callback = None

        FacebookAdsApi.init(app_id, app_secret, access_token)
        FacebookAdsApi.init(app_id, app_secret, access_token)
        self.account = AdAccount(account_id)

    # ======================== PERFORMANCE & INSIGHTS ========================

    def get_campaign_insights(self, date_preset='today'):
        """
        Busca métricas de performance das campanhas (Gasto, CAC, etc).
        Filtra por campanhas ACTIVE ou PAUSED.
        """
        try:
            fields = [
                'campaign_id',
                'campaign_name',
                'spend',
                'actions',
                'cost_per_action_type',
                'objective',
                'account_currency'
            ]
            params = {
                'level': 'campaign',
                'date_preset': date_preset,
                'filtering': [
                    {'field': 'campaign.effective_status', 'operator': 'IN', 'value': ['ACTIVE']}
                ],
                'limit': 100
            }
            
            insights = self.account.get_insights(fields=fields, params=params)
            
            results = []
            for ins in insights:
                # Extrair resultados (compras ou leads conforme o objetivo)
                # Heurística: se o objetivo for LEAD_GENERATION, busca 'lead'
                # se for OUTCOMES/CONVERSIONS, busca 'purchase' ou 'offsite_conversion.fb_pixel_purchase'
                
                actions = ins.get('actions', [])
                objective = ins.get('objective', '')
                
                res_count = 0
                cac = 0
                spend = float(ins.get('spend', 0))
                
                # Mapeamento dinâmico de resultados
                for action in actions:
                    a_type = action.get('action_type', '')
                    if objective == 'LEAD_GENERATION' and a_type == 'lead':
                        res_count += int(action.get('value', 0))
                    elif 'purchase' in a_type:
                        res_count += int(action.get('value', 0))
                    elif a_type == 'onsite_conversion.messaging_conversation_started_7d':
                        res_count += int(action.get('value', 0))

                if res_count > 0:
                    cac = spend / res_count
                
                results.append({
                    'id': ins.get('campaign_id'),
                    'name': ins.get('campaign_name'),
                    'spend': spend,
                    'results': res_count,
                    'cac': cac,
                    'currency': ins.get('account_currency', 'BRL'),
                    'status': 'ACTIVE'
                })
            
            print(f"📈 [get_campaign_insights] {len(results)} campanhas auditadas.")
            return results
        except Exception as e:
            print(f"❌ [get_campaign_insights] Erro: {e}")
            return []

    # ======================== IDENTITY & TRACKING FETCHERS ========================

    def get_pages(self):
        """Busca páginas administradas pelo usuário (1 chamada SDK, sem fallback individual)."""
        try:
            me = User(fbid='me')
            pages = me.get_accounts(fields=['name', 'access_token', 'instagram_business_account'])
            
            result = []
            for p in pages:
                ig_id = None
                if 'instagram_business_account' in p:
                    ig_id = p['instagram_business_account'].get('id')
                
                result.append({
                    'id': p.get('id'),
                    'name': p.get('name', '???'),
                    'instagram_id': ig_id
                })

            print(f"📄 [get_pages] {len(result)} páginas (1 API call)")
            return result
        except Exception as e:
            print(f"❌ [get_pages] Erro: {e}")
            return []

    def get_pixels(self):
        """Busca pixels da Ad Account."""
        try:
            pixels = self.account.get_ads_pixels(fields=['name', 'id'])
            return [{'id': p['id'], 'name': p['name']} for p in pixels]
        except Exception as e:
            self._log(f"⚠️ Erro ao buscar pixels: {e}")
            return []

    def get_leadgen_forms(self, page_id, page_access_token=None):
        """Busca formulários de lead de uma página do Facebook."""
        try:
            token = page_access_token or self.access_token

            # Camada de Segurança: Tentar obter o Page Access Token se não foi fornecido
            if not page_access_token:
                try:
                    p_resp = requests.get(
                        f"https://graph.facebook.com/v18.0/{page_id}",
                        params={'fields': 'access_token', 'access_token': self.access_token}
                    ).json()
                    if 'access_token' in p_resp:
                        token = p_resp['access_token']
                        print(f"🔑 [get_leadgen_forms] Usando Page Access Token para a página {page_id}")
                except Exception as e:
                    print(f"⚠️ [get_leadgen_forms] Falha ao obter Page Access Token (usando User Token): {e}")

            resp = requests.get(
                f"https://graph.facebook.com/v18.0/{page_id}/leadgen_forms",
                params={'fields': 'id,name,status', 'access_token': token, 'limit': 100}
            ).json()

            if 'error' in resp:
                error_msg = resp['error'].get('message', '?')
                print(f"⚠️ [get_leadgen_forms] API error: {error_msg}")
                self._log(f"⚠️ Erro ao buscar formulários: {error_msg}")
                return []

            forms = []
            for f in resp.get('data', []):
                forms.append({
                    'id': f.get('id'),
                    'name': f.get('name', f"Form {f.get('id')}"),
                    'status': f.get('status', 'ACTIVE')
                })
            print(f"📋 [get_leadgen_forms] {len(forms)} formulários encontrados para página {page_id}")
            return forms
        except Exception as e:
            print(f"❌ [get_leadgen_forms] Exception: {e}")
            return []

    def get_instagram_accounts(self, pages_data=None):
        """
        Busca IGs acessíveis para anúncios.
        Camada 1: act_{id}/instagram_accounts
        Camada 2: act_{id}/connected_instagram_accounts
        Camada 3: IGs já retornados pelas páginas (sem chamadas extras)
        """
        seen_ids = set()
        result = []

        def add_ig(ig_id, username, source):
            if ig_id and ig_id not in seen_ids:
                seen_ids.add(ig_id)
                result.append({'id': ig_id, 'username': username, 'source': source})

        # ---- Camada 1 + 2: batch via requests ----
        endpoints = [
            ('instagram_accounts', 'ad_account'),
            ('connected_instagram_accounts', 'connected')
        ]
        for edge, source in endpoints:
            try:
                resp = requests.get(
                    f"https://graph.facebook.com/v18.0/{self.account_id}/{edge}",
                    params={'fields': 'id,username', 'access_token': self.access_token, 'limit': 100}
                ).json()
                if 'error' in resp:
                    print(f"⚠️ [get_ig/{edge}] API error: {resp['error'].get('message', '?')}")
                else:
                    count = 0
                    for ig in resp.get('data', []):
                        add_ig(ig.get('id'), ig.get('username', f"IG {ig.get('id')}"), source)
                        count += 1
                    print(f"✅ [get_ig/{edge}] {count} IGs encontrados")
            except Exception as e:
                print(f"❌ [get_ig/{edge}] Exception: {e}")

        # ---- Camada 3: IGs das páginas (sem chamadas extras) ----
        if not result and pages_data:
            for page in pages_data:
                ig_id = page.get('instagram_id')
                if ig_id:
                    # Tentar buscar o username real do IG
                    ig_username = None
                    try:
                        resp = requests.get(
                            f"https://graph.facebook.com/v18.0/{ig_id}",
                            params={'fields': 'username', 'access_token': self.access_token}
                        ).json()
                        ig_username = resp.get('username')
                    except Exception:
                        pass
                    add_ig(ig_id, ig_username or page.get('name', f'IG {ig_id}'), 'page_linked')

        print(f"📸 [get_instagram_accounts] {len(result)} IGs ({len(seen_ids)} únicos)")
        if result:
            for ig in result:
                print(f"   → {ig['username']} (ID: {ig['id']}, fonte: {ig['source']})")
        return result

    def get_identity_data(self):
        """
        Busca páginas, instagrams e pixels em paralelo.
        Retorna dict com {pages, instagrams, pixels}.
        """
        from concurrent.futures import ThreadPoolExecutor
        import time

        start = time.time()
        pages_result = []
        pixels_result = []

        def fetch_pages():
            nonlocal pages_result
            pages_result = self.get_pages()

        def fetch_pixels():
            nonlocal pixels_result
            pixels_result = self.get_pixels()

        # Pages e Pixels em paralelo
        with ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(fetch_pages)
            executor.submit(fetch_pixels)

        # IGs dependem dos dados das páginas (para fallback), então roda depois
        instagrams_result = self.get_instagram_accounts(pages_data=pages_result)

        elapsed = time.time() - start
        print(f"⚡ [get_identity_data] Completo em {elapsed:.1f}s — {len(pages_result)} páginas, {len(instagrams_result)} IGs, {len(pixels_result)} pixels")

        return {
            'pages': pages_result,
            'instagrams': instagrams_result,
            'pixels': pixels_result
        }

    def set_callback(self, callback):
        """Define uma função callback para logs em tempo real: callback(msg)"""
        self._callback = callback

    def _log(self, msg):
        """Registra log e chama callback se existir."""
        self.logs.append(msg)
        if self._callback:
            self._callback(msg)

    # ======================== RATE LIMITING ========================

    def check_rate_limit(self, response_headers=None):
        """
        Verifica o header x-business-use-case-usage.
        Se uso >= 80%, pausa por 5 minutos.
        """
        if not response_headers:
            return False

        usage_header = response_headers.get('x-business-use-case-usage')
        if not usage_header:
            return False

        try:
            usage_data = json.loads(usage_header)
            for account_id, usages in usage_data.items():
                for usage in usages:
                    call_count = usage.get('call_count', 0)
                    total_cputime = usage.get('total_cputime', 0)
                    total_time = usage.get('total_time', 0)

                    max_usage = max(call_count, total_cputime, total_time)

                    if max_usage >= self.RATE_LIMIT_THRESHOLD:
                        self._log(
                            f"⏸️ Rate limit alto ({max_usage}%). "
                            f"Pausando {self.RATE_LIMIT_PAUSE_SECONDS // 60} min..."
                        )
                        time.sleep(self.RATE_LIMIT_PAUSE_SECONDS)
                        self._log("▶️ Retomando uploads após pausa de rate limit.")
                        return True
        except (json.JSONDecodeError, TypeError):
            pass

        return False

    # ======================== DELAY ========================

    def smart_delay(self):
        """Aplica delay aleatório entre uploads (1.5-3s)."""
        delay = random.uniform(self.DELAY_MIN, self.DELAY_MAX)
        self._log(f"⏳ Aguardando delay de segurança ({delay:.1f}s)...")
        time.sleep(delay)

    # ======================== RETRY ========================

    def _with_retry(self, operation_name, func):
        """Executa uma função com até MAX_RETRIES tentativas."""
        last_error = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                result = func()
                return result
            except Exception as e:
                last_error = e
                if attempt < self.MAX_RETRIES:
                    self._log(
                        f"⚠️ {operation_name} falhou (tentativa {attempt}/{self.MAX_RETRIES}): "
                        f"{str(e)[:100]}. Retentando em {self.RETRY_BACKOFF}s..."
                    )
                    time.sleep(self.RETRY_BACKOFF)
                else:
                    self._log(
                        f"❌ {operation_name} falhou após {self.MAX_RETRIES} tentativas: "
                        f"{str(e)[:150]}"
                    )
        raise last_error

    # ======================== UPLOAD DE MÍDIA ========================

    def _normalize_drive_link(self, link):
        """
        Converte links do Google Drive (visualização, compartilhamento, abreviações)
        em links de download direto robustos.
        """
        if not link or 'drive.google.com' not in link:
            return link
        
        file_id = None
        # Padrão 1: /file/d/ID/view
        if '/file/d/' in link:
            file_id = link.split('/file/d/')[1].split('/')[0]
        # Padrão 2: ?id=ID
        elif 'id=' in link:
            parsed = urllib.parse.urlparse(link)
            file_id = urllib.parse.parse_qs(parsed.query).get('id', [None])[0]
            
        if file_id:
            return f"https://drive.google.com/uc?export=download&id={file_id}"
        
        return link

    def _download_file(self, url, dest_path):
        """Baixa um arquivo de uma URL, tratando confirmação de vírus do Google Drive para arquivos grandes."""
        try:
            import re
            session = requests.Session()
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            # Estratégia 1: Download direto
            response = session.get(url, stream=True, timeout=60, headers=headers)
            
            # Checar se temos cookie de download_warning
            token = None
            for key, value in response.cookies.items():
                if key.startswith('download_warning'):
                    token = value
                    break
            
            if token:
                self._log("🛡️ Token via cookie detectado. Confirmando download...")
                response = session.get(url, params={'confirm': token}, stream=True, timeout=120, headers=headers)
            
            # Salvar resposta
            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=32768):
                    if chunk:
                        f.write(chunk)
            
            # Validar se é HTML (página de confirmação do Drive)
            file_size = os.path.getsize(dest_path)
            is_html = False
            if file_size < 200000:  # Arquivos < 200KB podem ser HTML
                with open(dest_path, 'rb') as f:
                    content = f.read()
                if b'<html' in content.lower() or b'<!doctype' in content.lower():
                    is_html = True
            
            if is_html:
                self._log("⚠️ Drive retornou página HTML. Tentando extrair token de confirmação...")
                content_str = content.decode('utf-8', errors='replace')
                
                # Estratégia 2: Extrair token do HTML (botão de confirmação)
                confirm_match = re.search(r'confirm=([0-9A-Za-z_-]+)', content_str)
                uuid_match = re.search(r'name="uuid"\s+value="([^"]+)"', content_str)
                
                if confirm_match:
                    confirm_token = confirm_match.group(1)
                    self._log(f"🔑 Token extraído do HTML: {confirm_token[:8]}...")
                    response = session.get(url, params={'confirm': confirm_token}, 
                                         stream=True, timeout=120, headers=headers)
                    with open(dest_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=32768):
                            if chunk:
                                f.write(chunk)
                else:
                    # Estratégia 3: Forçar confirm=t (funciona para muitos arquivos grandes)
                    self._log("🔑 Tentando confirm=t como fallback...")
                    response = session.get(url, params={'confirm': 't'}, 
                                         stream=True, timeout=120, headers=headers)
                    with open(dest_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=32768):
                            if chunk:
                                f.write(chunk)
                
                # Validar novamente
                file_size = os.path.getsize(dest_path)
                if file_size < 200000:
                    with open(dest_path, 'rb') as f:
                        header = f.read(500)
                    if b'<html' in header.lower() or b'<!doctype' in header.lower():
                        self._log(f"❌ Download falhou — Drive continua retornando HTML.")
                        self._log(f"   → Verifique se o arquivo tem permissão 'Qualquer pessoa com o link pode visualizar'.")
                        os.unlink(dest_path)
                        return False
            
            file_size = os.path.getsize(dest_path)
            self._log(f"✅ Download concluído: {os.path.basename(dest_path)} ({file_size / 1024:.0f} KB)")
            return True
        except Exception as e:
            self._log(f"❌ Erro no download do arquivo: {str(e)}")
            if os.path.exists(dest_path):
                os.unlink(dest_path)
            return False


    def upload_image_url(self, url):
        """Upload de imagem via URL (parâmetro url da Meta) com fallback em memória (BytesIO)."""
        url = self._normalize_drive_link(url)
        self._log(f"🔗 Enviando URL de imagem para a Meta: {url[:60]}...")
        
        api_url = f"https://graph.facebook.com/v18.0/{self.account_id}/adimages"
        
        def _do():
            resp = requests.post(api_url, data={'url': url, 'access_token': self.access_token})
            result = resp.json()
            if 'error' in result:
                msg = result['error'].get('message', '')
                # Fallback via bytes em memória (BytesIO) se a Meta falhar no download direto
                # Inclusão de 'capability' pois alguns Apps não podem enviar via URL direta
                if any(k in msg.lower() for k in ['problem', 'download', 'failed', 'could not', 'capability']):
                    self._log("⚠️ Meta falhou ao baixar URL. Tentando fallback via download local...")
                    try:
                        # FIX: Baixar para arquivo temp real (SDK não aceita BytesIO diretamente)
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                        }
                        r = requests.get(url, timeout=60, headers=headers)
                        r.raise_for_status()
                        
                        # Salvar em arquivo temporário real
                        tmp_path = os.path.join(tempfile.gettempdir(), f"img_fallback_{int(time.time())}.jpg")
                        with open(tmp_path, 'wb') as f:
                            f.write(r.content)
                        
                        try:
                            image_hash = self.upload_image(tmp_path)
                            return image_hash
                        finally:
                            if os.path.exists(tmp_path):
                                os.unlink(tmp_path)
                    except Exception as ex:
                        self._log(f"❌ Falha no fallback de download local: {str(ex)}")
                        raise ex
                raise Exception(msg)
            
            images = result.get('images', {})
            if not images: raise Exception("Meta não retornou hash da imagem")
            return list(images.values())[0].get('hash')

        image_hash = self._with_retry(f"Upload imagem via URL", _do)
        self._log(f"✅ Imagem via URL vinculada (hash: {image_hash[:12]}...)")
        return image_hash

    def upload_video_url(self, url):
        """Upload de vídeo via URL (parâmetro file_url da Meta) com fallback local robusto."""
        url = self._normalize_drive_link(url)
        self._log(f"🔗 Enviando URL de vídeo para a Meta: {url[:60]}...")
        
        api_url = f"https://graph.facebook.com/v18.0/{self.account_id}/advideos"
        
        def _do():
            resp = requests.post(api_url, data={'file_url': url, 'access_token': self.access_token})
            result = resp.json()
            if 'error' in result:
                msg = result['error'].get('message', '')
                # Fallback se a Meta não conseguir baixar o arquivo
                if any(k in msg.lower() for k in ['problem', 'download', 'failed', 'could not', 'capability']):
                    self._log("⚠️ Meta falhou ao baixar vídeo. Baixando localmente para fallback...")
                    tmp_path = os.path.join(tempfile.gettempdir(), f"vid_{int(time.time())}.mp4")
                    if self._download_file(url, tmp_path):
                        try:
                            # Extrair thumbnail AGORA, enquanto temos o arquivo local
                            thumb_hash = self.extract_video_thumbnail(tmp_path)
                            if thumb_hash:
                                self._log("✅ Thumbnail extraída do arquivo local do vídeo.")
                                self._pending_thumb_hash = thumb_hash
                            
                            res = self.upload_video(tmp_path)
                            return res
                        finally:
                            # Deletar arquivo temporário do vídeo após upload
                            if os.path.exists(tmp_path):
                                os.unlink(tmp_path)
                                self._log(f"🗑️ Arquivo temporário de vídeo deletado.")
                raise Exception(msg)
            return result.get('id')

        self._pending_thumb_hash = None  # Resetar antes de cada upload
        video_id = self._with_retry(f"Upload vídeo via URL", _do)
        self._log(f"✅ Vídeo via URL vinculado (ID: {video_id})")
        return video_id

    def extract_video_thumbnail(self, video_path):
        """
        Extrai um frame do vídeo como thumbnail usando ffmpeg.
        Retorna o image_hash da thumbnail ou None se falhar.
        """
        try:
            import subprocess
            thumb_path = os.path.join(tempfile.gettempdir(), f"thumb_{int(time.time())}.jpg")
            
            # Extrair frame no segundo 1 do vídeo
            # -ss APÓS -i para garantir compatibilidade com todos os formatos
            cmd = [
                'ffmpeg', '-y',
                '-i', video_path,
                '-ss', '00:00:01',
                '-vframes', '1',
                '-q:v', '2',
                '-f', 'image2',
                thumb_path
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=60)
            
            if result.returncode == 0 and os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
                self._log("🖼️ Thumbnail extraída do vídeo via ffmpeg.")
                try:
                    image_hash = self.upload_image(thumb_path)
                    return image_hash
                finally:
                    if os.path.exists(thumb_path):
                        os.unlink(thumb_path)
            else:
                stderr = result.stderr.decode('utf-8', errors='replace') if result.stderr else 'sem output'
                # Mostrar as últimas linhas relevantes do stderr para debug
                error_lines = [l for l in stderr.split('\n') if l.strip() and not l.strip().startswith('ffmpeg version')]
                error_msg = '\n'.join(error_lines[-3:]) if error_lines else stderr[:200]
                self._log(f"⚠️ ffmpeg falhou ao extrair thumbnail: {error_msg[:200]}")
                return None
        except FileNotFoundError:
            self._log("⚠️ ffmpeg não encontrado no sistema. Thumbnail automática não disponível.")
            return None
        except subprocess.TimeoutExpired:
            self._log("⚠️ ffmpeg timeout ao extrair thumbnail (>60s).")
            return None
        except Exception as e:
            self._log(f"⚠️ Erro ao extrair thumbnail: {e}")
            return None

    def extract_video_thumbnail_from_id(self, video_id):
        """
        Consulta a Meta API para obter a URL de download do vídeo,
        baixa localmente e extrai um frame como thumbnail.
        Retorna image_hash ou None se falhar.
        """
        try:
            self._log(f"🎬 Buscando URL do vídeo {video_id} para extrair thumbnail...")
            
            # Consultar a Meta API para obter a URL de download do vídeo
            url = f"https://graph.facebook.com/v18.0/{video_id}"
            params = {
                'fields': 'source',
                'access_token': self.access_token
            }
            resp = requests.get(url, params=params).json()
            
            if 'error' in resp or 'source' not in resp:
                self._log(f"⚠️ Não foi possível obter URL do vídeo: {resp.get('error', {}).get('message', 'sem source')}")
                return None
            
            video_url = resp['source']
            self._log(f"⬇️ Baixando vídeo para extração de thumbnail...")
            
            tmp_vid = os.path.join(tempfile.gettempdir(), f"vid_thumb_{int(time.time())}.mp4")
            if self._download_file(video_url, tmp_vid):
                try:
                    return self.extract_video_thumbnail(tmp_vid)
                finally:
                    if os.path.exists(tmp_vid):
                        os.unlink(tmp_vid)
            else:
                self._log("⚠️ Falha ao baixar vídeo para extração de thumbnail.")
                return None
        except Exception as e:
            self._log(f"⚠️ Erro em extract_video_thumbnail_from_id: {e}")
            return None

    def upload_image(self, file_path):
        """Upload de imagem para a conta. Retorna image_hash."""
        filename = os.path.basename(file_path)
        self._log(f"📤 Fazendo upload de imagem: {filename}...")

        def _do():
            image = AdImage(parent_id=self.account_id)
            image[AdImage.Field.filename] = file_path
            image.remote_create()
            return image[AdImage.Field.hash]

        image_hash = self._with_retry(f"Upload imagem '{filename}'", _do)
        self._log(f"✅ Imagem '{filename}' enviada (hash: {image_hash[:12]}...)")
        return image_hash

    def upload_video(self, file_path):
        """Upload de vídeo para a conta. Retorna video_id."""
        filename = os.path.basename(file_path)
        self._log(f"📤 Fazendo upload de vídeo: {filename}...")

        def _do():
            video = AdVideo(parent_id=self.account_id)
            video[AdVideo.Field.filepath] = file_path
            video.remote_create()
            return video.get_id()

        video_id = self._with_retry(f"Upload vídeo '{filename}'", _do)
        self._log(f"✅ Vídeo '{filename}' enviado (ID: {video_id})")
        return video_id

    def upload_media(self, file_path=None, url=None):
        """
        Upload de mídia (imagem ou vídeo), seja via arquivo local ou URL.
        Detecta tipo proativamente via extensão ou cabeçalhos HTTP.
        """
        # Se for URL
        if url:
            norm_url = self._normalize_drive_link(url)
            
            # Tentar detectar tipo via extensão primeiro
            ext = os.path.splitext(norm_url.split('?')[0])[1].lower()
            video_exts = {'.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v', '.gif'}
            image_exts = {'.jpg', '.jpeg', '.png', '.webp', '.heic'}
            
            media_type = None
            if ext in video_exts: media_type = 'video'
            elif ext in image_exts: media_type = 'image'

            # Se não detectou (ex: Drive link), fazer uma requisição HEAD para ver o Content-Type
            if not media_type or 'drive.google.com' in norm_url:
                try:
                    head = requests.head(norm_url, allow_redirects=True, timeout=5)
                    ct = head.headers.get('Content-Type', '').lower()
                    if 'video' in ct: media_type = 'video'
                    elif 'image' in ct: media_type = 'image'
                except Exception:
                    pass
            
            # Fallback final: se ainda não sabe, tenta vídeo primeiro (comportamento atual, mas mais seguro)
            if media_type == 'video' or (not media_type and 'drive.google.com' in norm_url):
                try:
                    video_id = self.upload_video_url(url)
                    # Capturar thumbnail gerada durante o download local (se houver)
                    thumb_hash = getattr(self, '_pending_thumb_hash', None)
                    return {'type': 'video', 'id': video_id, 'hash': None, 'thumb_hash': thumb_hash, 'source_url': url}
                except Exception as e:
                    # Se falhar como vídeo e o erro sugerir que é imagem, tenta imagem
                    if 'image' in str(e).lower() or 'not a video' in str(e).lower():
                         image_hash = self.upload_image_url(url)
                         return {'type': 'image', 'hash': image_hash, 'id': None, 'source_url': url}
                    raise e
            else:
                image_hash = self.upload_image_url(url)
                return {'type': 'image', 'hash': image_hash, 'id': None, 'source_url': url}

        # Se for arquivo local
        if file_path:
            ext = os.path.splitext(file_path)[1].lower()
            video_exts = {'.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v', '.gif'}

            if ext in video_exts:
                # Extrair thumbnail ANTES de subir o vídeo (temos o arquivo local)
                thumb_hash = self.extract_video_thumbnail(file_path)
                if thumb_hash:
                    self._log("✅ Thumbnail extraída do arquivo local de vídeo.")
                video_id = self.upload_video(file_path)
                return {'type': 'video', 'id': video_id, 'hash': None, 'thumb_hash': thumb_hash}
            else:
                image_hash = self.upload_image(file_path)
                return {'type': 'image', 'hash': image_hash, 'id': None}
        
        return None

    def wait_for_video_ready(self, video_id, timeout=120, interval=10):
        """
        Consulta o status do vídeo na Meta API até que esteja 'ready' ou atinja o timeout.
        Evita erro de 'arquivo inválido' ao criar criativos imediatamente após upload.
        """
        self._log(f"⏳ Consultando processamento do vídeo {video_id}...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Usando requests direta para evitar overhead do SDK em polling
                url = f"https://graph.facebook.com/v18.0/{video_id}"
                params = {
                    'fields': 'status',
                    'access_token': self.access_token
                }
                resp = requests.get(url, params=params).json()
                
                if 'error' in resp:
                    self._log(f"⚠️ Erro ao consultar status: {resp['error'].get('message')}")
                    time.sleep(interval)
                    continue

                status_data = resp.get('status', {})
                video_status = status_data.get('video_status')

                if video_status == 'ready':
                    self._log(f"✅ Vídeo {video_id} processado e pronto para uso.")
                    return True
                elif video_status == 'error':
                    err_msg = status_data.get('error_description', 'Erro de processamento na Meta')
                    self._log(f"❌ Erro no processamento do vídeo: {err_msg}")
                    return False
                
                self._log(f"   → Status: {video_status}. Aguardando {interval}s...")
                time.sleep(interval)
                
            except Exception as e:
                self._log(f"⚠️ Falha na chamada de polling: {e}")
                time.sleep(interval)

        self._log(f"⚠️ Timeout de {timeout}s atingido. Prosseguindo com cautela...")
        return False

    def wait_for_image_ready(self, image_hash, timeout=60, interval=5):
        """
        Consulta o status da imagem na Meta API até que esteja 'ACTIVE'.
        Embora imagens sejam processadas rápido, garante proatividade em URLs lentas.
        """
        self._log(f"⏳ Verificando disponibilidade da imagem {image_hash[:12]}...")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Consulta act_id/adimages com filtro de hash
                url = f"https://graph.facebook.com/v18.0/{self.account_id}/adimages"
                params = {
                    'hashes': json.dumps([image_hash]),
                    'fields': 'hash,status',
                    'access_token': self.access_token
                }
                resp = requests.get(url, params=params).json()
                
                if 'error' in resp:
                    self._log(f"⚠️ Erro ao consultar imagem: {resp['error'].get('message')}")
                    time.sleep(interval)
                    continue

                images = resp.get('data', [])
                if images:
                    img_status = images[0].get('status')
                    if img_status == 'ACTIVE':
                        self._log(f"✅ Imagem {image_hash[:12]} está ativa e pronta.")
                        return True
                    self._log(f"   → Status: {img_status}. Aguardando {interval}s...")
                else:
                    self._log(f"   → Imagem ainda não indexada. Aguardando {interval}s...")

                time.sleep(interval)
                
            except Exception as e:
                self._log(f"⚠️ Falha na chamada de polling de imagem: {e}")
                time.sleep(interval)

        self._log(f"⚠️ Timeout atingido para imagem. Prosseguindo...")
        return False

    # ======================== CREATIVE COM ASSET CUSTOMIZATION ========================

    # ======================== CREATIVE COM ASSET CUSTOMIZATION ========================

    def create_creative_with_placements(self, page_id, feed_media, stories_media,
                                         link_url, primary_texts, headlines,
                                         cta_type, instagram_user_id=None, url_tags='',
                                         lead_gen_form_id=None):
        """
        Cria AdCreative via REST API direta.
        Estratégia 1: asset_feed_spec com asset_customization_rules (feed+stories)
        Estratégia 2 (fallback): link_data simples (1 imagem para todos os placements)
        Retorna o ID numérico do creative criado.
        """
        import json

        self._log("🎨 Criando AdCreative...")

        # Fallback Logic
        if not feed_media and not stories_media:
             raise ValueError("Nenhuma mídia fornecida para criar o criativo.")
        if not feed_media:
            feed_media = stories_media
        if not stories_media:
            stories_media = feed_media

        api_url = f"https://graph.facebook.com/v18.0/{self.account_id}/adcreatives"

        # Normalizar textos
        body_text = primary_texts[0] if primary_texts else "Check this out!"
        if isinstance(body_text, dict):
            body_text = body_text.get('text', ' ')
        headline_text = headlines[0] if headlines else "Limited Offer"
        if isinstance(headline_text, dict):
            headline_text = headline_text.get('text', ' ')

        def _post_creative(payload_dict, label=""):
            """Helper: POST para /adcreatives com log completo."""
            post_data = {'access_token': self.access_token}
            for k, v in payload_dict.items():
                post_data[k] = json.dumps(v) if isinstance(v, (dict, list)) else v

            # Polling de segurança para mídias: Garante que a Meta processou tudo
            video_ids = []
            image_hashes = []

            # Detectar vídeos e imagens no payload
            id_str = str(payload_dict)
            
            # Polling de Vídeos
            if 'videos' in payload_dict:
                video_ids = [v.get('video_id') for v in payload_dict['videos'] if v.get('video_id')]
            elif 'object_story_spec' in payload_dict:
                vd = payload_dict['object_story_spec'].get('video_data', {})
                if vd.get('video_id'):
                    video_ids = [vd['video_id']]

            # Polling de Imagens
            if 'images' in payload_dict:
                image_hashes = [img.get('hash') for img in payload_dict['images'] if img.get('hash')]
            elif 'object_story_spec' in payload_dict:
                ld = payload_dict['object_story_spec'].get('link_data', {})
                if ld.get('image_hash'):
                    image_hashes = [ld['image_hash']]
                vd = payload_dict['object_story_spec'].get('video_data', {})
                if vd.get('image_hash'):
                    image_hashes.append(vd['image_hash'])

            # Aguardar mídias
            for vid in video_ids:
                self.wait_for_video_ready(vid)
            
            for h in image_hashes:
                self.wait_for_image_ready(h)

            resp = requests.post(api_url, data=post_data)
            result = resp.json()

            if 'error' in result:
                error = result['error']
                error_detail = (
                    f"Code: {error.get('code')}, "
                    f"SubCode: {error.get('error_subcode', 'N/A')}, "
                    f"Message: {error.get('message', '?')}, "
                    f"Type: {error.get('type', '?')}, "
                    f"Title: {error.get('error_user_title', 'N/A')}, "
                    f"UserMsg: {error.get('error_user_msg', 'N/A')}"
                )
                print(f"❌ [{label}] API Error Detail: {json.dumps(result, indent=2, ensure_ascii=False)}")
                self._log(f"❌ [{label}] Falha: {error.get('message', '?')} ({error.get('error_user_msg', 'N/A')})")
                return None, error.get('message', 'Unknown error')

            creative_id = result.get('id')
            if creative_id:
                print(f"✅ [{label}] Creative criado: {creative_id}")
            return creative_id, None

        # ===== ESTRATÉGIA 1: asset_feed_spec (Completa com Customização) =====
        def try_complex_creative():
            self._log("📋 Tentando criativo completo (asset_feed_spec)...")
            payload = self._build_creative_payload(
                page_id=page_id,
                feed_media=feed_media,
                stories_media=stories_media,
                link_url=link_url,
                bodies=primary_texts,
                titles=headlines,
                cta_type=cta_type,
                url_tags=url_tags,
                instagram_user_id=instagram_user_id,
                lead_gen_form_id=lead_gen_form_id
            )
            creative_id, error = _post_creative(payload, "complex")
            return creative_id, error

        # ===== ESTRATÉGIA 2: link_data ou video_data (Fallback) =====
        def try_simple_creative():
            self._log("📋 Tentando criativo simples (link_data/video_data) como fallback...")
            
            cta_payload = {
                'type': cta_type,
                'value': {
                    'lead_gen_form_id': lead_gen_form_id
                } if lead_gen_form_id else {
                    'link': link_url
                }
            }

            object_story_spec = {'page_id': page_id}
            
            if feed_media['type'] == 'video':
                # Para vídeo, usamos video_data
                video_payload = {
                    'video_id': feed_media['id'],
                    'message': body_text,
                    'call_to_action': cta_payload,
                    'title': headline_text
                }
                
                # FIX: Instagram EXIGE image_hash ou image_url em video_data
                # Prioridade 1: usar imagem do par (stories_media)
                if stories_media and stories_media['type'] == 'image':
                    video_payload['image_hash'] = stories_media['hash']
                    self._log("🖼️ Usando imagem do Stories como thumbnail para o vídeo de Feed.")
                
                # Prioridade 2: usar thumbnail já extraída durante o upload (do arquivo local do Drive)
                elif feed_media.get('thumb_hash'):
                    video_payload['image_hash'] = feed_media['thumb_hash']
                    self._log("✅ Usando thumbnail extraída durante o upload do vídeo.")
                
                # Prioridade 3: baixar do Drive novamente para extrair thumbnail (evita baixar do Facebook)
                elif feed_media.get('source_url'):
                    self._log("🔍 Extraindo thumbnail do vídeo via link original do Drive...")
                    try:
                        tmp_vid = os.path.join(tempfile.gettempdir(), f"vid_thumb_{int(time.time())}.mp4")
                        if self._download_file(feed_media['source_url'], tmp_vid):
                            try:
                                thumb_hash = self.extract_video_thumbnail(tmp_vid)
                                if thumb_hash:
                                    video_payload['image_hash'] = thumb_hash
                                    self._log("✅ Thumbnail gerada do link original do Drive.")
                                else:
                                    self._log("⚠️ ffmpeg não conseguiu extrair thumbnail do vídeo.")
                            finally:
                                if os.path.exists(tmp_vid):
                                    os.unlink(tmp_vid)
                                    self._log("🗑️ Arquivo temporário de thumbnail deletado.")
                    except Exception as te:
                        self._log(f"⚠️ Erro ao extrair thumbnail via Drive: {te}")
                
                else:
                    self._log("⚠️ Sem thumbnail disponível. Criativo pode ser rejeitado pelo Instagram.")
                
                object_story_spec['video_data'] = video_payload
            else:
                # Para imagem, usamos link_data
                object_story_spec['link_data'] = {
                    'link': link_url,
                    'message': body_text,
                    'name': headline_text,
                    'image_hash': feed_media['hash'],
                    'call_to_action': cta_payload
                }
            
            if instagram_user_id:
                object_story_spec['instagram_user_id'] = instagram_user_id

            payload = {'object_story_spec': object_story_spec}
            if url_tags:
                payload['url_tags'] = url_tags

            # Tentar com IG
            creative_id, error = _post_creative(payload, "simple+ig")
            
            # Se IG falhou, tenta sem
            if not creative_id and instagram_user_id and 'instagram' in (error or '').lower():
                self._log("⚠️ Removendo instagram_user_id e tentando novamente...")
                del object_story_spec['instagram_user_id']
                creative_id, error = _post_creative(payload, "simple-ig")

            return creative_id, error

        def _do():
            # Tentar Complexo primeiro para manter Stories 9:16
            cid, err = try_complex_creative()
            if cid: return cid
            
            # Se falhou, tenta Simples
            cid, err = try_simple_creative()
            if cid: return cid
            
            raise Exception(f"Criação de criativo falhou em todas as estratégias. Último erro: {err}")

        creative_id = self._with_retry("Criar AdCreative", _do)
        self._log(f"✅ AdCreative criado (ID: {creative_id})")
        return creative_id

    def _build_creative_payload(self, page_id, feed_media, stories_media,
                                link_url, bodies, titles, cta_type, url_tags, instagram_user_id=None, lead_gen_form_id=None):
        """Monta o payload completo para POST /adcreatives."""

        # Normalize: convert plain strings to {"text": "..."} format
        def to_text_list(items, fallback=' '):
            if not items:
                return [{'text': fallback}]
            return [{'text': item} if isinstance(item, str) else item for item in items]

        # Labels
        FEED_LABEL = "feed_creative"
        STORY_LABEL = "story_creative"

        # Build images/videos arrays
        images = []
        videos = []
        
        def add_media(media, label):
            if media['type'] == 'image':
                images.append({'hash': media['hash'], 'adlabels': [{'name': label}]})
            elif media['type'] == 'video':
                video_data = {'video_id': media['id'], 'adlabels': [{'name': label}]}
                # Proatividade: Tentar fornecer thumbnail se a mídia oposta for imagem
                other_media = stories_media if label == FEED_LABEL else feed_media
                if other_media and other_media['type'] == 'image':
                    video_data['image_hash'] = other_media['hash']
                    self._log(f"🖼️ Thumbnail para vídeo {label} linkada à imagem do par.")
                # Se não tem imagem do par, usar thumb_hash extraída durante upload
                elif media.get('thumb_hash'):
                    video_data['image_hash'] = media['thumb_hash']
                    self._log(f"✅ Thumbnail auto-gerada usada para vídeo {label}.")
                
                videos.append(video_data)

        add_media(feed_media, FEED_LABEL)
        add_media(stories_media, STORY_LABEL)

        # Build customization rules
        feed_rule = {
            'customization_spec': {
                'publisher_platforms': ['facebook', 'instagram'],
                'facebook_positions': ['feed', 'marketplace', 'video_feeds', 'search'],
                'instagram_positions': ['stream', 'explore', 'profile_feed'],
            },
        }
        stories_rule = {
            'customization_spec': {
                'publisher_platforms': ['facebook', 'instagram'],
                'facebook_positions': ['story'],
                'instagram_positions': ['story', 'reels'],
            },
        }

        # Match label type to media type
        if feed_media['type'] == 'image':
            feed_rule['image_label'] = {'name': FEED_LABEL}
        else:
            feed_rule['video_label'] = {'name': FEED_LABEL}

        if stories_media['type'] == 'image':
            stories_rule['image_label'] = {'name': STORY_LABEL}
        else:
            stories_rule['video_label'] = {'name': STORY_LABEL}

        link_url_payload = {'website_url': link_url}
        if lead_gen_form_id:
            link_url_payload['lead_gen_form_id'] = lead_gen_form_id
            # Para Lead Ads, website_url deve ser a URL da página ou omitido em alguns contextos
            # Mas na Graph API v18+, website_url é aceito como destino opcional
            if '//' in link_url:
                link_url_payload['display_url'] = link_url.split('//')[1].split('/')[0]

        asset_feed_spec = {
            'bodies': to_text_list(bodies),
            'titles': to_text_list(titles),
            'call_to_action_types': [cta_type],
            'link_urls': [link_url_payload],
            'asset_customization_rules': [feed_rule, stories_rule],
        }
        
        # Adicionar descrições apenas se houver algo para não poluir
        asset_feed_spec['descriptions'] = [{'text': ' '}]

        # Definir formatos e mídias
        formats = []
        if images: 
            formats.append('SINGLE_IMAGE')
            asset_feed_spec['images'] = images
        if videos: 
            formats.append('SINGLE_VIDEO')
            asset_feed_spec['videos'] = videos
        
        # Se for misto, a Meta geralmente aceita o primeiro ou espera ambos no ad_formats
        asset_feed_spec['ad_formats'] = formats if formats else ['SINGLE_IMAGE']

        object_story_spec = {'page_id': page_id}
        if instagram_user_id:
            object_story_spec['instagram_user_id'] = instagram_user_id

        payload = {
            'asset_feed_spec': asset_feed_spec,
            'object_story_spec': object_story_spec,
            'degrees_of_freedom_spec': {
                'creative_features_spec': {
                    # Features individuais (standard_enhancements deprecated na API v22.0)
                    'image_template': {'enroll_status': 'OPT_OUT'},
                    'image_touchups': {'enroll_status': 'OPT_OUT'},
                    'text_optimizations': {'enroll_status': 'OPT_OUT'},
                    'inline_comment': {'enroll_status': 'OPT_OUT'},
                    'video_auto_crop': {'enroll_status': 'OPT_OUT'},
                },
            },
        }
        if url_tags:
            payload['url_tags'] = url_tags

        return payload

    # ======================== CRIAR AD (PAUSADO) ========================

    def create_ad(self, adset_id, creative_id, ad_name, pixel_id=None):
        """Cria um Ad via REST API direta. Status PAUSED."""
        self._log(f"📌 Criando Ad '{ad_name}' (status: PAUSED)...")

        def _do():
            import json
            url = f"https://graph.facebook.com/v18.0/{self.account_id}/ads"
            
            # creative_id DEVE ser número (int), não string
            try:
                cid = int(creative_id)
            except (ValueError, TypeError):
                cid = creative_id

            post_data = {
                'access_token': self.access_token,
                'name': ad_name,
                'adset_id': adset_id,
                'creative': json.dumps({'creative_id': cid}),
                'status': 'PAUSED',
            }

            if pixel_id:
                post_data['tracking_specs'] = json.dumps([
                    {'action.type': 'offsite_conversion', 'fb_pixel': [pixel_id]}
                ])

            self._log(f"   → adset_id={adset_id}, creative_id={cid}, pixel_id={pixel_id}")
            
            # Log completo dos params (sem access_token)
            debug_data = {k: v for k, v in post_data.items() if k != 'access_token'}
            print(f"🔍 [create_ad] Params enviados: {json.dumps(debug_data, indent=2)}")
            
            resp = requests.post(url, data=post_data).json()
            print(f"🔍 [create_ad] Resposta completa: {json.dumps(resp, indent=2, ensure_ascii=False)}")

            if 'error' in resp:
                error = resp['error']
                error_detail = (
                    f"Code: {error.get('code')}, "
                    f"SubCode: {error.get('error_subcode', 'N/A')}, "
                    f"Message: {error.get('message', '?')}, "
                    f"Type: {error.get('type', '?')}, "
                    f"UserMsg: {error.get('error_user_msg', 'N/A')}"
                )
                self._log(f"❌ [create_ad] {error_detail}")
                raise Exception(f"Ad falhou: {error.get('message', 'Unknown')}")

            ad_id = resp.get('id')
            if not ad_id:
                raise Exception(f"Ad criado mas sem ID: {resp}")
            return ad_id

        ad_id = self._with_retry(f"Criar Ad '{ad_name}'", _do)
        self._log(f"✅ Ad '{ad_name}' criado com sucesso (ID: {ad_id}) — PAUSADO")
        return ad_id

    # ======================== DUPLICAR AD SET ========================

    def duplicate_adset(self, source_adset_id, new_name=None):
        """Duplica um Ad Set existente. Retorna o ID do novo Ad Set."""
        self._log(f"📋 Duplicando Ad Set {source_adset_id}...")

        def _do():
            source = AdSet(source_adset_id)
            result = source.create_copy(params={
                'deep_copy': False,
                'status_option': 'PAUSED',
                'rename_options': {
                    'rename_suffix': f' - Cópia {int(time.time())}',
                } if not new_name else {},
            })
            # result returns the copied adset data
            copied_id = result.get('copied_adset_id') or result.get('id')
            return copied_id

        adset_id = self._with_retry(f"Duplicar Ad Set", _do)
        self._log(f"✅ Ad Set duplicado (novo ID: {adset_id})")
        return adset_id

    # ======================== PROCESSAR FILA ========================

    def process_queue(self, queue_items, global_config, adset_id):
        """
        Processa a fila completa de uploads.

        queue_items: lista de dicts com {ad_name, feed_file_path, stories_file_path}
        global_config: dict com {url, utms, cta, textos, titulos, page_id}
        adset_id: ID do conjunto de anúncios destino

        Retorna lista de resultados: [{ad_name, success, ad_id, error}]
        """
        results = []
        total = len(queue_items)
        self._log(f"🚀 Lote iniciado — {total} anúncio(s) na fila")

        for i, item in enumerate(queue_items, 1):
            ad_name = item.get('ad_name', f'Ad {i}')
            self._log(f"\n{'='*40}")
            self._log(f"📦 Processando {i}/{total}: \"{ad_name}\"")

            try:
                # Upload feed media
                feed_media = None
                if item.get('feed_file_path'):
                    feed_media = self.upload_media(item['feed_file_path'])
                    self.smart_delay()

                # Upload stories media
                stories_media = None
                if item.get('stories_file_path'):
                    stories_media = self.upload_media(item['stories_file_path'])
                    self.smart_delay()

                # Create creative
                creative_id = self.create_creative_with_placements(
                    page_id=global_config.get('page_id'),
                    feed_media=feed_media,
                    stories_media=stories_media,
                    link_url=global_config['url'],
                    primary_texts=global_config.get('textos', []),
                    headlines=global_config.get('titulos', []),
                    cta_type=global_config.get('cta', 'LEARN_MORE'),
                    instagram_user_id=global_config.get('instagram_actor_id'), # Mapeia do front que ainda envia este nome
                    url_tags=global_config.get('utms', ''),
                    lead_gen_form_id=global_config.get('lead_gen_form_id'),
                )
                self.smart_delay()

                # Create ad (PAUSED)
                ad_id = self.create_ad(adset_id, creative_id, ad_name, pixel_id=global_config.get('pixel_id'))

                results.append({
                    'ad_name': ad_name,
                    'success': True,
                    'ad_id': ad_id,
                    'error': None,
                })

            except Exception as e:
                self._log(f"❌ Erro fatal ao processar '{ad_name}': {str(e)[:200]}")
                results.append({
                    'ad_name': ad_name,
                    'success': False,
                    'ad_id': None,
                    'error': str(e),
                })

            # Delay between ads
            if i < total:
                self.smart_delay()

        # Summary
        ok = sum(1 for r in results if r['success'])
        fail = sum(1 for r in results if not r['success'])
        self._log(f"\n{'='*40}")
        self._log(f"🏁 Lote concluído: {ok} ✅ sucesso, {fail} ❌ erro(s)")

        return results
