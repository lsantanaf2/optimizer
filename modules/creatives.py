import json
import time
import tempfile
import traceback
from flask import Blueprint, request, jsonify, session, Response, stream_with_context, render_template

from app import obter_token, inicializar_api
from meta_api import list_drive_folder

creatives_bp = Blueprint('creatives', __name__)

@creatives_bp.route('/api/drive/list')
def api_drive_list_folder():
    folder_url = request.args.get('folder_url')
    if not folder_url:
        return jsonify({"error": "Parâmetro folder_url ausente"}), 400

    try:
        if "drive.google.com" not in folder_url:
            return jsonify({"error": "A URL precisa ser do Google Drive"}), 400

        files = list_drive_folder(folder_url)

        # Retorna também copies vazias pra manter compatibilidade do front_end antigo
        images = []
        videos = []
        for file in files:
            t = file['mimeType']
            if t.startswith('image/'):
                images.append(file)
            elif t.startswith('video/'):
                videos.append(file)

        # Regras de pareamento (o mesmo do front-end)
        pairs = []
        def get_basename(name):
            if '.' in name:
                return name.rsplit('.', 1)[0].lower()
            return name.lower()

        unmatched_videos = []
        for v in videos:
            v_base = get_basename(v['name'])
            match = None
            for i in images:
                i_base = get_basename(i['name'])
                if v_base == i_base:
                    match = i
                    break
            
            if match:
                pairs.append({'video': v, 'image': match})
                images.remove(match) 
            else:
                unmatched_videos.append(v)
        
        return jsonify({
            "status": "success",
            "pairs": pairs,
            "unmatched_videos": unmatched_videos,
            "unmatched_images": images
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@creatives_bp.route('/historico/<campaign_id>')
def historico_textos(campaign_id):
    """
    Busca criativos recentes da campanha e retorna URLs, UTMs, textos e títulos únicos.
    """
    token = obter_token()
    if not token:
        return jsonify({"error": "Não autenticado"}), 401

    uploader = inicializar_api(token)
    if not uploader:
        return jsonify({"error": "Falha na inicialização da API"}), 500

    try:
        from facebook_business.adobjects.campaign import Campaign
        camp = Campaign(campaign_id)
        
        # Pega Ad Sets da campanha
        adsets = camp.get_ad_sets(fields=['id'])
        if not adsets:
            return jsonify({
                "urls": [],
                "utms": [],
                "bodies": [],
                "titles": []
            })
            
        recent_adset_id = adsets[0]['id']
        
        from facebook_business.adobjects.adset import AdSet
        ads = AdSet(recent_adset_id).get_ads(fields=['creative{id,object_story_spec,url_tags,call_to_action_type}'])
        
        urls = set()
        utms = set()
        bodies = set()
        titles = set()
        
        for ad in ads:
            creative = ad.get('creative', {})
            try:
                story_spec = creative.get('object_story_spec', {})
                link_data = story_spec.get('video_data') or story_spec.get('link_data')
                
                if link_data:
                    url = link_data.get('call_to_action', {}).get('value', {}).get('link')
                    if url: urls.add(url)
                    
                    msg = link_data.get('message')
                    if msg: bodies.add(msg)
                    
                    tit = link_data.get('title')
                    if tit: titles.add(tit)
                    
                url_tags = creative.get('url_tags')
                if url_tags: utms.add(url_tags)
                
            except Exception as e:
                print(f"Aviso: Erro ao ler criativo: {e}")
                continue

        return jsonify({
            "urls": list(urls),
            "utms": list(utms),
            "bodies": list(bodies),
            "titles": list(titles)
        })

    except Exception as e:
        print(f"Erro ao buscar histórico: {e}")
        return jsonify({"error": str(e)}), 500


@creatives_bp.route('/upload/<campaign_id>', methods=['POST'])
def upload_single(campaign_id):
    """
    Upload de um único anúncio. Chamado pelo frontend para cada item da fila.
    """
    token = obter_token()
    if not token:
        return jsonify({"error": "Não autenticado"}), 401

    uploader = inicializar_api(token)
    if not uploader:
        return jsonify({"error": "Falha na inicialização da API"}), 500

    try:
        data = request.json
        print("\n\n=== INICIANDO NOVO UPLOAD ===")
        print(f"Recebido payload para {data.get('name')}:")
        print(json.dumps(data, indent=2))

        # Configura callback para enviar logs via SSE
        def log_callback(msg):
            print(f"[API] {msg}")

        uploader.set_callback(log_callback)

        # Extrai parâmetros
        item = data # O JSON inteiro foi passado do frontend (era queue[i] antes)
        
        page_id = item.get('pageId')
        ig_id = item.get('igId')
        pixel_id = item.get('pixelId')
        
        # Pega as mídias - O frontend passa links do Drive
        feed_media = item.get('feedMedia') # {id, name, link}
        story_media = item.get('storyMedia')
        
        name = item.get('name')
        text = item.get('text', '')
        title = item.get('title', '')
        link_url = item.get('linkUrl')
        url_tags = item.get('urlTags', '')
        adset_id = item.get('adsetId')
        cta_type = item.get('ctaType', 'LEARN_MORE')
        leadgen_form_id = item.get('leadgenFormId')  # Novo: ID do form
        
        # Modificação para Formulários de Lead
        if leadgen_form_id:
            logger_msg = f"Iniciando criação (LEADGEN): {name}"
            # Quando for leadgen, ignoramos linkUrl e urlTags
            parsed_url = None
            url_tags = None
        else:
            logger_msg = f"Iniciando criação (CONVERSION): {name}"
            parsed_url = link_url
            
        uploader._log(logger_msg)
            
        print(f"Extraindo links do Google Drive...")
        
        # Configurar feed_media (obrigatório)
        extracted_feed = None
        if feed_media:
            video_link = feed_media.get('link')
            image_link = None
            is_video = 'video' in feed_media.get('mimeType', '')
            if not is_video:
                image_link = video_link
                video_link = None
                
            extracted_feed = {
                'url': video_link or image_link,
                'is_video': is_video
            }
            
        # Configurar story_media (opcional)
        extracted_stories = None
        if story_media:
            s_video = story_media.get('link')
            s_image = None
            s_is_video = 'video' in story_media.get('mimeType', '')
            if not s_is_video:
                s_image = s_video
                s_video = None
                
            extracted_stories = {
                'url': s_video or s_image,
                'is_video': s_is_video
            }
            
        print(f"Feed preparado: Video={bool(extracted_feed and extracted_feed.get('is_video'))}, Image={bool(extracted_feed and not extracted_feed.get('is_video'))}")

        # Cria o anúncio!
        ad_id = uploader.create_ad(
            adset_id=adset_id,
            page_id=page_id,
            name=name,
            feed_media=extracted_feed,
            stories_media=extracted_stories,
            link_url=parsed_url,
            bodies=[text] if text else [],
            titles=[title] if title else [],
            cta_type=cta_type,
            url_tags=url_tags,
            ig_id=ig_id,
            pixel_id=pixel_id,
            leadgen_form_id=leadgen_form_id
        )

        uploader._log(f"✅ Anúncio Criado. ID: {ad_id}")
        return jsonify({"status": "success", "id": ad_id})

    except Exception as e:
        error_msg = str(e)
        import traceback
        traceback.print_exc()
        if hasattr(e, 'api_error_message'):
            error_msg = e.api_error_message()
            
        if uploader:
            uploader._log(f"Erro fatal: {error_msg}")
            
        return jsonify({"error": error_msg}), 500


@creatives_bp.route('/duplicate_adset', methods=['POST'])
def duplicate_adset_route():
    """
    Duplica um Ad Set 1x. Retorna o ID do novo. Chamado ANTES do loop de criativos.
    """
    token = obter_token()
    if not token:
        return jsonify({"error": "Não autenticado"}), 401
        
    try:
        data = request.json
        adset_id = data.get('adset_id')
        new_name = data.get('new_name')
        
        if not adset_id:
            return jsonify({"error": "adset_id não fornecido"}), 400
            
        from facebook_business.adobjects.adset import AdSet
        
        print(f"Duplicando AdSet {adset_id}...")
        
        original_adset = AdSet(adset_id)
        
        # Meta usa o endpoint de cópia
        params = {
            'deep_copy': False, # Não copia os anúncios, apenas o AdSet em si
        }
        
        if new_name:
            params['rename_options'] = json.dumps({
                "rename_prefix": "",
                "rename_suffix": f" - {new_name}"
            })
            
        response = original_adset.api_post(
            endpoint='copies',
            params=params
        )
        
        copied_id = response.get('copied_adset_id')
        
        # Precisaremos garantir que esteja ATIVO, as vezes cria PAUSED
        try:
            from facebook_business.adobjects.adset import AdSet as A
            novo = A(copied_id)
            novo.api_update(params={'status': 'ACTIVE'})
        except Exception as st_err:
            print("Não conseguiu ativar de imediato", st_err)

        print(f"Novo AdSet criado: {copied_id}")
        return jsonify({"status": "success", "adset_id": copied_id})
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        error_msg = str(e)
        if hasattr(e, 'api_error_message'):
            error_msg = e.api_error_message()
        return jsonify({"error": error_msg}), 500
