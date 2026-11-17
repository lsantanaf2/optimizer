"""
Módulo de download de vídeos do Instagram via yt-dlp.
Blueprint: instagram_dl_bp
"""

import os
import shutil
import tempfile
import re
import glob
from flask import (Blueprint, render_template, request, jsonify, send_file,
                   session, redirect, url_for, after_this_request)

instagram_dl_bp = Blueprint('instagram_dl', __name__)

# Teto de tamanho e de duração: sem eles, um link de live de horas enche o
# disco da VPS e prende um worker por todo o download.
MAX_BYTES = 300 * 1024 * 1024
MAX_SEGUNDOS = 15 * 60


@instagram_dl_bp.before_request
def _exigir_login():
    """Era aberto para qualquer um: cada pedido roda yt-dlp + ffmpeg na VPS."""
    if session.get('user_id'):
        return None
    if request.path.startswith('/api/'):
        return jsonify({'error': 'não autenticado'}), 401
    return redirect(url_for('auth.login_page'))


@instagram_dl_bp.route('/tools/instagram-downloader')
def instagram_downloader_page():
    """Página principal do downloader."""
    return render_template('instagram_downloader.html')


@instagram_dl_bp.route('/api/instagram/download', methods=['POST'])
def instagram_download():
    """Recebe URL do Instagram, baixa o vídeo e retorna o arquivo."""
    import yt_dlp
    from modules.rate_limiter import check_rate_limit
    check_rate_limit('instagram-dl', 10, 600)

    data = request.get_json()
    url = (data or {}).get('url', '').strip()

    if not url:
        return jsonify({'error': 'URL não informada'}), 400

    # Validação básica do domínio
    if not re.match(r'https?://(www\.)?(instagram\.com|instagr\.am)/', url):
        return jsonify({'error': 'URL inválida. Use um link do Instagram.'}), 400

    tmpdir = tempfile.mkdtemp()
    output_template = os.path.join(tmpdir, 'video_%(id)s.%(ext)s')

    # A pasta temporária ficava para sempre (um vídeo inteiro por pedido).
    # Apaga depois que a resposta sai — inclusive em erro.
    @after_this_request
    def _limpar(resposta):
        resposta.call_on_close(lambda: shutil.rmtree(tmpdir, ignore_errors=True))
        return resposta

    def _filtro(info, *, incomplete):
        dur = info.get('duration')
        if dur and dur > MAX_SEGUNDOS:
            return 'Vídeo longo demais para baixar por aqui.'
        return None

    ydl_opts = {
        'format': 'bestvideo+bestaudio/best',
        'merge_output_format': 'mp4',
        'outtmpl': output_template,
        'quiet': True,
        'no_warnings': True,
        'max_filesize': MAX_BYTES,
        'match_filter': _filtro,
        'postprocessors': [{
            'key': 'FFmpegVideoConvertor',
            'preferedformat': 'mp4',
        }],
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)

        # Encontrar o arquivo baixado
        files = glob.glob(os.path.join(tmpdir, 'video_*'))
        if not files:
            return jsonify({'error': 'Download falhou — vídeo indisponível, '
                                     'longo demais ou acima de 300 MB.'}), 500

        filepath = files[0]
        title = info.get('title', 'instagram_video') if info else 'instagram_video'
        # Sanitizar nome do arquivo
        safe_title = re.sub(r'[^\w\s\-]', '', title)[:60].strip() or 'instagram_video'
        filename = f"{safe_title}.mp4"

        return send_file(
            filepath,
            as_attachment=True,
            download_name=filename,
            mimetype='video/mp4'
        )

    except Exception as e:
        error_msg = str(e)
        # Mensagens amigáveis para erros comuns
        if 'login' in error_msg.lower() or 'authentication' in error_msg.lower():
            error_msg = 'Este vídeo requer login no Instagram. Tente um vídeo público.'
        elif 'not a video' in error_msg.lower() or 'Unsupported URL' in error_msg.lower():
            error_msg = 'Este link não contém um vídeo disponível para download.'
        return jsonify({'error': error_msg}), 500
