"""
Módulo de download de vídeos do Instagram via yt-dlp.
Blueprint: instagram_dl_bp
"""

import os
import tempfile
import re
import glob
from flask import Blueprint, render_template, request, jsonify, send_file

instagram_dl_bp = Blueprint('instagram_dl', __name__)


@instagram_dl_bp.route('/tools/instagram-downloader')
def instagram_downloader_page():
    """Página principal do downloader."""
    return render_template('instagram_downloader.html')


@instagram_dl_bp.route('/api/instagram/download', methods=['POST'])
def instagram_download():
    """Recebe URL do Instagram, baixa o vídeo e retorna o arquivo."""
    import yt_dlp

    data = request.get_json()
    url = (data or {}).get('url', '').strip()

    if not url:
        return jsonify({'error': 'URL não informada'}), 400

    # Validação básica do domínio
    if not re.match(r'https?://(www\.)?(instagram\.com|instagr\.am)/', url):
        return jsonify({'error': 'URL inválida. Use um link do Instagram.'}), 400

    tmpdir = tempfile.mkdtemp()
    output_template = os.path.join(tmpdir, 'video_%(id)s.%(ext)s')

    ydl_opts = {
        'format': 'bestvideo[height<=1080]+bestaudio/best[height<=1080]/best',
        'merge_output_format': 'mp4',
        'outtmpl': output_template,
        'quiet': True,
        'no_warnings': True,
        # Cookies do browser podem ajudar com login-walls
        # 'cookiesfrombrowser': ('chrome',),
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)

        # Encontrar o arquivo baixado
        files = glob.glob(os.path.join(tmpdir, 'video_*'))
        if not files:
            return jsonify({'error': 'Download falhou — nenhum arquivo gerado'}), 500

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
