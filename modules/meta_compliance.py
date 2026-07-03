"""
Meta Platform compliance — endpoints exigidos pelo Platform Terms / App Review.

1. POST /meta/data-deletion — Data Deletion Request Callback (Seção 3.d dos
   Platform Terms). A Meta chama este endpoint quando um usuário remove o app
   ou solicita exclusão de dados. Registrar a URL no App Dashboard em:
   Settings → Advanced → Data Deletion Request URL.

2. GET /meta/data-deletion-status — página de confirmação que a Meta exibe
   ao usuário (URL retornada pelo callback).

3. GET /privacy — Privacy Policy pública (inglês) exigida pelo App Review.
4. GET /terms — Terms of Service públicos.

Fluxo do callback (documentação Meta):
  - Recebe POST form com 'signed_request' = '<sig_b64url>.<payload_b64url>'
  - Valida HMAC-SHA256 do payload com o APP_SECRET
  - payload JSON contém 'user_id' (Meta user id do solicitante)
  - Deleta TODOS os Platform Data desse usuário no nosso banco
  - Responde JSON: {"url": "<status_url>", "confirmation_code": "<code>"}
"""

import base64
import hashlib
import hmac
import json
import logging
import os

from flask import Blueprint, jsonify, render_template, request

from modules.database import execute, fetch_one

logger = logging.getLogger(__name__)

compliance_bp = Blueprint('compliance', __name__)


def _parse_signed_request(signed_request, app_secret):
    """Valida e decodifica o signed_request da Meta.

    Retorna o payload dict, ou None se assinatura inválida/formato errado.
    """
    try:
        sig_b64, payload_b64 = signed_request.split('.', 1)

        def _b64d(s):
            return base64.urlsafe_b64decode(s + '=' * (-len(s) % 4))

        signature = _b64d(sig_b64)
        payload = json.loads(_b64d(payload_b64))

        expected = hmac.new(
            app_secret.encode(), payload_b64.encode(), hashlib.sha256
        ).digest()
        if not hmac.compare_digest(signature, expected):
            logger.warning('data-deletion: assinatura HMAC inválida')
            return None
        return payload
    except (ValueError, TypeError, json.JSONDecodeError) as e:
        logger.warning(f'data-deletion: signed_request malformado: {e}')
        return None


def _delete_platform_data(meta_user_id):
    """Deleta todos os Platform Data associados a um Meta user id.

    Retorna quantidade de linhas afetadas (para log/auditoria).
    """
    deleted = 0
    # Token de acesso (o Platform Data mais sensível)
    row = fetch_one(
        "SELECT user_id FROM user_meta_tokens WHERE meta_user_id = %s",
        (str(meta_user_id),)
    )
    deleted += execute(
        "DELETE FROM user_meta_tokens WHERE meta_user_id = %s",
        (str(meta_user_id),)
    )
    # Contas de anúncio importadas + settings (cascade via FK)
    if row:
        deleted += execute(
            "DELETE FROM imported_ad_accounts WHERE user_id = %s",
            (row['user_id'],)
        )
    return deleted


@compliance_bp.route('/meta/data-deletion', methods=['POST'])
def data_deletion_callback():
    """Callback de exclusão de dados chamado pela Meta."""
    app_secret = os.getenv('APP_SECRET')
    signed_request = request.form.get('signed_request', '')
    if not signed_request or not app_secret:
        return jsonify({'error': 'signed_request ausente'}), 400

    payload = _parse_signed_request(signed_request, app_secret)
    if not payload:
        return jsonify({'error': 'assinatura inválida'}), 400

    meta_user_id = str(payload.get('user_id', ''))
    if not meta_user_id:
        return jsonify({'error': 'user_id ausente no payload'}), 400

    try:
        deleted = _delete_platform_data(meta_user_id)
        logger.info(f'data-deletion: meta_user={meta_user_id} — {deleted} registros removidos')
    except Exception as e:
        logger.error(f'data-deletion: falha ao deletar dados de {meta_user_id}: {e}')
        return jsonify({'error': 'erro interno'}), 500

    # Código de confirmação determinístico (não expõe o user_id)
    confirmation_code = hashlib.sha256(f'del:{meta_user_id}'.encode()).hexdigest()[:16]
    status_url = request.url_root.rstrip('/') + f'/meta/data-deletion-status?code={confirmation_code}'

    return jsonify({'url': status_url, 'confirmation_code': confirmation_code})


@compliance_bp.route('/meta/data-deletion-status')
def data_deletion_status():
    """Página de confirmação exibida ao usuário pela Meta."""
    code = request.args.get('code', '')
    return render_template('data_deletion_status.html', code=code)


@compliance_bp.route('/privacy')
def privacy_policy():
    """Privacy Policy pública (inglês) — exigida pelo Meta App Review."""
    return render_template('privacy.html')


@compliance_bp.route('/terms')
def terms_of_service():
    """Terms of Service públicos — exigidos pelo Meta App Review."""
    return render_template('terms.html')
