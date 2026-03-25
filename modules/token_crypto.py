"""
Criptografia simétrica para tokens de acesso Meta armazenados no banco.

Usa Fernet (AES-128-CBC + HMAC-SHA256) da biblioteca `cryptography`.
A chave é derivada do APP_SECRET via SHA-256 — sem dependência de env var extra.

Compatibilidade retroativa: se o valor armazenado não for um token Fernet válido
(tokens antigos em plaintext), retorna o valor como está e agenda re-criptografia.
"""

import os
import base64
import hashlib
import logging
from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)

_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    """Retorna instância Fernet singleton, inicializando na primeira chamada."""
    global _fernet
    if _fernet is None:
        app_secret = os.getenv('APP_SECRET', '')
        if not app_secret:
            raise RuntimeError('APP_SECRET não configurado — não é possível criptografar tokens.')
        # Deriva 32 bytes a partir do APP_SECRET e codifica em base64url (formato Fernet)
        key_bytes = hashlib.sha256(app_secret.encode()).digest()
        fernet_key = base64.urlsafe_b64encode(key_bytes)
        _fernet = Fernet(fernet_key)
    return _fernet


def encrypt_token(plaintext: str) -> str:
    """Criptografa um token de acesso e retorna a string cifrada (prefixo 'enc:')."""
    f = _get_fernet()
    encrypted = f.encrypt(plaintext.encode()).decode()
    return f'enc:{encrypted}'


def decrypt_token(stored: str) -> str:
    """
    Decifra um token armazenado.
    - Se tiver prefixo 'enc:' → decifra com Fernet.
    - Se não tiver prefixo → token legado em plaintext, retorna como está.
    """
    if not stored:
        return stored
    if stored.startswith('enc:'):
        f = _get_fernet()
        try:
            return f.decrypt(stored[4:].encode()).decode()
        except InvalidToken:
            logger.error('Falha ao decifrar token — possível chave incorreta ou dado corrompido.')
            raise
    # Token legado em plaintext — retorna direto (será re-criptografado no próximo login)
    return stored


def is_encrypted(stored: str) -> bool:
    """Verifica se o valor já está criptografado."""
    return bool(stored and stored.startswith('enc:'))
