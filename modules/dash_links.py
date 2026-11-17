"""
Links públicos com token para dashboards fora de dashboard_clients.

Tabela dash_links_publicos (nome PK, token). Nomes em uso:
  'faturamento'            → /dash/faturamento
  'lancamento:<slug>'      → /dash/lancamento/<slug>

Acesso = logado no sistema OU ?t= igual ao token do nome. Revogar um link é
um UPDATE do token. Os tokens nunca vão para o repositório (que é público).
"""

import hmac
import logging
import time

from flask import request, session

logger = logging.getLogger(__name__)

_cache = {}          # nome -> (token, monotonic)
_TTL = 60


def token_do_link(nome):
    agora = time.monotonic()
    hit = _cache.get(nome)
    if hit and agora - hit[1] < _TTL:
        return hit[0]
    from modules.database import fetch_one
    row = fetch_one("SELECT token FROM dash_links_publicos WHERE nome = %s", (nome,))
    token = row['token'] if row else None
    _cache[nome] = (token, agora)
    return token


def acesso_por_link(nome):
    """True se logado ou se ?t= confere com o token do link. Falha nega."""
    if session.get('user_id'):
        return True
    t = request.args.get('t', '').strip()
    if not t:
        return False
    try:
        esperado = token_do_link(nome)
    except Exception as e:
        logger.error(f'[dash_links] falha ao ler token de {nome}: {e}')
        return False
    return bool(esperado and hmac.compare_digest(t, esperado))


def url_com_token(url, nome):
    """URL com ?t= para o /dash admin mostrar o link pronto para enviar."""
    try:
        tok = token_do_link(nome)
    except Exception:
        tok = None
    return f'{url}?t={tok}' if tok else url
