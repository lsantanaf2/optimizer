"""
Confere se um @ do Instagram existe, para o formulário da LP de empresas.

Caminho oficial: Graph API `business_discovery`, com o token Meta da agência.
Ela só enxerga perfis comerciais ou de criador — @ pessoal e @ inexistente
voltam iguais ("não encontrado"). Por isso o resultado é um AVISO na LP, não
uma trava: o lead pode confirmar e enviar.

Resultados:
  'ok'              — perfil comercial/criador encontrado
  'nao_encontrado'  — a Meta respondeu que não acha esse usuário
  'indeterminado'   — qualquer outra coisa (sem token, limite, Meta fora):
                      nunca bloqueia o lead por problema nosso

Cache em memória (por worker): o mesmo @ não gera nova chamada por 6h, e o
número de chamadas fica na casa de uma por lead.
"""

import logging
import re
import time

import requests

from modules.meta_client import GRAPH_BASE, _log_api_call

logger = logging.getLogger(__name__)

USUARIO_RE = re.compile(r'^[a-z0-9._]{1,30}$')

_CACHE_TTL = 6 * 3600
_CACHE_MAX = 2000
_cache = {}                     # usuario -> (status, monotonic)
_ig_conta = {'id': None, 'at': 0.0}   # conta IG da agência usada na consulta

# Erro da Meta para usuário que o business_discovery não acha.
_NAO_ACHOU_SUBCODES = {2207013}


def usuario_valido(u):
    return bool(u) and bool(USUARIO_RE.fullmatch(u)) and '..' not in u \
        and not u.startswith('.') and not u.endswith('.')


def _token():
    # Mesmo token durável que as dashboards usam (o mais recente válido do
    # banco). Nunca sai do servidor.
    from app import _token_from_db
    return _token_from_db()


def _conta_ig(token):
    """ID de uma conta IG comercial ligada ao token — é por ela que a consulta
    é feita. Descoberto uma vez e guardado por 6h."""
    agora = time.monotonic()
    if _ig_conta['id'] and agora - _ig_conta['at'] < _CACHE_TTL:
        return _ig_conta['id']
    url = f'{GRAPH_BASE}/me/accounts'
    resp = requests.get(url, params={'fields': 'instagram_business_account', 'limit': 50,
                                     'access_token': token}, timeout=8)
    _log_api_call(url, resp.status_code, None)
    resp.raise_for_status()
    for pagina in resp.json().get('data', []):
        ig = (pagina.get('instagram_business_account') or {}).get('id')
        if ig:
            _ig_conta.update(id=ig, at=agora)
            return ig
    raise RuntimeError('nenhuma conta IG comercial ligada ao token')


def checar(usuario):
    usuario = (usuario or '').strip().lstrip('@').lower()
    if not usuario_valido(usuario):
        return 'nao_encontrado'

    agora = time.monotonic()
    hit = _cache.get(usuario)
    if hit and agora - hit[1] < _CACHE_TTL:
        return hit[0]

    status = 'indeterminado'
    try:
        token = _token()
        if not token:
            return status
        ig_id = _conta_ig(token)
        url = f'{GRAPH_BASE}/{ig_id}'
        resp = requests.get(url, params={
            'fields': f'business_discovery.username({usuario}){{username}}',
            'access_token': token,
        }, timeout=8)
        _log_api_call(url, resp.status_code, None)
        if resp.ok and (resp.json().get('business_discovery') or {}).get('username'):
            status = 'ok'
        else:
            err = (resp.json() or {}).get('error', {}) if resp.content else {}
            if err.get('error_subcode') in _NAO_ACHOU_SUBCODES or err.get('code') == 110:
                status = 'nao_encontrado'
            else:
                logger.warning('[ig-check] resposta inesperada para @%s: %s',
                               usuario, str(err)[:200])
    except Exception as e:
        logger.warning('[ig-check] falhou para @%s: %s', usuario, e)
        return 'indeterminado'        # não guarda em cache: tenta de novo depois

    if len(_cache) >= _CACHE_MAX:
        _cache.clear()
    _cache[usuario] = (status, agora)
    return status
