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
    """ID de uma conta IG comercial acessível pelo token — é por ela que a
    consulta é feita. Descoberto uma vez e guardado por 6h.

    Ordem: IG_DISCOVERY_USER_ID (env) → páginas do usuário do token → IG ligado
    às contas de anúncio conhecidas (o mesmo caminho da dash de lançamento; o
    token atual não tem página com IG ligado, verificado em 2026-11-19).
    """
    import os
    agora = time.monotonic()
    if _ig_conta['id'] and agora - _ig_conta['at'] < _CACHE_TTL:
        return _ig_conta['id']

    ig = os.getenv('IG_DISCOVERY_USER_ID')

    if not ig:
        url = f'{GRAPH_BASE}/me/accounts'
        resp = requests.get(url, params={'fields': 'instagram_business_account', 'limit': 50,
                                         'access_token': token}, timeout=8)
        _log_api_call(url, resp.status_code, None)
        if resp.ok:
            for pagina in resp.json().get('data', []):
                ig = (pagina.get('instagram_business_account') or {}).get('id')
                if ig:
                    break

    if not ig:
        from modules.lancamento_dash import LANCAMENTOS
        contas = {c.get('ad_account_id') for c in LANCAMENTOS.values() if c.get('ad_account_id')}
        for conta in sorted(contas):
            url = f'{GRAPH_BASE}/act_{conta}/connected_instagram_accounts'
            resp = requests.get(url, params={'fields': 'id', 'limit': 5,
                                             'access_token': token}, timeout=8)
            _log_api_call(url, resp.status_code, None)
            if resp.ok and resp.json().get('data'):
                ig = resp.json()['data'][0].get('id')
                break

    if not ig:
        raise RuntimeError('nenhuma conta IG comercial acessível pelo token')
    _ig_conta.update(id=ig, at=agora)
    return ig


# Perfil comercial que sabidamente existe. Se nem ele é encontrado, a consulta
# não está funcionando (app sem acesso, conta IG errada) e "não encontrado"
# não quer dizer nada — aí a checagem se desliga sozinha, em vez de mostrar
# aviso para TODO lead. Medido em 2026-11-19: com o app atual, @natura voltava
# "não encontrado". Quando o acesso for liberado, volta a valer sem deploy.
_SONDA = 'instagram'
_sonda = {'ok': None, 'at': 0.0}


def _consulta_confiavel():
    agora = time.monotonic()
    if _sonda['ok'] is not None and agora - _sonda['at'] < _CACHE_TTL:
        return _sonda['ok']
    ok = _consultar(_SONDA) == 'ok'
    _sonda.update(ok=ok, at=agora)
    if not ok:
        logger.warning('[ig-check] sonda @%s não encontrada — checagem desligada por 6h', _SONDA)
    return ok


def checar(usuario):
    usuario = (usuario or '').strip().lstrip('@').lower()
    if not usuario_valido(usuario):
        return 'nao_encontrado'

    agora = time.monotonic()
    hit = _cache.get(usuario)
    if hit and agora - hit[1] < _CACHE_TTL:
        return hit[0]

    try:
        if not _consulta_confiavel():
            return 'indeterminado'
    except Exception as e:
        logger.warning('[ig-check] sonda falhou: %s', e)
        return 'indeterminado'

    status = _consultar(usuario)
    if status == 'indeterminado':
        return status                 # não guarda em cache: tenta de novo depois
    if len(_cache) >= _CACHE_MAX:
        _cache.clear()
    _cache[usuario] = (status, agora)
    return status


def _consultar(usuario):
    """Uma consulta business_discovery. 'ok' | 'nao_encontrado' | 'indeterminado'."""
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
        return 'indeterminado'
    return status
