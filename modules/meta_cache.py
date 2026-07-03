"""
Cache TTL em memória para leituras da Meta Graph API.

Motivação: os dashboards /dash/<slug> são links públicos — cada F5 do cliente
dispara um leque de chamadas à Meta (período atual + anterior + top ads em
paralelo). Sem cache, um cliente apertando F5 repetidamente reproduz o padrão
de "carga atípica" que derrubou a conta de desenvolvedor anterior (Platform
Terms 7.e.i.2). Com cache, requisições repetidas dentro do TTL custam ZERO
chamadas à Meta.

Estratégia de TTL:
  - Período que inclui HOJE (dados ainda mudando): TTL curto (10 min)
  - Período fechado no passado (dados imutáveis na Meta): TTL longo (6 h)

Cache por processo/worker Gunicorn (4 workers = até 4 misses por chave — ainda
assim reduz o volume em >90% no uso real). Sem dependência externa.

Uso:
    from modules.meta_cache import get_or_fetch, ttl_for_period

    ttl = ttl_for_period(until_str)          # 'YYYY-MM-DD' ou None
    data = get_or_fetch(('daily', account_id, since, until), ttl,
                        lambda: fetch_caro(...))
"""

import copy
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

TTL_LIVE = 600          # 10 min — período inclui hoje
TTL_CLOSED = 6 * 3600   # 6 h — período fechado (dados históricos imutáveis)
MAX_ENTRIES = 500       # limite defensivo de memória

_lock = threading.Lock()
_cache = {}  # key -> (expires_at_monotonic, value)


def ttl_for_period(until_str):
    """Retorna o TTL adequado conforme o fim do período.

    until_str: 'YYYY-MM-DD' ou None/inválido (assume período vivo → TTL curto).
    """
    if not until_str:
        return TTL_LIVE
    try:
        until = datetime.strptime(str(until_str)[:10], '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return TTL_LIVE
    today = datetime.now(ZoneInfo('America/Sao_Paulo')).date()
    return TTL_CLOSED if until < today else TTL_LIVE


def get_or_fetch(key, ttl, producer):
    """Retorna valor do cache se fresco; senão executa producer() e armazena.

    producer roda FORA do lock — duas threads simultâneas no mesmo miss podem
    buscar em duplicidade, mas o throttle do meta_client serializa o custo.

    SEMPRE retorna deepcopy: os callers mutam os dados retornados (ex:
    cruzamento faz fb_ads.extend(google_ads) e injeta ad_status nos dicts) —
    sem cópia, o cache seria poluído entre requests.
    """
    now = time.monotonic()
    with _lock:
        entry = _cache.get(key)
        if entry and entry[0] > now:
            return copy.deepcopy(entry[1])

    value = producer()

    with _lock:
        if len(_cache) >= MAX_ENTRIES:
            # Evict: remove as entradas mais próximas de expirar
            for old_key in sorted(_cache, key=lambda k: _cache[k][0])[:MAX_ENTRIES // 5]:
                _cache.pop(old_key, None)
        _cache[key] = (time.monotonic() + ttl, copy.deepcopy(value))
    return value


def invalidate(prefix=None):
    """Limpa o cache inteiro ou apenas chaves cujo primeiro elemento == prefix."""
    with _lock:
        if prefix is None:
            _cache.clear()
        else:
            for k in [k for k in _cache if isinstance(k, tuple) and k and k[0] == prefix]:
                _cache.pop(k, None)
