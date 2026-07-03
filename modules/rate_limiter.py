"""
Rate limiter em memória para os endpoints públicos do /dash.

Motivação (P0.3 — conformidade Platform Terms 7.e.i.2): os dashboards são
links públicos (?t=<token>). Sem limite, um bot que descubra a URL — ou um
cliente com F5 compulsivo — transforma nosso servidor em amplificador de
carga contra a Meta API. O cache (meta_cache) absorve a maior parte, mas o
rate limit é a segunda camada: corta o abuso antes de qualquer processamento.

Estratégia: sliding window por chave, em memória (por worker Gunicorn — o
limite efetivo é ~4x o configurado no pior caso, ainda assim ordens de
magnitude abaixo de um abuso real).

Uso:
    from modules.rate_limiter import check_rate_limit
    check_rate_limit(f'dash:{slug}')   # aborta 429 se exceder
"""

import threading
import time
from collections import deque

from flask import abort, request

# Janela e limite padrão: 30 requests/min por (chave, IP).
# Um load do dashboard dispara ~4-6 requests de API — 30/min acomoda
# uso legítimo intenso (vários reloads) e bloqueia marteladas.
DEFAULT_MAX_REQUESTS = 30
DEFAULT_WINDOW_SECONDS = 60
MAX_KEYS = 2000  # limite defensivo de memória

_lock = threading.Lock()
_hits = {}  # key -> deque[timestamps monotonic]


def _client_ip():
    """IP real do cliente, considerando proxy reverso (nginx)."""
    fwd = request.headers.get('X-Forwarded-For', '')
    if fwd:
        return fwd.split(',')[0].strip()
    return request.remote_addr or 'unknown'


def is_allowed(key, max_requests=DEFAULT_MAX_REQUESTS, window=DEFAULT_WINDOW_SECONDS):
    """Registra o hit e retorna False se a chave excedeu o limite na janela."""
    now = time.monotonic()
    with _lock:
        if len(_hits) >= MAX_KEYS and key not in _hits:
            # Prune: descarta chaves sem hits recentes
            stale = [k for k, dq in _hits.items() if not dq or now - dq[-1] > window]
            for k in stale:
                _hits.pop(k, None)
        dq = _hits.setdefault(key, deque())
        # Remove hits fora da janela
        cutoff = now - window
        while dq and dq[0] < cutoff:
            dq.popleft()
        if len(dq) >= max_requests:
            return False
        dq.append(now)
        return True


def check_rate_limit(scope, max_requests=DEFAULT_MAX_REQUESTS, window=DEFAULT_WINDOW_SECONDS):
    """Aborta com 429 se o par (scope, IP do cliente) exceder o limite."""
    key = f'{scope}|{_client_ip()}'
    if not is_allowed(key, max_requests, window):
        abort(429, 'Muitas requisições. Aguarde um instante e tente novamente.')
