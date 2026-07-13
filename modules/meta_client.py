"""
Meta Graph API client central — ponto único de passagem para TODAS as leituras
da Graph API (insights, status, estruturas).

Motivação: conformidade com o Meta Platform Terms 7.e.i.2 ("harming the Platform").
A conta de desenvolvedor anterior foi desativada após consultas históricas em
volume/ritmo atípicos. Este módulo garante que nenhum caller consiga metralhar
a API, independente de quantas threads estejam ativas (SSE dos dashboards roda
período atual + anterior + top ads em paralelo via ThreadPoolExecutor).

Camadas de proteção:
  1. Throttle global thread-safe — intervalo mínimo entre chamadas por worker
  2. Leitura dos headers x-business-use-case-usage / x-app-usage em toda resposta
     → uso >= 75%: delay extra progressivo; uso >= 90%: pausa longa global
  3. Backoff exponencial nos códigos de rate limit (4, 17, 32, 613, 80xxx)
  4. Delay entre páginas de cursor embutido (toda página passa pelo throttle)

Uso:
    from modules.meta_client import meta_get, meta_get_paginated

    body = meta_get(url, params)                    # 1 request (com paging manual)
    items = meta_get_paginated(url, params)         # segue paging.next, retorna data[]
"""

import json
import time
import threading
from datetime import datetime, timedelta

import requests

# ── Versão da Graph API (ponto ÚNICO de mudança para upgrades) ───────────────
# Atual: v22.0 (jan/2025, suportada). Última disponível: v25.0 (fev/2026).
# Para fazer upgrade: mudar APENAS esta linha e testar.
GRAPH_API_VERSION = 'v22.0'
GRAPH_BASE = f'https://graph.facebook.com/{GRAPH_API_VERSION}'

# ── Configuração ──────────────────────────────────────────────────────────────

MIN_INTERVAL = 0.5          # segundos mínimos entre chamadas (por worker Gunicorn)
USAGE_SOFT_THRESHOLD = 75   # % de uso que ativa delay extra
USAGE_HARD_THRESHOLD = 90   # % de uso que ativa pausa longa
SOFT_EXTRA_DELAY = 2.0      # segundos extras quando uso >= soft
HARD_PAUSE_SECONDS = 60     # pausa global quando uso >= hard
MAX_RETRIES = 3             # tentativas em erro de rate limit
BACKOFF_BASE = 15           # 15s → 30s → 60s

# Códigos de erro de rate limit da Meta:
# 4 = app-level, 17 = user-level, 32 = page-level, 613 = custom throttle,
# 80000/80003/80004 = business use case (ads insights / ads management)
RATE_LIMIT_ERROR_CODES = {4, 17, 32, 613, 80000, 80001, 80002, 80003, 80004}

# ── Estado global (por processo/worker) ──────────────────────────────────────

_lock = threading.Lock()
_last_call_at = 0.0     # timestamp da última chamada
_paused_until = 0.0     # pausa global (uso >= hard threshold)
_extra_delay = 0.0      # delay adicional dinâmico (uso >= soft threshold)


def _max_usage_pct(headers):
    """Extrai o maior percentual de uso dos headers de rate limit da Meta.

    x-business-use-case-usage: {"<acct_id>": [{"call_count": N, "total_cputime": N,
                                               "total_time": N, ...}]}
    x-app-usage: {"call_count": N, "total_time": N, "total_cputime": N}
    Retorna 0 se headers ausentes/ilegíveis (nunca levanta exceção).
    """
    max_pct = 0
    try:
        buc = headers.get('x-business-use-case-usage')
        if buc:
            for entries in (json.loads(buc) or {}).values():
                for entry in entries or []:
                    for key in ('call_count', 'total_cputime', 'total_time'):
                        max_pct = max(max_pct, int(entry.get(key, 0) or 0))
    except (ValueError, TypeError, AttributeError):
        pass
    try:
        app_usage = headers.get('x-app-usage')
        if app_usage:
            data = json.loads(app_usage) or {}
            for key in ('call_count', 'total_cputime', 'total_time'):
                max_pct = max(max_pct, int(data.get(key, 0) or 0))
    except (ValueError, TypeError, AttributeError):
        pass
    try:
        # Header específico do Insights API (recomendação oficial: monitorar
        # app_id_util_pct / acc_id_util_pct para frear ANTES do bloqueio)
        ins = headers.get('x-fb-ads-insights-throttle')
        if ins:
            data = json.loads(ins) or {}
            for key in ('app_id_util_pct', 'acc_id_util_pct'):
                max_pct = max(max_pct, int(float(data.get(key, 0) or 0)))
    except (ValueError, TypeError, AttributeError):
        pass
    return max_pct


def _throttle():
    """Aguarda o slot de chamada respeitando intervalo mínimo, delay extra
    dinâmico e pausa global. Thread-safe: o lock serializa a reserva do slot."""
    global _last_call_at
    while True:
        with _lock:
            now = time.monotonic()
            if now < _paused_until:
                sleep_for = min(_paused_until - now, 5.0)
            else:
                wait = (_last_call_at + MIN_INTERVAL + _extra_delay) - now
                if wait <= 0:
                    _last_call_at = now
                    return
                sleep_for = min(wait, 1.0)
        # dorme FORA do lock para não bloquear outras threads
        time.sleep(max(sleep_for, 0.05))


def _update_usage_state(headers):
    """Ajusta delay extra / pausa global conforme os headers da resposta."""
    global _extra_delay, _paused_until
    pct = _max_usage_pct(headers)
    with _lock:
        if pct >= USAGE_HARD_THRESHOLD:
            _paused_until = time.monotonic() + HARD_PAUSE_SECONDS
            _extra_delay = SOFT_EXTRA_DELAY
            print(f"🛑 Meta API usage {pct}% — pausa global de {HARD_PAUSE_SECONDS}s")
        elif pct >= USAGE_SOFT_THRESHOLD:
            _extra_delay = SOFT_EXTRA_DELAY
            print(f"⚠️ Meta API usage {pct}% — delay extra de {SOFT_EXTRA_DELAY}s ativado")
        else:
            _extra_delay = 0.0
    return pct


def _log_api_call(url, status_code, usage_pct):
    """Registra a chamada em api_call_logs (auditoria de conformidade).

    Loga apenas o PATH (nunca query string — contém access_token).
    Falha de log nunca afeta a chamada (best-effort).
    """
    try:
        from urllib.parse import urlparse
        endpoint = urlparse(url).path
        from modules.database import execute
        execute(
            "INSERT INTO api_call_logs (endpoint, response_code, usage_pct) VALUES (%s, %s, %s)",
            (endpoint, status_code, usage_pct)
        )
    except Exception:
        pass  # log é best-effort — nunca derruba a chamada


def _extract_error(resp):
    """Extrai (code, mensagem rica) do body de erro da Meta. Nunca levanta."""
    try:
        err = (resp.json() or {}).get('error', {}) or {}
        code = err.get('code')
        detail = err.get('error_user_msg') or err.get('message') or 'sem detalhe'
        msg = (
            f'Meta API: {detail} '
            f'(code={code}, subcode={err.get("error_subcode")}, '
            f'type={err.get("type")}, fbtrace={err.get("fbtrace_id")})'
        )
        return code, msg
    except ValueError:
        return None, f'Meta API: HTTP {resp.status_code} — {resp.text[:300]}'


def meta_get(url, params=None, *, timeout=30):
    """GET na Graph API com throttle global, monitor de usage e backoff.

    Levanta Exception com mensagem rica em erro não recuperável.
    Retorna o body JSON (dict) em sucesso.
    """
    for attempt in range(MAX_RETRIES + 1):
        _throttle()
        resp = requests.get(url, params=params, timeout=timeout)
        usage_pct = _update_usage_state(resp.headers)
        _log_api_call(url, resp.status_code, usage_pct)

        if resp.ok:
            return resp.json()

        code, msg = _extract_error(resp)
        is_rate_limit = code in RATE_LIMIT_ERROR_CODES or 'request limit' in msg.lower()
        if is_rate_limit and attempt < MAX_RETRIES:
            wait = BACKOFF_BASE * (2 ** attempt)  # 15s → 30s → 60s
            print(f"⏳ Rate limit Meta (code={code}) — backoff {wait}s "
                  f"(tentativa {attempt + 1}/{MAX_RETRIES})")
            time.sleep(wait)
            continue
        raise Exception(msg)


def _split_time_range(since_str, until_str, max_days=90):
    """Divide um período em blocos de até max_days dias.

    Retorna lista de tuplas (since, until) como strings YYYY-MM-DD.
    Se as datas forem inválidas, retorna o período original (sem split).
    """
    try:
        since = datetime.strptime(str(since_str)[:10], '%Y-%m-%d').date()
        until = datetime.strptime(str(until_str)[:10], '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return [(since_str, until_str)]
    if since > until:
        return [(since_str, until_str)]

    chunks = []
    cursor = since
    while cursor <= until:
        chunk_end = min(cursor + timedelta(days=max_days - 1), until)
        chunks.append((cursor.isoformat(), chunk_end.isoformat()))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def meta_get_insights_rows(url, params, *, timeout=30, chunk_days=90):
    """Busca insights com paginação E fragmentação temporal automática.

    Recomendação oficial da Meta ("break down the query into smaller queries
    by using filters like date range"): períodos longos numa única query
    síncrona geram carga alta — foi a causa raiz do bloqueio 7.e.i.2 da conta
    anterior. Se params['time_range'] cobre mais que chunk_days dias, divide
    em blocos sequenciais (o throttle global espaça cada um) e concatena as
    linhas. Seguro para callers que AGREGAM (somam) as linhas retornadas.
    """
    time_range_raw = (params or {}).get('time_range')
    if not time_range_raw:
        return meta_get_paginated(url, params, timeout=timeout)

    try:
        tr = json.loads(time_range_raw) if isinstance(time_range_raw, str) else dict(time_range_raw)
        chunks = _split_time_range(tr.get('since'), tr.get('until'), max_days=chunk_days)
    except (ValueError, TypeError):
        return meta_get_paginated(url, params, timeout=timeout)

    if len(chunks) <= 1:
        return meta_get_paginated(url, params, timeout=timeout)

    print(f"📆 meta_get_insights_rows: período longo dividido em {len(chunks)} blocos de até {chunk_days} dias")
    rows = []
    for since, until in chunks:
        chunk_params = dict(params)
        chunk_params['time_range'] = json.dumps({'since': since, 'until': until},
                                                separators=(',', ':'))
        rows.extend(meta_get_paginated(url, chunk_params, timeout=timeout))
    return rows


def meta_get_paginated(url, params=None, *, timeout=30, max_pages=None):
    """Segue paging.next e retorna a lista concatenada de data[].

    Cada página passa pelo throttle global (delay entre páginas garantido).
    max_pages: limite defensivo opcional de páginas (None = sem limite).
    """
    results = []
    next_url = url
    cur_params = params
    pages = 0
    while next_url:
        body = meta_get(next_url, cur_params, timeout=timeout)
        results.extend(body.get('data', []))
        next_url = (body.get('paging', {}) or {}).get('next')
        cur_params = None  # next já embute os params
        pages += 1
        if max_pages and pages >= max_pages:
            print(f"⚠️ meta_get_paginated: limite de {max_pages} páginas atingido — truncando")
            break
    return results
