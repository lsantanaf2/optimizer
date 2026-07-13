"""
Snapshot "última leitura boa" dos dashboards públicos (/dash).

Motivação: o dashboard do cliente é um link público que NÃO pode mostrar erro.
Quando a fonte (Meta API) falha — token inválido, rate limit, instabilidade —
servimos os dados da última carga bem-sucedida com um aviso de defasagem,
em vez de tela de erro.

Formato: os eventos SSE de uma carga bem-sucedida são salvos como lista de
strings (chunks 'data: {...}') em JSONB. O fallback simplesmente "replaya"
os mesmos eventos — o frontend renderiza como se fosse uma carga normal.

Tabela (criada por migração idempotente no app.py:ensure_db):
    dash_snapshots (slug, endpoint, period_key, events JSONB, updated_at)
"""

import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from modules.database import fetch_one, execute

logger = logging.getLogger(__name__)
_BR_TZ = ZoneInfo('America/Sao_Paulo')

# Não deixar snapshots gigantes derrubarem o banco (payloads de dash ~100-500KB)
MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024


def period_key(date_preset, since, until):
    """Chave estável do período consultado."""
    return f"{date_preset or ''}|{since or ''}|{until or ''}"


def save_snapshot(slug, endpoint, pkey, events):
    """Persiste os chunks SSE de uma carga bem-sucedida. Best-effort."""
    try:
        payload = json.dumps(events)
        if len(payload) > MAX_SNAPSHOT_BYTES:
            logger.warning(f'[snapshot] {slug}/{endpoint} muito grande ({len(payload)}b) — não salvo')
            return
        execute(
            """INSERT INTO dash_snapshots (slug, endpoint, period_key, events, updated_at)
               VALUES (%s, %s, %s, %s, NOW())
               ON CONFLICT (slug, endpoint, period_key)
               DO UPDATE SET events = EXCLUDED.events, updated_at = NOW()""",
            (slug, endpoint, pkey, payload)
        )
    except Exception as e:
        logger.warning(f'[snapshot] falha ao salvar {slug}/{endpoint}: {e}')


def load_snapshot(slug, endpoint, pkey):
    """Retorna (events, updated_at) da última carga boa, ou (None, None)."""
    try:
        row = fetch_one(
            """SELECT events, updated_at FROM dash_snapshots
               WHERE slug = %s AND endpoint = %s AND period_key = %s""",
            (slug, endpoint, pkey)
        )
        if not row:
            return None, None
        events = row['events']
        if isinstance(events, str):
            events = json.loads(events)
        return events, row['updated_at']
    except Exception as e:
        logger.warning(f'[snapshot] falha ao carregar {slug}/{endpoint}: {e}')
        return None, None


def stale_notice_chunk(updated_at):
    """Chunk SSE de aviso de dados defasados (stage 'stale').

    Frontends que conhecem o stage mostram banner; os que não conhecem
    simplesmente ignoram — o replay continua funcionando de qualquer forma.
    """
    try:
        when = updated_at.astimezone(_BR_TZ).strftime('%d/%m às %H:%M')
    except Exception:
        when = str(updated_at)[:16]
    payload = {
        'stage': 'stale',
        'message': f'Exibindo últimos dados disponíveis ({when}). '
                   f'A atualização ao vivo está temporariamente indisponível.',
        'as_of': str(updated_at),
    }
    return f"data: {json.dumps(payload)}\n\n"


def replay(events, updated_at):
    """Generator que emite o aviso de defasagem + os eventos salvos."""
    yield stale_notice_chunk(updated_at)
    for chunk in events:
        yield chunk
