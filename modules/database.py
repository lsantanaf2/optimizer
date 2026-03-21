"""
Módulo de conexão com Supabase PostgreSQL.
Usa pool de conexões para eficiência com Gunicorn (4 workers).
"""

import os
import logging
from contextlib import contextmanager
from psycopg2 import pool, extras

logger = logging.getLogger(__name__)

# Pool global — inicializado uma vez por worker
_pool = None


def init_db():
    """Inicializa o pool de conexões. Chamar uma vez no startup do app."""
    global _pool
    if _pool is not None:
        return

    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        logger.warning("DATABASE_URL não configurada — banco desabilitado")
        return

    try:
        _pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=db_url
        )
        logger.info("Pool de conexões PostgreSQL inicializado")
    except Exception as e:
        logger.error(f"Erro ao conectar no banco: {e}")
        _pool = None


def close_db():
    """Fecha o pool. Chamar no shutdown do app."""
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None


@contextmanager
def get_conn():
    """Context manager que pega e devolve conexão do pool."""
    if _pool is None:
        raise RuntimeError("Banco não inicializado. Chame init_db() primeiro.")
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


@contextmanager
def get_cursor(dict_cursor=True):
    """Context manager que retorna cursor pronto para queries.

    Uso:
        with get_cursor() as cur:
            cur.execute("SELECT * FROM app_users WHERE email = %s", (email,))
            user = cur.fetchone()
    """
    with get_conn() as conn:
        cursor_factory = extras.RealDictCursor if dict_cursor else None
        cur = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cur
        finally:
            cur.close()


# --- Helpers genéricos ---

def fetch_one(query, params=None):
    """Executa query e retorna um registro como dict (ou None)."""
    with get_cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()


def fetch_all(query, params=None):
    """Executa query e retorna lista de dicts."""
    with get_cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def execute(query, params=None):
    """Executa INSERT/UPDATE/DELETE e retorna rowcount."""
    with get_cursor() as cur:
        cur.execute(query, params)
        return cur.rowcount


def execute_returning(query, params=None):
    """Executa INSERT/UPDATE com RETURNING e retorna o registro."""
    with get_cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()
