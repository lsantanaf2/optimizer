"""
Módulo de conexão com Supabase PostgreSQL.
Usa pool de conexões para eficiência com Gunicorn (4 workers).
"""

import os
import logging
from contextlib import contextmanager
from psycopg2 import pool, extras, OperationalError, InterfaceError

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
        # v2.11.1: timeouts + TCP keepalive para que um socket Postgres "stale"
        # (Supabase derruba conexões ociosas) ERRE rápido em vez de pendurar o
        # execute() pra sempre — era um dos vetores de travamento silencioso do
        # upload (a thread parava num socket morto, sem done e sem error).
        _pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=db_url,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
            # statement_timeout no servidor: aborta qualquer query > 30s
            options='-c statement_timeout=30000'
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
    """Context manager que pega e devolve conexão do pool.

    Resiliência a conexões 'stale': o pooler do Supabase (e um pause do projeto
    free-tier) derruba conexões ociosas, mas elas continuam no pool marcadas
    como vivas. Se a query falha com erro de conexão, a conexão é DESCARTADA do
    pool (close=True) em vez de devolvida morta — senão ela circula e derruba
    requests seguintes, causando 403/500 intermitentes. Os helpers
    (fetch_one/all/execute) fazem retry, pegando uma conexão nova/saudável.
    """
    if _pool is None:
        raise RuntimeError("Banco não inicializado. Chame init_db() primeiro.")
    conn = _pool.getconn()
    broken = False
    try:
        yield conn
        conn.commit()
    except (OperationalError, InterfaceError):
        broken = True  # conexão morta — não devolver ao pool
        raise
    except Exception:
        try:
            conn.rollback()
        except (OperationalError, InterfaceError):
            broken = True
        raise
    finally:
        try:
            _pool.putconn(conn, close=broken)
        except Exception:
            pass


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

def _with_conn_retry(op, attempts=3):
    """Executa op() com retry quando a conexão vem morta do pool (stale).

    Cada falha de conexão faz o get_conn descartar a conexão morta (close=True),
    então a próxima tentativa pega uma conexão nova/saudável. attempts=3 cobre
    até 2 conexões mortas antes de encontrar/criar uma viva.
    """
    last_exc = None
    for attempt in range(attempts):
        try:
            return op()
        except (OperationalError, InterfaceError) as e:
            last_exc = e
            continue  # conexão morta foi descartada; tenta com outra
    raise last_exc


def fetch_one(query, params=None):
    """Executa query e retorna um registro como dict (ou None)."""
    def _op():
        with get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()
    return _with_conn_retry(_op)


def fetch_all(query, params=None):
    """Executa query e retorna lista de dicts."""
    def _op():
        with get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
    return _with_conn_retry(_op)


def execute(query, params=None):
    """Executa INSERT/UPDATE/DELETE e retorna rowcount."""
    def _op():
        with get_cursor() as cur:
            cur.execute(query, params)
            return cur.rowcount
    return _with_conn_retry(_op)


def execute_returning(query, params=None):
    """Executa INSERT/UPDATE com RETURNING e retorna o registro."""
    def _op():
        with get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()
    return _with_conn_retry(_op)
