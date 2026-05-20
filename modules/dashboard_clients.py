"""
Módulo de gerenciamento de Dashboards multi-tenant.

Cada registro em `dashboard_clients` representa um dashboard isolado de cliente
(ex: VINCI, Sorveteiro Raiz, Bandog) com sua própria config de conectores
(Meta Ads, Google Ads, Google Sheets) e link público com token anti-enum.

Padrão: espelha modules/account_settings.py para consistência.
"""

import json
import logging
import secrets
from modules.database import fetch_one, fetch_all, execute, execute_returning

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _db_ok():
    """Verifica se o pool de banco está disponível."""
    try:
        from modules.database import _pool
        return _pool is not None
    except Exception:
        return False


def _row_to_dict(row):
    """Converte RealDictRow do psycopg2 em dict puro, normalizando JSON e arrays."""
    if not row:
        return None
    d = dict(row)
    # excluded_campaign_patterns vem como JSONB (lista já decodificada por psycopg2)
    if d.get('excluded_campaign_patterns') is None:
        d['excluded_campaign_patterns'] = []
    elif isinstance(d['excluded_campaign_patterns'], str):
        try:
            d['excluded_campaign_patterns'] = json.loads(d['excluded_campaign_patterns'])
        except Exception:
            d['excluded_campaign_patterns'] = []
    return d


def _generate_token(prefix='dsh'):
    """Gera token público anti-enum: {prefix}_{32-char-hex}."""
    return f"{prefix}_{secrets.token_hex(16)}"


# ─────────────────────────────────────────────────────────────
# Read operations
# ─────────────────────────────────────────────────────────────

def get_client(slug):
    """Retorna config completa de um cliente pelo slug, ou None se não existe."""
    if not _db_ok() or not slug:
        return None
    try:
        row = fetch_one(
            "SELECT * FROM dashboard_clients WHERE slug = %s",
            (slug,)
        )
        return _row_to_dict(row)
    except Exception as e:
        logger.error(f"[dashboard_clients] get_client({slug}) erro: {e}")
        return None


def get_client_by_token(token):
    """
    Resolve cliente pelo public_link_token (autenticação anti-enum).
    Retorna None se token inválido ou link desabilitado.
    """
    if not _db_ok() or not token:
        return None
    try:
        row = fetch_one(
            "SELECT * FROM dashboard_clients WHERE public_link_token = %s AND public_link_enabled = TRUE",
            (token,)
        )
        return _row_to_dict(row)
    except Exception as e:
        logger.error(f"[dashboard_clients] get_client_by_token erro: {e}")
        return None


def list_clients():
    """Lista todos os clientes (admin view). Ordenado por nome."""
    if not _db_ok():
        return []
    try:
        rows = fetch_all(
            "SELECT slug, name, display_name, meta_ad_account_id, "
            "google_ads_customer_id, google_ads_sheet_id, public_link_enabled, "
            "public_link_token, created_at, updated_at "
            "FROM dashboard_clients ORDER BY name"
        )
        return [_row_to_dict(r) for r in rows] if rows else []
    except Exception as e:
        logger.error(f"[dashboard_clients] list_clients erro: {e}")
        return []


# ─────────────────────────────────────────────────────────────
# Write operations
# ─────────────────────────────────────────────────────────────

def create_client(slug, name, meta_ad_account_id, **kwargs):
    """
    Cria um novo cliente. Campos obrigatórios: slug, name, meta_ad_account_id.

    Kwargs aceitos (todos opcionais):
      - display_name, meta_token_user_id, typeform_action_type
      - google_ads_customer_id, google_ads_user_id
      - google_ads_sheet_id, google_ads_sheet_gid, google_ads_filter_keyword
      - mqls_spreadsheet_id, locked_period
      - excluded_campaign_patterns (list)
      - public_link_token (gerado se não fornecido)
      - public_link_enabled (default True)

    Retorna o dict do cliente criado, ou None em erro.
    """
    if not _db_ok():
        logger.error("[dashboard_clients] create_client: banco indisponível")
        return None

    token = kwargs.get('public_link_token') or _generate_token()
    excluded = kwargs.get('excluded_campaign_patterns', [])
    if isinstance(excluded, list):
        excluded = json.dumps(excluded)

    try:
        row = execute_returning(
            """
            INSERT INTO dashboard_clients (
                slug, name, display_name,
                meta_ad_account_id, meta_token_user_id, typeform_action_type,
                google_ads_customer_id, google_ads_user_id,
                google_ads_sheet_id, google_ads_sheet_gid, google_ads_filter_keyword,
                mqls_spreadsheet_id, locked_period,
                excluded_campaign_patterns,
                public_link_enabled, public_link_token
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s::jsonb,
                %s, %s
            )
            RETURNING *
            """,
            (
                slug, name, kwargs.get('display_name'),
                meta_ad_account_id, kwargs.get('meta_token_user_id'),
                kwargs.get('typeform_action_type', 'offsite_conversion.fb_pixel_custom'),
                kwargs.get('google_ads_customer_id'), kwargs.get('google_ads_user_id'),
                kwargs.get('google_ads_sheet_id'), kwargs.get('google_ads_sheet_gid'),
                kwargs.get('google_ads_filter_keyword'),
                kwargs.get('mqls_spreadsheet_id'), kwargs.get('locked_period'),
                excluded,
                kwargs.get('public_link_enabled', True), token,
            )
        )
        return _row_to_dict(row)
    except Exception as e:
        logger.error(f"[dashboard_clients] create_client({slug}) erro: {e}")
        return None


def update_client(slug, **kwargs):
    """
    Atualiza campos do cliente. Aceita os mesmos kwargs de create_client (exceto slug).
    Retorna o dict atualizado ou None em erro.
    """
    if not _db_ok() or not slug:
        return None

    # Campos editáveis (slug é PK e não pode mudar)
    allowed = {
        'name', 'display_name',
        'meta_ad_account_id', 'meta_token_user_id', 'typeform_action_type',
        'google_ads_customer_id', 'google_ads_user_id',
        'google_ads_sheet_id', 'google_ads_sheet_gid', 'google_ads_filter_keyword',
        'mqls_spreadsheet_id', 'locked_period',
        'excluded_campaign_patterns',
        'public_link_enabled', 'public_link_token',
    }

    sets = []
    vals = []
    for k, v in kwargs.items():
        if k not in allowed:
            continue
        if k == 'excluded_campaign_patterns':
            if isinstance(v, list):
                v = json.dumps(v)
            sets.append(f"{k} = %s::jsonb")
        else:
            sets.append(f"{k} = %s")
        vals.append(v)

    if not sets:
        return get_client(slug)  # nada a atualizar — retorna estado atual

    sets.append("updated_at = NOW()")
    vals.append(slug)

    try:
        row = execute_returning(
            f"UPDATE dashboard_clients SET {', '.join(sets)} WHERE slug = %s RETURNING *",
            tuple(vals)
        )
        return _row_to_dict(row)
    except Exception as e:
        logger.error(f"[dashboard_clients] update_client({slug}) erro: {e}")
        return None


def rotate_token(slug):
    """Gera novo public_link_token (invalida link antigo). Útil se vazar."""
    new_token = _generate_token()
    return update_client(slug, public_link_token=new_token)


def delete_client(slug):
    """Remove um cliente. Retorna True se removeu, False caso contrário."""
    if not _db_ok() or not slug:
        return False
    try:
        execute("DELETE FROM dashboard_clients WHERE slug = %s", (slug,))
        return True
    except Exception as e:
        logger.error(f"[dashboard_clients] delete_client({slug}) erro: {e}")
        return False


# ─────────────────────────────────────────────────────────────
# Filtros excluídos — substitui FILTERS_FILE (cruzamento_filters.json)
# ─────────────────────────────────────────────────────────────

def get_excluded_patterns(slug):
    """Retorna lista de padrões de campanha excluídos do dashboard do cliente."""
    client = get_client(slug)
    if not client:
        return []
    return client.get('excluded_campaign_patterns', []) or []


def save_excluded_patterns(slug, patterns):
    """Salva lista de padrões. Retorna (success: bool, patterns: list)."""
    clean = [str(p).strip() for p in (patterns or []) if str(p).strip()]
    updated = update_client(slug, excluded_campaign_patterns=clean)
    if updated:
        return True, clean
    return False, []
