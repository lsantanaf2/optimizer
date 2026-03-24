"""
Módulo de configurações e histórico por conta de anúncios.
Gerencia: imported_ad_accounts, ad_account_settings, upload_history, visualization_modes
"""

import json
import logging
from modules.database import fetch_one, fetch_all, execute, execute_returning

logger = logging.getLogger(__name__)


def _db_ok():
    """Verifica se o pool está disponível sem importar o pool diretamente."""
    try:
        from modules.database import _pool
        return _pool is not None
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────
# Squad 1.3 — imported_ad_accounts
# ─────────────────────────────────────────────────────────────

def list_imported_accounts(user_id):
    """Retorna lista de contas importadas do usuário."""
    if not _db_ok():
        return []
    try:
        rows = fetch_all(
            "SELECT id, meta_account_id, account_name FROM imported_ad_accounts WHERE user_id = %s ORDER BY account_name",
            (user_id,)
        )
        return [dict(r) for r in rows] if rows else []
    except Exception as e:
        logger.error(f"[account_settings] Erro ao list_imported_accounts: {e}")
        return []


def get_or_create_imported_account(user_id, meta_account_id, account_name=None):
    """
    Retorna o UUID interno de uma conta importada, criando se necessário.
    Aceita meta_account_id com ou sem prefixo 'act_'.
    Retorna UUID string ou None se banco indisponível.
    """
    if not _db_ok():
        return None
    try:
        clean_id = str(meta_account_id).replace('act_', '')

        row = fetch_one(
            "SELECT id FROM imported_ad_accounts WHERE user_id = %s AND meta_account_id = %s",
            (user_id, clean_id)
        )
        if row:
            if account_name:
                execute(
                    "UPDATE imported_ad_accounts SET account_name = %s WHERE id = %s",
                    (account_name, row['id'])
                )
            return str(row['id'])

        new_row = execute_returning(
            """INSERT INTO imported_ad_accounts (user_id, meta_account_id, account_name)
               VALUES (%s, %s, %s) RETURNING id""",
            (user_id, clean_id, account_name or clean_id)
        )
        return str(new_row['id']) if new_row else None
    except Exception as e:
        logger.error(f"[account_settings] Erro ao upsert imported_account: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# Squad 2 — ad_account_settings
# ─────────────────────────────────────────────────────────────

def get_account_settings(imported_account_id):
    """
    Retorna {'saved_assets': {...}, 'cac_target_value': float|None}
    para um imported_account_id (UUID).
    Cria a linha se não existir. Retorna {} se banco indisponível.
    """
    if not _db_ok() or not imported_account_id:
        return {}
    try:
        row = fetch_one(
            "SELECT saved_assets, cac_target_value FROM ad_account_settings WHERE ad_account_id = %s",
            (imported_account_id,)
        )
        if not row:
            execute(
                "INSERT INTO ad_account_settings (ad_account_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (imported_account_id,)
            )
            return {}

        assets = row['saved_assets'] or {}
        if isinstance(assets, str):
            assets = json.loads(assets)

        return {
            'saved_assets': assets,
            'cac_target_value': float(row['cac_target_value']) if row['cac_target_value'] else None
        }
    except Exception as e:
        logger.error(f"[account_settings] Erro ao get_account_settings: {e}")
        return {}


def get_settings_for_setup(user_id, meta_account_id):
    """
    Atalho: dado user_id + meta_account_id, retorna settings completo.
    Seguro para chamar mesmo se banco estiver offline.
    """
    if not _db_ok():
        return {}
    try:
        imported_id = get_or_create_imported_account(user_id, meta_account_id)
        if not imported_id:
            return {}
        return get_account_settings(imported_id)
    except Exception as e:
        logger.error(f"[account_settings] Erro ao get_settings_for_setup: {e}")
        return {}


def _upsert_in_list(lst, key, value, extra=None):
    """
    Incrementa use_count se item já existe, ou adiciona novo.
    Retorna lista atualizada.
    """
    if not value:
        return lst or []
    lst = lst or []
    for entry in lst:
        if str(entry.get(key)) == str(value):
            entry['use_count'] = entry.get('use_count', 0) + 1
            return lst
    new_entry = {key: value, 'use_count': 1}
    if extra:
        new_entry.update(extra)
    lst.append(new_entry)
    return lst


def save_upload_assets(user_id, meta_account_id, upload_data):
    """
    Após upload bem-sucedido, atualiza saved_assets da conta.

    upload_data esperado:
    {
        'page_id': str,
        'instagram_id': str,
        'pixel_id': str,
        'primary_texts': [str, ...],
        'headlines': [str, ...],
        'url': str,
        'utm': str,
        'cta': str,
    }
    """
    if not _db_ok():
        return
    try:
        imported_id = get_or_create_imported_account(user_id, meta_account_id)
        if not imported_id:
            return

        execute(
            "INSERT INTO ad_account_settings (ad_account_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (imported_id,)
        )

        row = fetch_one(
            "SELECT saved_assets FROM ad_account_settings WHERE ad_account_id = %s",
            (imported_id,)
        )
        assets = (row['saved_assets'] or {}) if row else {}
        if isinstance(assets, str):
            assets = json.loads(assets)

        # Pages
        if upload_data.get('page_id'):
            assets['facebook_pages'] = _upsert_in_list(
                assets.get('facebook_pages', []), 'id', upload_data['page_id']
            )

        # Instagram
        if upload_data.get('instagram_id'):
            assets['instagram_profiles'] = _upsert_in_list(
                assets.get('instagram_profiles', []), 'id', upload_data['instagram_id']
            )

        # Pixel
        if upload_data.get('pixel_id'):
            assets['pixels'] = _upsert_in_list(
                assets.get('pixels', []), 'id', upload_data['pixel_id']
            )

        # Textos
        for text in (upload_data.get('primary_texts') or []):
            if text and text.strip():
                assets['primary_texts'] = _upsert_in_list(
                    assets.get('primary_texts', []), 'text', text.strip()
                )

        # Headlines
        for headline in (upload_data.get('headlines') or []):
            if headline and headline.strip():
                assets['headlines'] = _upsert_in_list(
                    assets.get('headlines', []), 'text', headline.strip()
                )

        # URL destino
        if upload_data.get('url'):
            assets['urls'] = _upsert_in_list(
                assets.get('urls', []), 'url', upload_data['url']
            )

        # UTM pattern
        if upload_data.get('utm'):
            assets['utms'] = _upsert_in_list(
                assets.get('utms', []), 'pattern', upload_data['utm']
            )

        # CTA padrão
        if upload_data.get('cta'):
            assets['default_cta'] = upload_data['cta']

        execute(
            "UPDATE ad_account_settings SET saved_assets = %s WHERE ad_account_id = %s",
            (json.dumps(assets, ensure_ascii=False), imported_id)
        )
    except Exception as e:
        logger.error(f"[account_settings] Erro ao save_upload_assets: {e}")


def save_single_asset(user_id, meta_account_id, asset_type, key_field, value, extra=None):
    """
    Salva/favorita um único item nos saved_assets.
    asset_type: 'facebook_pages', 'instagram_profiles', 'pixels', 'primary_texts', 'headlines', 'urls', 'utms'
    key_field: 'id', 'text', 'url', 'pattern' — depende do tipo
    value: valor do campo chave
    extra: dict com campos extras (ex: {'name': 'Minha Página'})
    """
    if not _db_ok() or not value:
        return False
    try:
        imported_id = get_or_create_imported_account(user_id, meta_account_id)
        if not imported_id:
            return False
        execute(
            "INSERT INTO ad_account_settings (ad_account_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (imported_id,)
        )
        row = fetch_one("SELECT saved_assets FROM ad_account_settings WHERE ad_account_id = %s", (imported_id,))
        assets = (row['saved_assets'] or {}) if row else {}
        if isinstance(assets, str):
            assets = json.loads(assets)

        assets[asset_type] = _upsert_in_list(assets.get(asset_type, []), key_field, value, extra)

        execute(
            "UPDATE ad_account_settings SET saved_assets = %s WHERE ad_account_id = %s",
            (json.dumps(assets, ensure_ascii=False), imported_id)
        )
        return True
    except Exception as e:
        logger.error(f"[account_settings] Erro ao save_single_asset: {e}")
        return False


def remove_single_asset(user_id, meta_account_id, asset_type, key_field, value):
    """
    Remove um item dos saved_assets.
    """
    if not _db_ok() or not value:
        return False
    try:
        imported_id = get_or_create_imported_account(user_id, meta_account_id)
        if not imported_id:
            return False
        row = fetch_one("SELECT saved_assets FROM ad_account_settings WHERE ad_account_id = %s", (imported_id,))
        if not row:
            return False
        assets = row['saved_assets'] or {}
        if isinstance(assets, str):
            assets = json.loads(assets)

        lst = assets.get(asset_type, [])
        assets[asset_type] = [item for item in lst if str(item.get(key_field)) != str(value)]

        execute(
            "UPDATE ad_account_settings SET saved_assets = %s WHERE ad_account_id = %s",
            (json.dumps(assets, ensure_ascii=False), imported_id)
        )
        return True
    except Exception as e:
        logger.error(f"[account_settings] Erro ao remove_single_asset: {e}")
        return False


def save_cac_target(user_id, meta_account_id, cac_value):
    """Salva o CAC ideal de uma conta."""
    if not _db_ok():
        return False
    try:
        imported_id = get_or_create_imported_account(user_id, meta_account_id)
        if not imported_id:
            return False
        execute(
            "INSERT INTO ad_account_settings (ad_account_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (imported_id,)
        )
        execute(
            "UPDATE ad_account_settings SET cac_target_value = %s WHERE ad_account_id = %s",
            (cac_value, imported_id)
        )
        return True
    except Exception as e:
        logger.error(f"[account_settings] Erro ao save_cac_target: {e}")
        return False


# ─────────────────────────────────────────────────────────────
# Squad 3 — upload_history
# ─────────────────────────────────────────────────────────────

def save_upload_history(user_id, meta_account_id, campaign_name, ad_name, strategy, success, error_message=None):
    """Registra um upload (sucesso ou falha) no histórico."""
    if not _db_ok():
        return
    try:
        imported_id = get_or_create_imported_account(user_id, meta_account_id)
        if not imported_id:
            return
        execute(
            """INSERT INTO upload_history
               (user_id, ad_account_id, campaign_name, ad_name, strategy, success, error_message)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (user_id, imported_id, campaign_name, ad_name, strategy, success, error_message)
        )
    except Exception as e:
        logger.error(f"[account_settings] Erro ao save_upload_history: {e}")


def get_upload_history(user_id, limit=50):
    """Retorna os últimos N uploads do usuário."""
    if not _db_ok():
        return []
    try:
        rows = fetch_all(
            """SELECT h.campaign_name, h.ad_name, h.strategy, h.success, h.error_message,
                      h.created_at, a.account_name, a.meta_account_id
               FROM upload_history h
               JOIN imported_ad_accounts a ON a.id = h.ad_account_id
               WHERE h.user_id = %s
               ORDER BY h.created_at DESC
               LIMIT %s""",
            (user_id, limit)
        )
        return [dict(r) for r in rows] if rows else []
    except Exception as e:
        logger.error(f"[account_settings] Erro ao get_upload_history: {e}")
        return []


# ─────────────────────────────────────────────────────────────
# Squad 5 — visualization_modes (Turbinada)
# ─────────────────────────────────────────────────────────────

def get_viz_modes(user_id):
    """Retorna lista de modos de visualização salvos pelo usuário."""
    if not _db_ok():
        return []
    try:
        rows = fetch_all(
            """SELECT id, mode_name, is_default, periods, sort_order
               FROM visualization_modes
               WHERE user_id = %s
               ORDER BY sort_order, mode_name""",
            (user_id,)
        )
        result = []
        for r in (rows or []):
            periods = r['periods'] or {}
            if isinstance(periods, str):
                periods = json.loads(periods)
            result.append({
                'id': str(r['id']),
                'mode_name': r['mode_name'],
                'is_default': r['is_default'],
                'periods': periods,
                'sort_order': r['sort_order'],
            })
        return result
    except Exception as e:
        logger.error(f"[account_settings] Erro ao get_viz_modes: {e}")
        return []


def save_viz_mode(user_id, mode_name, periods, is_default=False, mode_id=None):
    """
    Salva ou atualiza um modo de visualização.
    periods: dict {'columns': [{'key': ..., 'label': ..., 'since': ..., 'until': ...}]}
    Retorna o UUID do modo.
    """
    if not _db_ok():
        return None
    try:
        periods_json = json.dumps(periods, ensure_ascii=False)

        if is_default:
            # Remover is_default dos outros modos
            execute(
                "UPDATE visualization_modes SET is_default = false WHERE user_id = %s",
                (user_id,)
            )

        if mode_id:
            execute(
                """UPDATE visualization_modes
                   SET mode_name = %s, periods = %s, is_default = %s
                   WHERE id = %s AND user_id = %s""",
                (mode_name, periods_json, is_default, mode_id, user_id)
            )
            return mode_id
        else:
            row = execute_returning(
                """INSERT INTO visualization_modes (user_id, mode_name, periods, is_default)
                   VALUES (%s, %s, %s, %s) RETURNING id""",
                (user_id, mode_name, periods_json, is_default)
            )
            return str(row['id']) if row else None
    except Exception as e:
        logger.error(f"[account_settings] Erro ao save_viz_mode: {e}")
        return None


def delete_viz_mode(user_id, mode_id):
    """Remove um modo de visualização do usuário."""
    if not _db_ok():
        return False
    try:
        execute(
            "DELETE FROM visualization_modes WHERE id = %s AND user_id = %s",
            (mode_id, user_id)
        )
        return True
    except Exception as e:
        logger.error(f"[account_settings] Erro ao delete_viz_mode: {e}")
        return False
