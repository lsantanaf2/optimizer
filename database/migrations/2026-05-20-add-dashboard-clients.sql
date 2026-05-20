-- ============================================================
-- Migration: dashboard_clients (multi-tenant /dash)
-- Data: 2026-05-20
-- Idempotente: pode rodar múltiplas vezes sem erro.
-- ============================================================

CREATE TABLE IF NOT EXISTS dashboard_clients (
    slug                       TEXT PRIMARY KEY,
    name                       TEXT NOT NULL,
    display_name               TEXT,

    -- Meta Ads
    meta_ad_account_id         TEXT NOT NULL,
    meta_token_user_id         UUID,                     -- user_id no app que possui o token Meta válido. NULL = usa token persistente do sistema.
    typeform_action_type       TEXT DEFAULT 'offsite_conversion.fb_pixel_custom',

    -- Google Ads (OAuth via MCC ou planilha pública)
    google_ads_customer_id     TEXT,                     -- preferido (OAuth)
    google_ads_user_id         UUID,                     -- quem tem o token OAuth Google Ads (geralmente o admin/MCC)
    google_ads_sheet_id        TEXT,                     -- fallback (planilha pública)
    google_ads_sheet_gid       TEXT,
    google_ads_filter_keyword  TEXT,                     -- 'VINCI' filtra nome de campanha na planilha

    -- Planilha MQLs/Wons (Service Account)
    mqls_spreadsheet_id        TEXT,

    -- Comportamento UI
    locked_period              TEXT,                     -- 'this_month' / 'last_30_days' / NULL
    excluded_campaign_patterns JSONB DEFAULT '[]'::jsonb,

    -- Acesso público (link com token)
    public_link_enabled        BOOLEAN DEFAULT TRUE,
    public_link_token          TEXT UNIQUE NOT NULL,     -- token aleatório anti-enum

    -- Auditoria
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT dashboard_clients_slug_format CHECK (slug ~ '^[a-z0-9-]+$')
);

CREATE INDEX IF NOT EXISTS idx_dashboard_clients_token ON dashboard_clients (public_link_token);
CREATE INDEX IF NOT EXISTS idx_dashboard_clients_meta_account ON dashboard_clients (meta_ad_account_id);

-- ============================================================
-- Seed: VINCI (cópia da config hardcoded atual)
-- ============================================================
-- Token gerado: vnc_<random32> — substituir pelo final após geração
-- ============================================================

INSERT INTO dashboard_clients (
    slug, name, display_name,
    meta_ad_account_id, typeform_action_type,
    google_ads_sheet_id, google_ads_sheet_gid, google_ads_filter_keyword,
    mqls_spreadsheet_id,
    locked_period, excluded_campaign_patterns,
    public_link_token
) VALUES (
    'vinci',
    'Vinci',
    'Vinci',
    'act_2023939324650844',
    'offsite_conversion.fb_pixel_custom',
    '1vhctrrIBQujABaD0VROW8dNHuZIqA-MIX8ESZC77tLg',
    '2054617579',
    'VINCI',
    '1m6syDzMDZqB44ZTKaRj5t79HUDuyEqaN2RgAo0kpECc',
    'this_month',
    '["[DEMO-180]", "[EVENTO MQL]", "[BRANDING RENAISSANCE]"]'::jsonb,
    'vnc_' || encode(gen_random_bytes(16), 'hex')
)
ON CONFLICT (slug) DO NOTHING;

-- ============================================================
-- RLS (opcional - habilitar depois quando tiver login de clientes)
-- ============================================================
-- ALTER TABLE dashboard_clients ENABLE ROW LEVEL SECURITY;
