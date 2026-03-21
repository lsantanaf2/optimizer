-- ============================================================
-- Optimizer Database Schema - Supabase (PostgreSQL)
-- Gerado em: 2026-03-21
-- Versão: 1.0
-- ============================================================

-- Extensão para UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- 💳 BLOCO 1: Identidade, Pagamento e Agências
-- ============================================================

-- 1. app_users (Usuários do Sistema)
CREATE TABLE app_users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    payment_gateway TEXT,
    gateway_customer_id TEXT UNIQUE,
    gateway_subscription_id TEXT UNIQUE,
    plan TEXT NOT NULL DEFAULT 'free',
    subscription_status TEXT DEFAULT 'active',
    current_period_end TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_app_users_email ON app_users (email);
CREATE INDEX idx_app_users_plan ON app_users (plan);
CREATE INDEX idx_app_users_subscription_status ON app_users (subscription_status);

-- 2. agencies (Guarda-Chuva Financeiro para Equipes)
CREATE TABLE agencies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    owner_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    agency_name TEXT NOT NULL,
    plan_type TEXT NOT NULL,
    max_seats INT NOT NULL DEFAULT 1,
    payment_gateway TEXT,
    gateway_customer_id TEXT,
    gateway_subscription_id TEXT,
    subscription_status TEXT DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_agencies_owner ON agencies (owner_id);

-- 3. agency_invites (Controle de Assentos)
CREATE TABLE agency_invites (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    agency_id UUID NOT NULL REFERENCES agencies(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    joined_at TIMESTAMPTZ,
    UNIQUE (agency_id, user_id)
);

CREATE INDEX idx_agency_invites_agency ON agency_invites (agency_id);
CREATE INDEX idx_agency_invites_user ON agency_invites (user_id);
CREATE INDEX idx_agency_invites_status ON agency_invites (status);

-- 4. billing_logs (Histórico e Extrato)
CREATE TABLE billing_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    payment_gateway TEXT NOT NULL,
    gateway_transaction_id TEXT,
    event_type TEXT NOT NULL,
    amount_paid NUMERIC(12, 2) NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'BRL',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_billing_logs_user ON billing_logs (user_id);
CREATE INDEX idx_billing_logs_event ON billing_logs (event_type);
CREATE INDEX idx_billing_logs_created ON billing_logs (created_at DESC);

-- ============================================================
-- 🛡️ BLOCO 2: Conexão Meta e Gestão de Contas
-- ============================================================

-- 5. user_meta_tokens (Isolamento de Token)
CREATE TABLE user_meta_tokens (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    meta_user_id TEXT NOT NULL,
    access_token TEXT NOT NULL,
    token_scope TEXT,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_user_meta_tokens_user ON user_meta_tokens (user_id);
CREATE INDEX idx_user_meta_tokens_expires ON user_meta_tokens (expires_at);

-- 6. imported_ad_accounts (Seletor de Trabalho)
CREATE TABLE imported_ad_accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    meta_account_id TEXT NOT NULL,
    account_name TEXT,
    import_status TEXT NOT NULL DEFAULT 'active',
    pinned_order INT DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, meta_account_id)
);

CREATE INDEX idx_imported_ad_accounts_user ON imported_ad_accounts (user_id);
CREATE INDEX idx_imported_ad_accounts_meta ON imported_ad_accounts (meta_account_id);

-- 7. ad_account_settings (Configurações e Acervo)
CREATE TABLE ad_account_settings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ad_account_id UUID NOT NULL UNIQUE REFERENCES imported_ad_accounts(id) ON DELETE CASCADE,
    cac_target_value NUMERIC(12, 2),
    default_visualization_mode_id UUID,
    saved_assets JSONB NOT NULL DEFAULT '{}'::jsonb
);

COMMENT ON COLUMN ad_account_settings.saved_assets IS '
Formato esperado:
{
  "facebook_pages": [{"id": "123", "name": "Página Principal", "is_default": true}],
  "instagram_profiles": [{"id": "456", "username": "@principal", "is_default": true}],
  "pixels": [{"id": "789", "name": "Pixel Vendas", "is_default": true}],
  "primary_texts": [{"text": "...", "use_count": 8}],
  "headlines": [{"text": "...", "use_count": 22}],
  "descriptions": [{"text": "...", "use_count": 5}],
  "urls": [{"url": "https://...", "label": "...", "use_count": 15}],
  "utms": [{"pattern": "utm_source=fb", "label": "...", "use_count": 12}],
  "default_cta": "LEARN_MORE"
}';

-- ============================================================
-- 📊 BLOCO 3: Operação, Automação e Auditoria
-- ============================================================

-- 8. visualization_modes (Aba Turbinada)
CREATE TABLE visualization_modes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    mode_name TEXT NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT false,
    periods JSONB NOT NULL DEFAULT '{"columns": []}'::jsonb,
    sort_order INT NOT NULL DEFAULT 0
);

CREATE INDEX idx_visualization_modes_user ON visualization_modes (user_id);

COMMENT ON COLUMN visualization_modes.periods IS '
Formato esperado:
{
  "columns": [
    {"key": "hoje", "label": "Hoje", "since": "today", "until": "today"},
    {"key": "ontem", "label": "Ontem", "since": "-1d", "until": "-1d"},
    {"key": "p7d", "label": "7 Dias", "since": "-7d", "until": "today"},
    {"key": "p30d", "label": "30 Dias", "since": "-30d", "until": "today"}
  ]
}';

-- FK de ad_account_settings → visualization_modes (adicionada após ambas existirem)
ALTER TABLE ad_account_settings
    ADD CONSTRAINT fk_default_visualization_mode
    FOREIGN KEY (default_visualization_mode_id)
    REFERENCES visualization_modes(id)
    ON DELETE SET NULL;

-- 9. upload_history (Rastreio de Ações em Massa)
CREATE TABLE upload_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    ad_account_id UUID NOT NULL REFERENCES imported_ad_accounts(id) ON DELETE CASCADE,
    campaign_name TEXT,
    ad_name TEXT,
    strategy TEXT,
    success BOOLEAN NOT NULL DEFAULT false,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_upload_history_user ON upload_history (user_id);
CREATE INDEX idx_upload_history_account ON upload_history (ad_account_id);
CREATE INDEX idx_upload_history_created ON upload_history (created_at DESC);

-- 10. optimization_rules (Regras Automatizadas)
CREATE TABLE optimization_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    ad_account_id UUID NOT NULL REFERENCES imported_ad_accounts(id) ON DELETE CASCADE,
    rule_name TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    entity_type TEXT NOT NULL,
    metric TEXT NOT NULL,
    operator TEXT NOT NULL,
    threshold NUMERIC(12, 2) NOT NULL,
    lookback_period TEXT NOT NULL DEFAULT 'last_7d',
    action_to_take TEXT NOT NULL,
    check_frequency TEXT NOT NULL DEFAULT 'daily',
    last_triggered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_entity_type CHECK (entity_type IN ('campaign', 'adset', 'ad')),
    CONSTRAINT chk_operator CHECK (operator IN ('gt', 'lt', 'eq', 'gte', 'lte')),
    CONSTRAINT chk_action CHECK (action_to_take IN ('pause', 'increase_budget', 'decrease_budget')),
    CONSTRAINT chk_frequency CHECK (check_frequency IN ('hourly', 'daily', 'weekly')),
    CONSTRAINT chk_lookback CHECK (lookback_period IN ('today', 'last_3d', 'last_7d', 'last_14d', 'last_30d'))
);

CREATE INDEX idx_optimization_rules_user ON optimization_rules (user_id);
CREATE INDEX idx_optimization_rules_account ON optimization_rules (ad_account_id);
CREATE INDEX idx_optimization_rules_active ON optimization_rules (is_active) WHERE is_active = true;

-- 11. audit_logs (Histórico de Ações Executadas)
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    rule_id UUID REFERENCES optimization_rules(id) ON DELETE SET NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action_type TEXT NOT NULL,
    details TEXT,
    meta_object_id TEXT
);

CREATE INDEX idx_audit_logs_user ON audit_logs (user_id);
CREATE INDEX idx_audit_logs_rule ON audit_logs (rule_id);
CREATE INDEX idx_audit_logs_executed ON audit_logs (executed_at DESC);
CREATE INDEX idx_audit_logs_action ON audit_logs (action_type);

-- ============================================================
-- 🔄 Trigger: auto-update updated_at
-- ============================================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_app_users_updated
    BEFORE UPDATE ON app_users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_user_meta_tokens_updated
    BEFORE UPDATE ON user_meta_tokens
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_optimization_rules_updated
    BEFORE UPDATE ON optimization_rules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ============================================================
-- 🔒 RLS (Row Level Security) - Supabase
-- ============================================================

ALTER TABLE app_users ENABLE ROW LEVEL SECURITY;
ALTER TABLE agencies ENABLE ROW LEVEL SECURITY;
ALTER TABLE agency_invites ENABLE ROW LEVEL SECURITY;
ALTER TABLE billing_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_meta_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE imported_ad_accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE ad_account_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE visualization_modes ENABLE ROW LEVEL SECURITY;
ALTER TABLE upload_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE optimization_rules ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_logs ENABLE ROW LEVEL SECURITY;

-- Policies: cada user vê apenas seus próprios dados
CREATE POLICY "Users see own data" ON app_users
    FOR ALL USING (auth.uid() = id);

CREATE POLICY "Users see own agencies" ON agencies
    FOR ALL USING (owner_id = auth.uid());

CREATE POLICY "Users see own invites" ON agency_invites
    FOR ALL USING (user_id = auth.uid());

CREATE POLICY "Users see own billing" ON billing_logs
    FOR ALL USING (user_id = auth.uid());

CREATE POLICY "Users see own tokens" ON user_meta_tokens
    FOR ALL USING (user_id = auth.uid());

CREATE POLICY "Users see own accounts" ON imported_ad_accounts
    FOR ALL USING (user_id = auth.uid());

CREATE POLICY "Users see own account settings" ON ad_account_settings
    FOR ALL USING (
        ad_account_id IN (
            SELECT id FROM imported_ad_accounts WHERE user_id = auth.uid()
        )
    );

CREATE POLICY "Users see own viz modes" ON visualization_modes
    FOR ALL USING (user_id = auth.uid());

CREATE POLICY "Users see own uploads" ON upload_history
    FOR ALL USING (user_id = auth.uid());

CREATE POLICY "Users see own rules" ON optimization_rules
    FOR ALL USING (user_id = auth.uid());

CREATE POLICY "Users see own audit" ON audit_logs
    FOR ALL USING (user_id = auth.uid());
