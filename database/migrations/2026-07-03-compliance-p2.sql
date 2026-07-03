-- ============================================================
-- Migração: Conformidade Meta Platform Terms — Bloco P2
-- Data: 2026-07-03 (v2.21.0)
--
-- NOTA: aplicada automaticamente pelo app no primeiro request
-- de cada worker (app.py:ensure_db — idempotente). Este arquivo
-- é o registro canônico para o schema.
-- ============================================================

-- P2.1 — Log de chamadas à Meta Graph API (auditoria de conformidade).
-- Loga apenas o PATH do endpoint (nunca query string / tokens).
-- Retenção: 90 dias (sweep automático no boot do worker).
CREATE TABLE IF NOT EXISTS api_call_logs (
    id BIGSERIAL PRIMARY KEY,
    endpoint TEXT NOT NULL,
    response_code INT,
    usage_pct INT,
    called_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_call_logs_called ON api_call_logs (called_at DESC);

-- P2.3 — Momento do consentimento do usuário (conclusão do diálogo OAuth
-- da Meta, que lista exatamente os dados compartilhados).
ALTER TABLE app_users ADD COLUMN IF NOT EXISTS meta_consent_at TIMESTAMPTZ;

-- P2.2 — token_scope já existe no schema (user_meta_tokens.token_scope);
-- passa a ser preenchido no callback OAuth com os escopos CONCEDIDOS
-- (via GET /me/permissions, status=granted).

-- P2.4 — Retenção de upload_history: 90 dias (sweep automático, sem DDL).
