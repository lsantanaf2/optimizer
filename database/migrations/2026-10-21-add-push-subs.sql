-- ============================================================
-- Migration: inscrições de Web Push do CRM
-- Data: 2026-10-21
-- Idempotente.
--
-- Guarda para quem o servidor deve empurrar o aviso de lead novo. Com isso o
-- alerta chega mesmo com a aba do CRM FECHADA — quem recebe é o Service
-- Worker, não a página.
--
-- A inscrição é por NAVEGADOR, não por usuário: o mesmo operador no desktop e
-- no celular gera duas linhas, e as duas devem ser avisadas.
-- ============================================================

CREATE TABLE IF NOT EXISTS crm_push_subs (
    -- O endpoint é a identidade da inscrição para o navegador; é longo
    -- (chega a ~500 chars) e único por dispositivo.
    endpoint     TEXT PRIMARY KEY,
    p256dh       TEXT NOT NULL,          -- chave pública do cliente (cifra a mensagem)
    auth         TEXT NOT NULL,          -- segredo de autenticação do cliente
    user_id      UUID REFERENCES app_users (id) ON DELETE CASCADE,
    user_agent   TEXT,

    -- Operação: uma inscrição morta responde 404/410 e precisa sair daqui,
    -- senão cada lead novo gasta uma requisição à toa para sempre.
    criado_em    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ultimo_ok_em TIMESTAMPTZ,
    falhas       INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_crm_push_user ON crm_push_subs (user_id);
