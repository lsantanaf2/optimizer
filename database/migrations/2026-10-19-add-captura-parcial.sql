-- ============================================================
-- Migration: captura parcial (lead de abandono)
-- Data: 2026-10-19
-- Idempotente.
--
-- Lead parcial é quem digitou um WhatsApp válido no formulário e foi embora
-- sem enviar. Ele NÃO consentiu o envio, então nasce marcado e fica fora das
-- métricas por padrão — o operador liga a visualização quando quiser avaliar
-- se vale a pena abordar esse público.
--
-- A marca cai sozinha se a pessoa voltar e enviar o formulário de verdade.
-- ============================================================

ALTER TABLE crm_leads
    ADD COLUMN IF NOT EXISTS captura_parcial BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN crm_leads.captura_parcial IS
    'TRUE = digitou o telefone e abandonou sem enviar. Vira FALSE quando envia o formulário.';

-- Índice parcial: a consulta normal do kanban filtra captura_parcial = FALSE,
-- e os parciais são minoria — indexar só eles basta.
CREATE INDEX IF NOT EXISTS idx_crm_leads_parcial
    ON crm_leads (captura_parcial) WHERE captura_parcial;

-- ── Novos tipos de evento ────────────────────────────────────────────────────
-- O CHECK antigo não conhece 'lead_parcial'/'lead_completado'; recriado com
-- a lista inteira para o histórico continuar registrando tudo.
ALTER TABLE crm_lead_events DROP CONSTRAINT IF EXISTS crm_lead_events_tipo_valido;
ALTER TABLE crm_lead_events ADD CONSTRAINT crm_lead_events_tipo_valido CHECK (
    tipo IN (
        'lead_criado',
        'lead_parcial',        -- digitou o telefone e abandonou
        'lead_completado',     -- voltou e enviou de verdade
        'recadastro',
        'etapa_mudou',
        'status_mudou',
        'contato_tentado',
        'contato_efetivo',
        'call_agendada',
        'no_show',
        'call_realizada',
        'proposta_enviada',
        'ganho',
        'perdido',
        'qualificacao_atualizada',
        'nota_adicionada',
        'tarefa_criada',
        'tarefa_concluida'
    )
);
