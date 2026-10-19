-- ============================================================
-- Migration: SLA medido pela primeira TENTATIVA de contato
-- Data: 2026-10-19
-- Idempotente.
--
-- Problema que isto corrige: o SLA estava amarrado a `primeiro_contato_em`,
-- que só é preenchido quando o lead RESPONDE. Quem mandava mensagem e não era
-- atendido ficava, no painel, como se nunca tivesse sido abordado — o card
-- seguia marcando "aguardando" em vermelho.
--
-- São duas coisas diferentes e agora têm colunas diferentes:
--   primeira_tentativa_em → quando o operador tentou. ELE controla. É o SLA.
--   primeiro_contato_em   → quando o lead respondeu. ELE NÃO controla.
-- ============================================================

ALTER TABLE crm_leads
    ADD COLUMN IF NOT EXISTS primeira_tentativa_em TIMESTAMPTZ;

COMMENT ON COLUMN crm_leads.primeira_tentativa_em IS
    'Primeira tentativa de contato, com ou sem resposta. Base do SLA.';
COMMENT ON COLUMN crm_leads.primeiro_contato_em IS
    'Primeira vez que o lead RESPONDEU. Base da taxa de resposta, não do SLA.';

CREATE INDEX IF NOT EXISTS idx_crm_leads_primeira_tentativa
    ON crm_leads (primeira_tentativa_em);

-- ── Recupera o histórico ────────────────────────────────────────────────────
-- O evento da tentativa sempre foi gravado com a hora certa, mesmo quando o
-- resultado era "não atendeu"; só não havia coluna para ele. Reconstrói a
-- partir do primeiro 'contato_tentado' de cada lead.
UPDATE crm_leads l
SET primeira_tentativa_em = t.quando
FROM (
    SELECT lead_id, MIN(created_at) AS quando
    FROM crm_lead_events
    WHERE tipo IN ('contato_tentado', 'contato_efetivo')
    GROUP BY lead_id
) t
WHERE l.id = t.lead_id
  AND l.primeira_tentativa_em IS NULL;

-- Quem já tinha resposta registrada mas nenhum evento de tentativa (lead
-- movido direto de etapa) usa o próprio contato efetivo como tentativa.
UPDATE crm_leads
SET primeira_tentativa_em = primeiro_contato_em
WHERE primeira_tentativa_em IS NULL
  AND primeiro_contato_em IS NOT NULL;
