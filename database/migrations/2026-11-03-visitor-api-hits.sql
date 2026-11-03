-- ============================================================
-- Migration: contador de uso da Visitor API própria
-- Data: 2026-11-03
-- Idempotente.
--
-- A Visitor API própria é um Cloudflare Worker que substituiu a visitorapi.com
-- (US$ 7/mês). O Worker é público por natureza — qualquer página pode chamá-lo —
-- então sem este contador não há como saber quem está usando, nem quanto falta
-- para o teto de 100 mil requisições/dia do plano grátis da Cloudflare.
--
-- Granularidade: UMA LINHA POR (domínio, dia). Linha por hit foi descartada de
-- propósito: no teto do plano seriam ~3 milhões de linhas/mês para responder
-- uma pergunta que o contador já responde.
--
-- O `dia` é gravado no fuso de Brasília a partir do NOW() do Postgres, nunca do
-- relógio da aplicação — ele corre ~100s atrasado e jogaria hits do começo do
-- dia para a véspera.
-- ============================================================

CREATE TABLE IF NOT EXISTS visitor_api_hits (
    -- Hostname extraído do header Origin da chamada (ex: meucachorroadestrado.com).
    -- Chamada sem Origin (curl, monitoração) cai em '(sem origin)' em vez de
    -- sumir, senão esse tráfego fica invisível na soma do teto.
    dominio    TEXT NOT NULL,
    dia        DATE NOT NULL,
    hits       BIGINT NOT NULL DEFAULT 0,

    criado_em  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    visto_em   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (dominio, dia)
);

-- O painel sempre lê os últimos N dias ordenados por dia desc.
CREATE INDEX IF NOT EXISTS idx_visitor_api_hits_dia ON visitor_api_hits (dia DESC);

-- Convenção da casa: RLS ligado em toda tabela. Esta é server-only (só o Flask
-- escreve e lê, via pool), então fica com ZERO policies — ninguém passa pelo
-- PostgREST. Nunca usar FORCE aqui.
ALTER TABLE visitor_api_hits ENABLE ROW LEVEL SECURITY;
