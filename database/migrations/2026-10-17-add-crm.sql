-- ============================================================
-- Migration: CRM de Leads (braço /crm do optimizer)
-- Data: 2026-10-17
-- Idempotente: pode rodar múltiplas vezes sem erro.
--
-- Modelo "pessoa + submissões":
--   crm_leads            -> 1 linha por PESSOA (chave: whatsapp E.164). O card do kanban.
--   crm_lead_submissions -> 1 linha por CADASTRO. Guarda a atribuição daquele cadastro
--                           específico. É o que responde "ele se cadastrou 3x e de onde".
--   crm_lead_events      -> histórico IMUTÁVEL de tudo que aconteceu (mudança de etapa,
--                           tentativa de contato, no-show...). Base de TODAS as métricas:
--                           sem esta tabela não existe SLA, show-rate nem tempo de ciclo.
--   crm_notes            -> anotações com timestamp e autor (não sobrescreve histórico).
--   crm_tasks            -> tarefas genéricas (recontato, follow-up, call).
--   crm_forms            -> formulários de captura (form_key pública por LP/origem).
--
-- RLS LIGADO E SEM POLICIES, de propósito (mesmo padrão de dashboard_clients,
-- api_call_logs e dash_snapshots neste projeto):
--   - o Flask conecta como `postgres`, que é DONO das tabelas e por isso ignora
--     RLS (relforcerowsecurity = false) — a aplicação funciona normalmente;
--   - anon/authenticated passam pelo PostgREST e caem no RLS: zero policies =
--     zero linhas. Sem isso, `https://<ref>.supabase.co/rest/v1/crm_leads` com a
--     anon key devolveria a base inteira de nome/WhatsApp/e-mail.
-- Dado pessoal de lead só sai por /crm, atrás de @login_required.
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- 1. crm_forms — formulários de captura
-- ============================================================
-- form_key é PÚBLICA (vai no HTML da LP). Não é segredo: serve para identificar
-- a origem, permitir/revogar um formulário e aplicar rate limit por formulário.
CREATE TABLE IF NOT EXISTS crm_forms (
    form_key        TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    slug            TEXT UNIQUE,                       -- rota /lp/<slug> quando a LP é servida pelo Flask
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    allowed_origins JSONB NOT NULL DEFAULT '[]'::jsonb, -- CORS: ["https://lp.exemplo.com"]. [] = só mesma origem.
    redirect_url    TEXT,                               -- página de obrigado
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT crm_forms_key_format CHECK (form_key ~ '^[a-z0-9_-]{6,64}$')
);

-- ============================================================
-- 2. crm_leads — a pessoa (1 card no kanban)
-- ============================================================
CREATE TABLE IF NOT EXISTS crm_leads (
    id                 UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Identidade. whatsapp_e164 é a chave de deduplicação: mesma pessoa
    -- cadastrando 2x NÃO cria card novo, cria uma submission nova.
    -- E.164 internacional ("+5511987654321", "+351912345678") — não só Brasil.
    whatsapp_e164      TEXT NOT NULL UNIQUE,
    whatsapp_pais      TEXT,                            -- ISO-2 do país detectado ("BR", "PT")
    nome               TEXT,
    email              TEXT,
    empresa_segmento   TEXT,

    -- Carteira. NULL = sem dono (hoje só existe 1 operador; já preparado para SDRs).
    owner_id           UUID REFERENCES app_users (id) ON DELETE SET NULL,

    -- Funil enxuto: 5 etapas. Sub-etapas viram eventos/tags, não coluna.
    etapa              TEXT NOT NULL DEFAULT 'novo',
    -- Status lateral: tira o lead do fluxo ativo sem criar coluna infinita.
    status             TEXT NOT NULL DEFAULT 'ativo',

    -- Motivo de saída (preenchido quando status != 'ativo')
    motivo_saida       TEXT,

    -- Ficha de qualificação SDR. Valores NUMÉRICOS livres (sem faixas):
    -- não há histórico para definir faixas ainda; o lead informa o número.
    faturamento_anual  NUMERIC(14, 2),
    investimento_midia NUMERIC(14, 2),                  -- investimento mensal atual em mídia
    ja_fez_trafego     BOOLEAN,                          -- NULL = desconhecido (estado inicial real)
    urgencia           TEXT,                             -- 'imediata' | '30_dias' | 'sem_pressa'
    qualificado        BOOLEAN,                          -- NULL = ainda não avaliado

    -- Valores do negócio (sem isto não existe CAC nem ROAS real)
    valor_proposta     NUMERIC(14, 2),
    valor_fechado      NUMERIC(14, 2),
    data_fechamento    TIMESTAMPTZ,

    -- Marcos desnormalizados (derivados de crm_lead_events, mantidos aqui para
    -- o kanban não precisar agregar eventos a cada render)
    primeiro_contato_em TIMESTAMPTZ,                     -- base do SLA de 1º contato
    ultimo_contato_em   TIMESTAMPTZ,
    submissions_count   INTEGER NOT NULL DEFAULT 0,      -- quantas vezes se cadastrou
    last_submission_at  TIMESTAMPTZ,

    arquivado_em       TIMESTAMPTZ,                      -- fora do kanban, continua nas métricas
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT crm_leads_etapa_valida CHECK (
        etapa IN ('novo', 'contatado', 'call_agendada', 'proposta', 'fechado')
    ),
    CONSTRAINT crm_leads_status_valido CHECK (
        status IN ('ativo', 'perdido', 'timing_errado')
    ),
    CONSTRAINT crm_leads_urgencia_valida CHECK (
        urgencia IS NULL OR urgencia IN ('imediata', '30_dias', 'sem_pressa')
    ),
    CONSTRAINT crm_leads_whatsapp_e164 CHECK (whatsapp_e164 ~ '^\+[1-9][0-9]{6,14}$')
);

CREATE INDEX IF NOT EXISTS idx_crm_leads_etapa       ON crm_leads (etapa) WHERE arquivado_em IS NULL;
CREATE INDEX IF NOT EXISTS idx_crm_leads_status      ON crm_leads (status);
CREATE INDEX IF NOT EXISTS idx_crm_leads_owner       ON crm_leads (owner_id);
CREATE INDEX IF NOT EXISTS idx_crm_leads_created     ON crm_leads (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_crm_leads_email       ON crm_leads (LOWER(email));
CREATE INDEX IF NOT EXISTS idx_crm_leads_fechamento  ON crm_leads (data_fechamento) WHERE data_fechamento IS NOT NULL;

-- ============================================================
-- 3. crm_lead_submissions — cada cadastro, com sua atribuição
-- ============================================================
-- Uma linha por submit do formulário. Nunca atualizada: é o registro de
-- "quando e de onde ele veio naquela vez".
CREATE TABLE IF NOT EXISTS crm_lead_submissions (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lead_id        UUID NOT NULL REFERENCES crm_leads (id) ON DELETE CASCADE,
    form_key       TEXT REFERENCES crm_forms (form_key) ON DELETE SET NULL,

    -- Atribuição LAST-touch (da URL no momento do submit)
    utm_source     TEXT,
    utm_medium     TEXT,
    utm_campaign   TEXT,
    utm_content    TEXT,
    utm_term       TEXT,

    -- Atribuição FIRST-touch (persistida em localStorage na 1ª visita).
    -- Os dois importam: first mostra quem descobriu, last mostra quem converteu.
    first_utm_source   TEXT,
    first_utm_medium   TEXT,
    first_utm_campaign TEXT,
    first_utm_content  TEXT,
    first_utm_term     TEXT,
    first_seen_at      TIMESTAMPTZ,

    -- Click IDs e cookies de plataforma. ISTO é o que amarra lead -> anúncio real
    -- e o que permite devolver Lead/Purchase para a Meta via CAPI depois.
    fbclid         TEXT,
    gclid          TEXT,
    fbp            TEXT,                                 -- cookie _fbp
    fbc            TEXT,                                 -- cookie _fbc
    ttclid         TEXT,

    -- IDs de entidade Meta, quando a LP recebe as macros ({{ad.id}} etc.)
    meta_ad_id     TEXT,
    meta_adset_id  TEXT,
    meta_campaign_id TEXT,

    -- Contexto
    referrer       TEXT,
    landing_url    TEXT,
    ip             TEXT,
    user_agent     TEXT,
    payload        JSONB NOT NULL DEFAULT '{}'::jsonb,   -- campos extras do form, sem migration

    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crm_subs_lead     ON crm_lead_submissions (lead_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_crm_subs_created  ON crm_lead_submissions (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_crm_subs_campaign ON crm_lead_submissions (utm_campaign);
CREATE INDEX IF NOT EXISTS idx_crm_subs_adid     ON crm_lead_submissions (meta_ad_id) WHERE meta_ad_id IS NOT NULL;

-- ============================================================
-- 4. crm_lead_events — histórico imutável (motor das métricas)
-- ============================================================
CREATE TABLE IF NOT EXISTS crm_lead_events (
    id          BIGSERIAL PRIMARY KEY,
    lead_id     UUID NOT NULL REFERENCES crm_leads (id) ON DELETE CASCADE,

    tipo        TEXT NOT NULL,
    -- Transição de etapa (preenchido quando tipo='etapa_mudou')
    de_etapa    TEXT,
    para_etapa  TEXT,
    de_status   TEXT,
    para_status TEXT,

    autor_id    UUID REFERENCES app_users (id) ON DELETE SET NULL,  -- NULL = sistema/automático
    payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT crm_lead_events_tipo_valido CHECK (
        tipo IN (
            'lead_criado',          -- 1º cadastro
            'recadastro',           -- cadastrou de novo (gera submission, não card)
            'etapa_mudou',
            'status_mudou',
            'contato_tentado',      -- ligou/mandou whats (payload.canal, payload.resultado)
            'contato_efetivo',      -- respondeu — marca primeiro_contato_em
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
    )
);

CREATE INDEX IF NOT EXISTS idx_crm_events_lead  ON crm_lead_events (lead_id, created_at);
CREATE INDEX IF NOT EXISTS idx_crm_events_tipo  ON crm_lead_events (tipo, created_at DESC);

-- ============================================================
-- 5. crm_notes — anotações com timestamp
-- ============================================================
CREATE TABLE IF NOT EXISTS crm_notes (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lead_id    UUID NOT NULL REFERENCES crm_leads (id) ON DELETE CASCADE,
    autor_id   UUID REFERENCES app_users (id) ON DELETE SET NULL,
    texto      TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crm_notes_lead ON crm_notes (lead_id, created_at DESC);

-- ============================================================
-- 6. crm_tasks — tarefas genéricas (recontato é um tipo)
-- ============================================================
CREATE TABLE IF NOT EXISTS crm_tasks (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lead_id      UUID NOT NULL REFERENCES crm_leads (id) ON DELETE CASCADE,
    owner_id     UUID REFERENCES app_users (id) ON DELETE SET NULL,
    tipo         TEXT NOT NULL DEFAULT 'recontato',
    titulo       TEXT,
    motivo       TEXT,
    due_at       TIMESTAMPTZ NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pendente',
    concluido_em TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT crm_tasks_tipo_valido   CHECK (tipo IN ('recontato', 'follow_up', 'call', 'outro')),
    CONSTRAINT crm_tasks_status_valido CHECK (status IN ('pendente', 'concluido', 'cancelado'))
);

CREATE INDEX IF NOT EXISTS idx_crm_tasks_due  ON crm_tasks (due_at) WHERE status = 'pendente';
CREATE INDEX IF NOT EXISTS idx_crm_tasks_lead ON crm_tasks (lead_id, due_at);

-- ============================================================
-- 7. RLS — fecha as tabelas para o PostgREST (anon key)
-- ============================================================
-- Ligar sem criar policy é intencional: nega tudo para anon/authenticated.
-- NÃO usar FORCE ROW LEVEL SECURITY — isso faria o próprio Flask (role
-- `postgres`, dono das tabelas) deixar de enxergar os dados.
ALTER TABLE crm_forms            ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_leads            ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_lead_submissions ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_lead_events      ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_notes            ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_tasks            ENABLE ROW LEVEL SECURITY;

-- ============================================================
-- Triggers de updated_at (reusa update_updated_at() do schema base)
-- ============================================================
-- Cria apenas se não existir: a função é compartilhada com app_users,
-- user_meta_tokens e optimization_rules — um CREATE OR REPLACE aqui
-- sobrescreveria o que já está em produção sem necessidade.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public' AND p.proname = 'update_updated_at'
    ) THEN
        CREATE FUNCTION update_updated_at()
        RETURNS TRIGGER AS $fn$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $fn$ LANGUAGE plpgsql;
    END IF;
END $$;

DROP TRIGGER IF EXISTS trg_crm_leads_updated ON crm_leads;
CREATE TRIGGER trg_crm_leads_updated
    BEFORE UPDATE ON crm_leads
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS trg_crm_tasks_updated ON crm_tasks;
CREATE TRIGGER trg_crm_tasks_updated
    BEFORE UPDATE ON crm_tasks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

DROP TRIGGER IF EXISTS trg_crm_forms_updated ON crm_forms;
CREATE TRIGGER trg_crm_forms_updated
    BEFORE UPDATE ON crm_forms
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ============================================================
-- Formulário inicial (LP servida pelo próprio Flask em /lp/sk)
-- ============================================================
INSERT INTO crm_forms (form_key, name, slug, allowed_origins)
VALUES ('sk_captura_01', 'Captura SK — principal', 'sk', '[]'::jsonb)
ON CONFLICT (form_key) DO NOTHING;
