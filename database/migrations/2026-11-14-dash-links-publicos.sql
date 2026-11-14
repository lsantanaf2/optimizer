-- Token de link público para dashboards fixas (fora de dashboard_clients).
--
-- /dash/faturamento era aberta sem token e a API devolvia bruto, líquido,
-- reembolsos, ROI e CAC para qualquer um. Agora exige ?t=<token> (ou login).
-- dashboard_clients não serve: cada linha lá vira dashboard Meta de cliente
-- no /dash admin.
--
-- Server-only: RLS ligado e zero policies (ver convenção de RLS).

CREATE TABLE IF NOT EXISTS dash_links_publicos (
    nome        text PRIMARY KEY,
    token       text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE dash_links_publicos ENABLE ROW LEVEL SECURITY;

-- O token é gerado fora do repositório (que é público):
--   INSERT INTO dash_links_publicos (nome, token) VALUES ('faturamento', '<token>');
-- Para revogar um link: UPDATE ... SET token = '<novo>' WHERE nome = '...'.
