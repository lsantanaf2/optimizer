-- Acesso ao sistema passa a exigir liberação explícita.
--
-- O cadastro em /auth/register estava aberto e o CRM autorizava qualquer
-- usuário logado: dois e-mails descartáveis se cadastraram (2026-05-12 e
-- 2026-10-23) e tinham leitura de todos os leads. Login agora só vale para
-- quem tem acesso_liberado = true; o resto é barrado no login e tem a sessão
-- derrubada a cada request (app.py, before_request).
--
-- Default false: conta nova nasce bloqueada.

ALTER TABLE app_users
    ADD COLUMN IF NOT EXISTS acesso_liberado boolean NOT NULL DEFAULT false;

-- Liberação é manual e fica fora do repositório (que é público):
--   UPDATE app_users SET acesso_liberado = true WHERE email = '<email>';
