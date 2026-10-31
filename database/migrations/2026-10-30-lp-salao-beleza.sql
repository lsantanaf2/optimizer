-- ============================================================
-- LP de captura para salões de beleza
-- ============================================================
-- Segunda oferta servida pelo próprio Flask. A LP fica em:
--   https://optimizer.xn--trfego-qta.com/lp/salao
--   https://cadastro.xn--trfego-qta.com/salao
--
-- O template é escolhido por convenção de nome em modules/crm.py:
-- crm_lp_<slug>.html quando o arquivo existe, senão crm_lp.html. Como o slug
-- é 'salao', a página usa templates/crm_lp_salao.html — não é preciso coluna
-- nova nem if por slug no código.
--
-- form_key é PÚBLICA (vai no HTML da LP) e precisa casar com
-- crm_forms_key_format: ^[a-z0-9_-]{6,64}$
--
-- allowed_origins vazio = só aceita envio da própria origem, igual à LP
-- principal. Só precisa de entrada aqui se a LP for hospedada fora.

INSERT INTO crm_forms (form_key, name, slug, allowed_origins)
VALUES ('sk_salao_01', 'Captura SK — salões de beleza', 'salao', '[]'::jsonb)
ON CONFLICT (form_key) DO NOTHING;
