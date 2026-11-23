-- LP da leva do Sorveteiro Raiz (/lp/sorveteiros): 10 vagas com 25% de
-- desconto, tráfego vindo do WhatsApp da base de alunos dele.
--
-- O destino depois do envio fica em redirect_url: é o link do WhatsApp com a
-- mensagem pronta. Fica NULL até o número ser confirmado — sem ele o lead cai
-- em /obrigado, que funciona.
INSERT INTO crm_forms (form_key, name, slug, allowed_origins) VALUES
    ('sk_sorveteiros_01', 'Captura SK — Sorveteiro Raiz (leva 1)', 'sorveteiros', '[]'::jsonb)
ON CONFLICT (form_key) DO NOTHING;

-- Quando o número estiver definido:
--   UPDATE crm_forms SET redirect_url =
--     'https://wa.me/55DDDNUMERO?text=' || <mensagem codificada>
--   WHERE form_key = 'sk_sorveteiros_01';
