-- LP genérica de captura (/lp/empresas), sem nicho — substitui as LPs de
-- nicho depois que os anúncios apontarem para ela. Template por convenção:
-- templates/crm_lp_empresas.html.
INSERT INTO crm_forms (form_key, name, slug, allowed_origins) VALUES
    ('sk_empresas_01', 'Captura SK — empresas (genérica)', 'empresas', '[]'::jsonb)
ON CONFLICT (form_key) DO NOTHING;
