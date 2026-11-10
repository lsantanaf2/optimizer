-- ============================================================
-- LPs de captura: imobiliárias/construtoras, salões de festa e delivery
-- ============================================================
-- Terceira, quarta e quinta ofertas servidas pelo próprio Flask:
--   /lp/imobiliaria   · /lp/salaodefesta   · /lp/delivery
--   (e em cadastro.xn--trfego-qta.com, /imobiliaria, /salaodefesta, /delivery)
--
-- Template por convenção de nome em modules/crm.py: crm_lp_<slug>.html quando
-- o arquivo existe, senão crm_lp.html. Os três templates já estão no repo, em
-- templates/crm_lp_imobiliaria.html, _salaodefesta.html e _delivery.html.
--
-- Os três saíram da LP de salão de beleza, que já é a versão sem vídeo: nenhum
-- desses nichos tem material de cliente, e foto ou vídeo inventado não entra.
-- No lugar da mídia o hero traz três pontos que sustentam a headline.
--
-- form_key é PÚBLICA (vai no HTML da LP) e precisa casar com
-- crm_forms_key_format: ^[a-z0-9_-]{6,64}$
--
-- allowed_origins vazio = só aceita envio da própria origem, igual às outras.
-- Só precisa de entrada aqui se a LP for hospedada fora.

INSERT INTO crm_forms (form_key, name, slug, allowed_origins)
VALUES
    ('sk_imobiliaria_01',  'Captura SK — imobiliárias e construtoras', 'imobiliaria',  '[]'::jsonb),
    ('sk_salaodefesta_01', 'Captura SK — salões de festa',             'salaodefesta', '[]'::jsonb),
    ('sk_delivery_01',     'Captura SK — delivery',                    'delivery',     '[]'::jsonb)
ON CONFLICT (form_key) DO NOTHING;
