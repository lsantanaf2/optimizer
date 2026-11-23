-- Motivo de perda em lista fechada.
--
-- Já existia `motivo_saida`, texto livre: serve para o relato, não para
-- métrica — não dá para somar "caro demais", "achou caro" e "preço". Esta
-- coluna guarda a categoria; `motivo_saida` segue com o detalhe escrito.
--
-- Nasce dos 4 leads que receberam proposta e não fecharam: hoje eles só podem
-- virar "perdido" sem dizer por quê.

ALTER TABLE crm_leads ADD COLUMN IF NOT EXISTS motivo_perda TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'crm_leads_motivo_perda_valido'
    ) THEN
        ALTER TABLE crm_leads ADD CONSTRAINT crm_leads_motivo_perda_valido CHECK (
            motivo_perda IS NULL OR motivo_perda IN (
                'preco', 'timing', 'sumiu', 'concorrencia', 'fora_do_perfil', 'outro'
            )
        );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_crm_leads_motivo_perda
    ON crm_leads (motivo_perda) WHERE motivo_perda IS NOT NULL;
