# AGENTS.md

Plataforma interna da SK para gestão de tráfego pago: sobe criativos na Meta Ads,
serve dashboards por cliente e por lançamento, e roda o CRM de leads próprio.
Python 3.11 + Flask com Blueprints, PostgreSQL no Supabase, Docker na VPS.

## Comandos

| O quê | Comando | Verificado |
|---|---|---|
| Checar sintaxe Python | `python -m py_compile modules/<arquivo>.py` | ✅ |
| Checar sintaxe JS | `node --check static/<arquivo>.js` | ✅ |
| "Teste" de cruzamento | `python test_cruzamento_logic.py` → imprime `SUCCESS` | ✅ |
| Lint | `python -m ruff check .` → hoje passa limpo | ✅ |
| Lint, corrigindo o automático | `python -m ruff check . --fix` | ✅ |
| Build da imagem | `docker build -t optimizer-image:auto .` | ⚠️ não verificado (sem Docker local) |
| Subir em produção | `bash /var/www/optimizer/deploy.sh` (na VPS) | ⚠️ não verificado |

**Não existe suíte de testes nem build de front.** Sem pytest configurado, sem
bundler. Não invente `npm test` nem `pytest`. `test_creative.py` não é teste:
chama a API da Meta ao vivo — não execute.

O lint é `ruff` (config em `ruff.toml`, instalação em `requirements-dev.txt`).
Ele **passa limpo hoje**: qualquer violação nova quebra. As regras boas que
ainda têm violação estão listadas como baseline no próprio `ruff.toml`, com a
contagem — voltam a valer quando zerarem. Não suba o teto nem amplie o
`per-file-ignores` para fazer código novo passar.

## Deploy

Push na `main` **é** deploy. Dois mecanismos disparam sozinhos:

1. **Timer systemd** na VPS polla o git a cada 60s e roda `deploy.sh` — caminho real em uso.
2. **GitHub Actions** (`.github/workflows/deploy.yml`) faz SSH e recria o mesmo container.

⚠️ **Os dois recriam `meta-optimizer-sniper` com variáveis diferentes.** A Action
injeta só `APP_ID`, `APP_SECRET` e `REDIRECT_URI` — sem `DATABASE_URL` nem
`FLASK_SECRET_KEY`. Se rodar por último, sobe container sem banco.

Antes de todo push: **bumpar `VERSION` em `app.py`** (e `SW_VERSION` em
`static/sw.js` se o SW mudou), senão o cache serve código velho.

## Não mexa

- **`python app.py` local** — roda na VPS. Scripts podem importar os módulos,
  mas não suba o servidor aqui.
- **Segredos no repositório** — `deploy.sh`, `.env`, `notepad.env` e `token.json`
  são gitignored e é lá que as chaves vivem. Variável nova entra no `deploy.sh`
  da VPS; default no código só para o que é público.
- **Migrations já aplicadas** — são histórico. Schema muda com arquivo novo,
  datado e idempotente.
- **Tabela nova sem RLS** — todas do `public` têm RLS; as server-only ficam com
  RLS e zero policy. Nunca `FORCE ROW LEVEL SECURITY`.
- **`requests` sem `timeout=`** em caminho quente — já travou produção.
- **Código que funciona**, sem motivo declarado.

## Onde está o detalhe

- `.clauderules` — regras de conduta (mostrar opções, não deletar sem perguntar).
- `database/schema.sql` + `database/migrations/` — schema completo e histórico.
- `docs/SETUP-NOVO-PC.md` — preparar máquina nova.
- `deploy/README.md` — como o autodeploy por systemd foi instalado.
- `deploy/nginx/README.md` — DNS, nginx e TLS do subdomínio de captura.
- `ruff.toml` — regras do lint, baseline medido e dívida por arquivo.
- `README.md` — visão do produto e stack.

## Mapa

`app.py` registra 10 blueprints; o resto vive em `modules/` — rotas (`crm.py`,
`dash.py`, `lancamento_dash.py`, `cruzamento.py`), lógica pura (`crm_core.py`,
`lancamento_metrics.py`), dados (`database.py`) e clientes externos
(`meta_client.py`, `meta_capi.py`, `crm_push.py`). `meta_api.py` e `meta_150.py`
ficaram na raiz, fora de `modules/`.
