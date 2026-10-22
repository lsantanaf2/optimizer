# Optimizer — plataforma interna da SK

Plataforma de gestão de tráfego pago: sobe criativos na Meta Ads, serve
dashboards de resultado por cliente e por lançamento, e roda o CRM de leads
próprio da agência.

**Produção:** https://optimizer.xn--trfego-qta.com (`optimizer.tráfego.com`)

> Trabalhando neste repositório com um agente de IA? Comece por
> **[AGENTS.md](AGENTS.md)** — comandos, restrições e mapa do código.

## Superfícies

| Rota | O que faz |
|---|---|
| `/setup` | Upload de criativos para a Meta em background, via Service Worker |
| `/otimizar`, `/turbinada` | Otimização e escala de campanhas |
| `/anuncios` | Listagem e gestão de anúncios |
| `/dash/<slug>?t=<token>` | Dashboard do cliente, link público com token |
| `/dash/lancamento/<slug>` | Dashboard de lançamento, separado por fase |
| `/crm` | CRM de leads, atrás de login |
| `/lp/<slug>` | Landing page de captura, pública |
| `/cruzamento` | Cruzamento Meta Ads × planilha de vendas |

## Stack

- **Python 3.11** (container; veja o `Dockerfile`)
- **Flask** com Blueprints — 10 registrados em `app.py`
- **Gunicorn** — 3 workers, 4 threads, `gthread`, timeout 600s
- **PostgreSQL** no Supabase, via pool psycopg2
- **Docker** na VPS
- Front em Jinja2 + JS puro — **sem build, sem bundler, sem TypeScript**
- Meta Ads API, Meta Conversions API, Google Ads API, Google Sheets

## Rodar e publicar

A aplicação **roda na VPS**, não localmente. Push na `main` é deploy: um timer
systemd polla o git a cada 60s e executa o `deploy.sh`, que reconstrói a imagem
e recria o container.

```bash
# Local — lembre de bumpar VERSION em app.py antes
AIOX_ACTIVE_AGENT=devops git push origin main
```

```bash
# VPS — para forçar sem esperar o timer
bash /var/www/optimizer/deploy.sh
```

No PowerShell, encadeie com `;` — o `&&` não funciona lá.

⚠️ Existe também um workflow do GitHub Actions que faz deploy por SSH. Ele
recria o mesmo container **sem passar `DATABASE_URL` nem `FLASK_SECRET_KEY`**;
se rodar por último, derruba o banco da aplicação. Detalhe em [AGENTS.md](AGENTS.md).

## Configuração

Segredos **não** vivem no repositório. `deploy.sh`, `.env`, `notepad.env` e
`token.json` estão no `.gitignore`; as variáveis são injetadas no `docker run`
dentro do `deploy.sh`, na VPS.

Principais: `DATABASE_URL`, `FLASK_SECRET_KEY`, `APP_ID`, `APP_SECRET`,
`REDIRECT_URI`, `META_CAPI_TOKEN`, `VAPID_PRIVATE_KEY`, e as `GOOGLE_ADS_*`.

`FLASK_SECRET_KEY` precisa ser **fixo**: sem ele, todo restart do container
desloga todo mundo.

## Banco

Schema completo em `database/schema.sql`; mudanças entram como arquivo novo e
datado em `database/migrations/`, idempotente. Toda tabela do schema `public`
tem RLS ligado — as de uso exclusivo do servidor ficam com RLS e **zero policy**,
o que basta porque a aplicação conecta como dona das tabelas.

## Testes

**Não há suíte de testes nem linter configurados.** `test_cruzamento_logic.py`
roda como script e imprime `SUCCESS`; `test_creative.py` é debug que chama a API
da Meta ao vivo e não deve ser executado à toa.

## Documentação

- [AGENTS.md](AGENTS.md) — comandos e restrições para agentes
- [CLAUDE.md](CLAUDE.md) — específico do Claude neste repositório
- `.clauderules` — regras de conduta
- `docs/SETUP-NOVO-PC.md` — preparar máquina nova
- `deploy/README.md` — autodeploy por systemd
- `deploy/nginx/README.md` — DNS, nginx e TLS do subdomínio de captura
