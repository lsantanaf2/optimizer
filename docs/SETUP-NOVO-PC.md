# Setup do Optimizer em um novo PC

Guia para colocar o projeto rodando em outra máquina com **todos os acessos e o
contexto** que a máquina original tem.

> Este arquivo está no repositório de propósito: depois do `git clone`, ele já
> está disponível no PC novo. **Nenhum segredo é escrito aqui** — apenas os
> nomes dos arquivos que você precisa levar.

---

## 1. Contexto do projeto (leia antes de mexer)

**Optimizer** é um SaaS Flask multi-tenant para automação de Meta Ads, com duas
superfícies:

1. **`/setup`** — criação de anúncios em lote, via Service Worker em background
2. **`/dash`** — dashboards por cliente, com link público (`?t=<token>`)

| Item | Valor |
|---|---|
| Produção | https://optimizer.xn--trfego-qta.com |
| Repositório | https://github.com/lsantanaf2/optimizer.git |
| Caminho local (PC original) | `D:\Clientes\SK MKT\OPTIMIZER` |
| Caminho na VPS | `/var/www/optimizer` |
| Stack | Python 3.11 · Flask · Gunicorn (4w) · Docker · PostgreSQL (Supabase) |
| Frontend | HTML + CSS + JS vanilla + Jinja2 (sem build step) |

### Dashboards ativos

| Cliente | URL | Template |
|---|---|---|
| VINCI | `/dash/vinci?t=<token>` | `cruzamento.html` (Meta × Sheets) |
| BANDOG | `/dash/bandog?t=<token>` | `dash_meta.html` (Meta-only) |
| Faturamento Hotmart | `/dash/faturamento` | `dash_faturamento.html` (público) |
| Lançamento LP11 | `/dash/lancamento/lp11` | `dash_lancamento.html` (público) |

### Como fazer deploy (importante)

**Não existe passo manual na VPS.** Um timer systemd verifica o Git a cada 60s
e faz o deploy sozinho:

```bash
# Só isso. A VPS detecta, builda e sobe em ~2-4 min.
AIOX_ACTIVE_AGENT=devops git push origin main
```

- Sempre incremente `VERSION` em `app.py` antes do push.
- Para forçar um redeploy sem mudar código:
  `git commit --allow-empty -m "deploy: force" && git push`
- Durante o restart o site dá **502 por ~30-60s** — é normal, não é falha.
- Diagnóstico rápido: `https://optimizer.xn--trfego-qta.com/ping` mostra
  `[db:ok]` ou `[db:down]`.

### Armadilhas que já custaram tempo

| Sintoma | Causa real |
|---|---|
| Dash 403 + login 500, app de pé | Supabase pausado por inatividade (free tier). Hoje há keepalive automático, mas se voltar: dar *Resume* no painel do Supabase |
| Deploy não sobe, versão travada | O `git pull` abortava por alteração local. Já corrigido com `git reset --hard` no autodeploy |
| Token Meta inválido após trocar `APP_SECRET` | A chave Fernet deriva do `APP_SECRET` — todos precisam reconectar o Facebook |
| Taxas do funil acima de 100% | `action_types` da Meta se sobrepõem. Usar **prioridade**, nunca somar |
| Erro `DSH-101` na tela do cliente | Link antigo. Códigos `DSH-1xx` estão em `modules/dash.py` |

### Estado atual (v2.31.0)

- Conformidade com o Meta Platform Terms implementada (throttle, cache, rate
  limit, data-deletion, privacy/terms, imposto de 12,15% em todos os custos)
- Dash de lançamento LP11 com funil, tabela diária e aba Instagram
- Pendências em aberto: grid de criativos com flag de fadiga, motor de
  diagnóstico, métricas do painel VINCI (alcance ÷ público e CPM por
  posicionamento), deploy sem janela de 502

---

## 2. O que o `git clone` já traz

Código, `CLAUDE.md`, e toda a pasta `.claude/` (agents, commands, hooks, rules,
skills, `settings.json`). **Nada a fazer** além de clonar:

```bash
git clone https://github.com/lsantanaf2/optimizer.git "D:\Clientes\SK MKT\OPTIMIZER"
```

> **Use exatamente este caminho.** O nome da pasta de memória do Claude é
> derivado dele (`D--Clientes-SK-MKT-OPTIMIZER`). Caminho diferente = memória
> não encontrada.

---

## 3. Arquivos que você precisa levar na mão

Estão no `.gitignore` e **não vêm no clone**. Sem eles nada funciona.

### 3.1 Na raiz do projeto

| Arquivo | Conteúdo |
|---|---|
| `notepad.env` | `APP_ID`, `APP_SECRET`, `REDIRECT_URI`, `FLASK_SECRET_KEY`, `GOOGLE_ADS_*` |
| `google_credentials.json` | Service Account do Google (lê as planilhas) |
| `deploy.sh` | Script de deploy com credenciais de produção |
| `.env` | `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `SUPABASE_*`, `GITHUB_TOKEN`, `CLICKUP_API_KEY` … |
| `.claude/settings.local.json` | Permissões locais do Claude neste projeto |

### 3.2 Memória do Claude (o mais esquecido)

Pasta inteira, do PC antigo para o novo:

```
C:\Users\<usuario>\.claude\projects\D--Clientes-SK-MKT-OPTIMIZER\memory\
```

Contém `MEMORY.md` e `meta-compliance-plan.md` — histórico de decisões,
incidentes e soluções. Sem isso o Claude recomeça do zero.

### 3.3 Configuração global do Claude

```
C:\Users\<usuario>\.claude\settings.json
```

Permissões pré-aprovadas (evita confirmar comando a comando), `effortLevel` e tema.

**Não** copie: `history.jsonl`, `sessions/`, `cache/`, `shell-snapshots/`,
`.credentials.json` (esse é recriado no login).

### 3.4 Chaves SSH da VPS

```
C:\Users\<usuario>\.ssh\id_rsa_optimizer
C:\Users\<usuario>\.ssh\id_rsa_optimizer.pub
```

Com o auto-deploy quase não se usa, mas guarde para emergências.

> ⚠️ **Segurança:** o `.env` tem `SUPABASE_SERVICE_ROLE_KEY` e `GITHUB_TOKEN`
> (acesso total). Transfira por pendrive ou cofre de senhas —
> **nunca por WhatsApp, e-mail ou Drive**.

---

## 4. Conectores / MCPs — nada a fazer

Não há MCP configurado localmente. Os conectores (Meta Ads, Google Drive, Gmail,
ClickUp, Supermetrics) pertencem à **conta do claude.ai**, não à máquina.
Basta entrar com o mesmo e-mail.

---

## 5. Dependências

```bash
pip install -r requirements.txt
pip install cryptography
```

O `cryptography` **não está** no `requirements.txt` mas é necessário para ler
planilhas via Service Account nos testes locais.

Confirme também que o launcher `py -3` existe (os scripts de validação o usam).

---

## 6. Regras do projeto que valem em qualquer máquina

- ❌ **Nunca** rodar `python app.py` local — todo teste é em produção, após deploy
- ✅ Push sempre com `AIOX_ACTIVE_AGENT=devops` (há um hook que bloqueia sem isso)
- ✅ Incrementar `VERSION` em `app.py` antes de cada push
- ✅ Bumpar `SW_VERSION` em `static/sw.js` quando mexer no Service Worker

Testes que rodam localmente (não sobem servidor):

```bash
py -3 test_cruzamento_logic.py
py -3 verify_payload.py
```

---

## 7. Checklist final

- [ ] `git clone` em `D:\Clientes\SK MKT\OPTIMIZER`
- [ ] Copiar os 5 arquivos secretos (item 3.1)
- [ ] Copiar a pasta `memory/` do Claude (item 3.2)
- [ ] Copiar `~/.claude/settings.json` (item 3.3)
- [ ] Copiar as chaves SSH (item 3.4)
- [ ] `pip install -r requirements.txt && pip install cryptography`
- [ ] Abrir o Claude Code na pasta e pedir: *"leia a memória do projeto"*
- [ ] Validar: `py -3 test_cruzamento_logic.py` deve imprimir `SUCCESS`
- [ ] Validar: abrir `https://optimizer.xn--trfego-qta.com/ping` → `[db:ok]`

Para empacotar tudo automaticamente, use os scripts em `scripts/`:

```powershell
# No PC ATUAL — gera a pasta para levar no pendrive
powershell -ExecutionPolicy Bypass -File scripts\exportar-config.ps1

# No PC NOVO — instala nos lugares certos
powershell -ExecutionPolicy Bypass -File scripts\importar-config.ps1 -Origem "E:\optimizer-config"
```
