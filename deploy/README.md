# Auto-deploy — Optimizer VPS

Serviço systemd que checa o `origin/main` a cada 60s e roda `deploy.sh`
automaticamente quando detecta commit novo.

**Depois de instalado, o fluxo de deploy vira apenas:**

```
git push  →  (até 60s)  →  VPS deploya sozinha
```

Para forçar um redeploy sem mudança de código (ex.: env var alterada no deploy.sh):

```bash
git commit --allow-empty -m "deploy: force redeploy" && git push
```

## Instalação (uma única vez, na VPS)

```bash
cd /var/www/optimizer
git pull origin main

# 1. Script executável
chmod +x deploy/autodeploy.sh

# 2. Instala as units do systemd
sudo cp deploy/optimizer-autodeploy.service /etc/systemd/system/
sudo cp deploy/optimizer-autodeploy.timer   /etc/systemd/system/
sudo systemctl daemon-reload

# 3. Ativa o timer (inicia agora + a cada boot)
sudo systemctl enable --now optimizer-autodeploy.timer

# 4. Confirma
systemctl list-timers optimizer-autodeploy.timer
```

## Operação

```bash
# Ver logs dos deploys automáticos
journalctl -u optimizer-autodeploy -n 50 --no-pager

# Acompanhar em tempo real
journalctl -u optimizer-autodeploy -f

# Pausar o auto-deploy (ex.: manutenção manual)
sudo systemctl stop optimizer-autodeploy.timer

# Reativar
sudo systemctl start optimizer-autodeploy.timer

# Rodar um ciclo manualmente (teste)
sudo systemctl start optimizer-autodeploy.service
```

## Como funciona

- `optimizer-autodeploy.timer` dispara o service a cada 60s
- `autodeploy.sh` faz `git fetch` e compara `HEAD` local × `origin/main`
  - Iguais → sai em silêncio (não polui o journal)
  - Diferentes → `git pull` + `bash deploy.sh`
- `flock` garante que dois deploys nunca rodem em paralelo
  (build Docker demora mais que o intervalo do timer)
- O systemd também não sobrepõe execuções do mesmo service (Type=oneshot)

## Segurança

- Nenhuma porta nova exposta, nenhum secret novo criado
- O canal de comando é o próprio git (pull-only — a VPS só lê o GitHub)
- `deploy.sh` (com credenciais) continua existindo apenas na VPS
