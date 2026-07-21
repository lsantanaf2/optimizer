#!/usr/bin/env bash
# ============================================================
# Optimizer — Auto-deploy por polling do git
#
# Roda via systemd timer (a cada 60s). Se origin/main tiver
# commit novo em relação ao working tree local, roda deploy.sh.
#
# Instalação: ver deploy/README.md
# ============================================================
set -euo pipefail

REPO_DIR="/var/www/optimizer"
DEPLOY_SCRIPT="$REPO_DIR/deploy.sh"
LOCK_FILE="/tmp/optimizer-autodeploy.lock"
LOG_PREFIX="[autodeploy]"

cd "$REPO_DIR"

# Keepalive do banco: /ping faz SELECT 1 best-effort. Rodando a cada ciclo
# do timer (60s), o Supabase free tier nunca pausa por inatividade.
curl -sf -m 10 "http://127.0.0.1:5000/ping" >/dev/null 2>&1 || true

# Lock: impede dois deploys em paralelo (build Docker demora > 1 min).
# flock libera sozinho se o processo morrer — sem lock órfão.
exec 200>"$LOCK_FILE"
if ! flock -n 200; then
    echo "$LOG_PREFIX deploy já em andamento — pulando este ciclo."
    exit 0
fi

# Compara local × remoto sem alterar nada
git fetch origin main --quiet
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" = "$REMOTE" ]; then
    # Nada novo — sai em silêncio (não polui o journal)
    exit 0
fi

echo "$LOG_PREFIX mudança detectada: $LOCAL → $REMOTE"
echo "$LOG_PREFIX iniciando deploy..."

git pull origin main --quiet

if bash "$DEPLOY_SCRIPT"; then
    echo "$LOG_PREFIX ✅ deploy concluído no commit $(git rev-parse --short HEAD)"
else
    echo "$LOG_PREFIX ❌ deploy FALHOU no commit $(git rev-parse --short HEAD) — verifique: journalctl -u optimizer-autodeploy"
    exit 1
fi
