# Meta Ads Optimizer

Sistema automatizado para upload e otimização de anúncios na Meta Ads API, com suporte a fluxos complexos de criativos e download robusto do Google Drive.

## 🚀 Guia de Deploy (v1.3.5)

### 💻 Local (Windows / PowerShell)
Para realizar o commit e push das alterações no Windows, utilize o ponto e vírgula (`;`) para encadear os comandos, já que o operador `&&` não é suportado em versões padrão do PowerShell:

```powershell
git add .; git commit -m "Descritivo da mudança"; git push origin main
```

### 🚀 VPS (Linux / Docker)
Acesse a VPS via SSH e execute:

```bash
cd /var/www/optimizer
git pull origin main
docker build -t optimizer-image:auto .
docker rm -f meta-optimizer-sniper || true
docker run -d --name meta-optimizer-sniper -p 5000:5000 \
  -e APP_ID="seu_app_id" \
  -e APP_SECRET="seu_app_secret" \
  -e REDIRECT_URI="https://seu-dominio.com/callback" \
  --restart always optimizer-image:auto
```

## 🛠️ Tecnologias
- Python 3.10+
- Flask (Web Server)
- Facebook Business SDK
- Docker & Docker Compose
- gdown (Google Drive Download)
