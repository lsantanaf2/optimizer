# Meta Ads Optimizer

Sistema automatizado para upload e otimização de anúncios na Meta Ads API, com suporte a fluxos complexos de criativos e download robusto do Google Drive.

## 🚀 Guia de Deploy (v1.3.5)

### 💻 Local (Windows / PowerShell)
Para realizar o commit e push das alterações no Windows, utilize o ponto e vírgula (`;`) para encadear os comandos, já que o operador `&&` não é suportado em versões padrão do PowerShell:

```powershell
git add .; git commit -m "Descritivo da mudança"; git push origin main
```

### 🚀 VPS (Linux / Docker)
As instruções completas, arquitetura do servidor e comandos do Docker Compose para deploy em VPS foram movidas para a Base de Conhecimento do projeto por questões de documentação.

👉 [Consulte o Guia de Deploy VPS no OPTIMIZER_PROJECT_KB.md](OPTIMIZER_PROJECT_KB.md)

## 🛠️ Tecnologias
- Python 3.10+
- Flask (Web Server)
- Facebook Business SDK
- Docker & Docker Compose
- gdown (Google Drive Download)
