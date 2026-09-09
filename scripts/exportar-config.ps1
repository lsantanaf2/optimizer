# =============================================================================
# Exporta a configuracao do Optimizer para levar a outro PC
#
# Junta numa unica pasta: arquivos secretos do projeto, memoria do Claude,
# settings globais e chaves SSH. Rode no PC que JA esta configurado.
#
#   powershell -ExecutionPolicy Bypass -File scripts\exportar-config.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\exportar-config.ps1 -Destino "E:\optimizer-config"
#
# ATENCAO: a pasta gerada contem CREDENCIAIS. Leve por pendrive ou cofre de
# senhas; nunca por WhatsApp, e-mail ou Drive. Apague depois de instalar.
# =============================================================================

param(
    [string]$Destino = "$env:USERPROFILE\Desktop\optimizer-config"
)

$ErrorActionPreference = 'Stop'
$projeto  = Split-Path -Parent $PSScriptRoot
$memoria  = "$env:USERPROFILE\.claude\projects\D--Clientes-SK-MKT-OPTIMIZER\memory"

Write-Host ""
Write-Host "=== Exportando configuracao do Optimizer ===" -ForegroundColor Cyan
Write-Host "Projeto: $projeto"
Write-Host "Destino: $Destino"
Write-Host ""

# Estrutura de pastas do pacote
New-Item -ItemType Directory -Force -Path "$Destino\projeto\.claude" | Out-Null
New-Item -ItemType Directory -Force -Path "$Destino\claude-memory"    | Out-Null
New-Item -ItemType Directory -Force -Path "$Destino\claude-global"    | Out-Null
New-Item -ItemType Directory -Force -Path "$Destino\ssh"              | Out-Null

$copiados = 0
$faltando = @()

function Copiar($origem, $destino, $rotulo) {
    if (Test-Path $origem) {
        Copy-Item $origem $destino -Force
        $tam = [math]::Round((Get-Item $origem).Length / 1KB, 1)
        Write-Host ("  [OK]    {0} ({1} KB)" -f $rotulo, $tam) -ForegroundColor Green
        return $true
    }
    Write-Host ("  [FALTA] {0}" -f $rotulo) -ForegroundColor Yellow
    return $false
}

# 1. Segredos da raiz do projeto
Write-Host "1. Arquivos secretos do projeto" -ForegroundColor White
foreach ($f in @('notepad.env', 'google_credentials.json', 'deploy.sh', '.env')) {
    if (Copiar "$projeto\$f" "$Destino\projeto\$f" $f) { $copiados++ } else { $faltando += $f }
}
if (Copiar "$projeto\.claude\settings.local.json" "$Destino\projeto\.claude\settings.local.json" ".claude/settings.local.json") {
    $copiados++
} else { $faltando += '.claude/settings.local.json' }

# 2. Memoria do Claude (historico de decisoes do projeto)
Write-Host ""
Write-Host "2. Memoria do Claude" -ForegroundColor White
if (Test-Path $memoria) {
    Copy-Item "$memoria\*" "$Destino\claude-memory\" -Recurse -Force
    $n = (Get-ChildItem "$Destino\claude-memory" -File).Count
    Write-Host ("  [OK]    {0} arquivo(s) de memoria" -f $n) -ForegroundColor Green
    $copiados += $n
} else {
    Write-Host "  [FALTA] pasta de memoria nao encontrada" -ForegroundColor Yellow
    $faltando += 'claude memory'
}

# 3. Settings globais do Claude
Write-Host ""
Write-Host "3. Configuracao global do Claude" -ForegroundColor White
if (Copiar "$env:USERPROFILE\.claude\settings.json" "$Destino\claude-global\settings.json" "settings.json") {
    $copiados++
} else { $faltando += 'claude settings.json' }

# 4. Chaves SSH da VPS
Write-Host ""
Write-Host "4. Chaves SSH da VPS" -ForegroundColor White
foreach ($k in @('id_rsa_optimizer', 'id_rsa_optimizer.pub')) {
    if (Copiar "$env:USERPROFILE\.ssh\$k" "$Destino\ssh\$k" $k) { $copiados++ } else { $faltando += $k }
}

# Instrucoes dentro do proprio pacote
@"
PACOTE DE CONFIGURACAO DO OPTIMIZER
Gerado em: $(Get-Date -Format 'dd/MM/yyyy HH:mm')

>>> ESTE PACOTE CONTEM CREDENCIAIS. Apague depois de instalar. <<<

COMO INSTALAR NO PC NOVO
1. Clone o repositorio em D:\Clientes\SK MKT\OPTIMIZER
     git clone https://github.com/lsantanaf2/optimizer.git "D:\Clientes\SK MKT\OPTIMIZER"
   (use exatamente esse caminho: a pasta de memoria do Claude depende dele)

2. Rode o importador, apontando para esta pasta:
     powershell -ExecutionPolicy Bypass -File scripts\importar-config.ps1 -Origem "<caminho desta pasta>"

3. Instale as dependencias:
     pip install -r requirements.txt
     pip install cryptography

O guia completo esta em docs\SETUP-NOVO-PC.md (vem no clone).
"@ | Out-File "$Destino\LEIA-ME.txt" -Encoding UTF8

Write-Host ""
Write-Host "=== Concluido: $copiados item(ns) em $Destino ===" -ForegroundColor Cyan
if ($faltando.Count -gt 0) {
    Write-Host "Nao encontrados: $($faltando -join ', ')" -ForegroundColor Yellow
    Write-Host "(se algum for essencial, copie manualmente antes de levar)" -ForegroundColor Yellow
}
Write-Host ""
Write-Host "LEMBRETE: a pasta contem credenciais. Pendrive ou cofre de senhas." -ForegroundColor Red
Write-Host ""
