# =============================================================================
# Instala a configuracao do Optimizer neste PC
#
# Le a pasta gerada por exportar-config.ps1 e coloca cada arquivo no lugar
# certo. Rode no PC NOVO, apos o git clone.
#
#   powershell -ExecutionPolicy Bypass -File scripts\importar-config.ps1 -Origem "E:\optimizer-config"
#
# Nada e sobrescrito sem aviso: arquivos existentes viram .bak antes.
# =============================================================================

param(
    [Parameter(Mandatory = $true)]
    [string]$Origem
)

$ErrorActionPreference = 'Stop'
$projeto = Split-Path -Parent $PSScriptRoot
$memoria = "$env:USERPROFILE\.claude\projects\D--Clientes-SK-MKT-OPTIMIZER\memory"

if (-not (Test-Path $Origem)) {
    Write-Host "Pasta de origem nao encontrada: $Origem" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== Instalando configuracao do Optimizer ===" -ForegroundColor Cyan
Write-Host "Origem : $Origem"
Write-Host "Projeto: $projeto"
Write-Host ""

# Alerta se o caminho do projeto for diferente do esperado (quebra a memoria)
$esperado = 'D:\Clientes\SK MKT\OPTIMIZER'
if ($projeto -ne $esperado) {
    Write-Host "ATENCAO: o projeto esta em" -ForegroundColor Yellow
    Write-Host "  $projeto" -ForegroundColor Yellow
    Write-Host "mas o esperado e" -ForegroundColor Yellow
    Write-Host "  $esperado" -ForegroundColor Yellow
    Write-Host "A memoria do Claude e vinculada ao caminho. Em outro caminho," -ForegroundColor Yellow
    Write-Host "renomeie a pasta em ~\.claude\projects\ conforme o novo path." -ForegroundColor Yellow
    Write-Host ""
}

$instalados = 0

function Instalar($origem, $destino, $rotulo) {
    if (-not (Test-Path $origem)) {
        Write-Host ("  [PULA]  {0} (nao veio no pacote)" -f $rotulo) -ForegroundColor DarkGray
        return $false
    }
    $pasta = Split-Path -Parent $destino
    if (-not (Test-Path $pasta)) { New-Item -ItemType Directory -Force -Path $pasta | Out-Null }
    if (Test-Path $destino) {
        Copy-Item $destino "$destino.bak" -Force
        Write-Host ("  [BAK]   {0} existente salvo como .bak" -f $rotulo) -ForegroundColor DarkYellow
    }
    Copy-Item $origem $destino -Force
    Write-Host ("  [OK]    {0}" -f $rotulo) -ForegroundColor Green
    return $true
}

# 1. Segredos na raiz do projeto
Write-Host "1. Arquivos secretos do projeto" -ForegroundColor White
foreach ($f in @('notepad.env', 'google_credentials.json', 'deploy.sh', '.env')) {
    if (Instalar "$Origem\projeto\$f" "$projeto\$f" $f) { $instalados++ }
}
if (Instalar "$Origem\projeto\.claude\settings.local.json" "$projeto\.claude\settings.local.json" ".claude/settings.local.json") {
    $instalados++
}

# 2. Memoria do Claude
Write-Host ""
Write-Host "2. Memoria do Claude" -ForegroundColor White
if (Test-Path "$Origem\claude-memory") {
    New-Item -ItemType Directory -Force -Path $memoria | Out-Null
    Copy-Item "$Origem\claude-memory\*" "$memoria\" -Recurse -Force
    $n = (Get-ChildItem $memoria -File).Count
    Write-Host ("  [OK]    {0} arquivo(s) em {1}" -f $n, $memoria) -ForegroundColor Green
    $instalados += $n
} else {
    Write-Host "  [PULA]  memoria nao veio no pacote" -ForegroundColor DarkGray
}

# 3. Settings globais
Write-Host ""
Write-Host "3. Configuracao global do Claude" -ForegroundColor White
if (Instalar "$Origem\claude-global\settings.json" "$env:USERPROFILE\.claude\settings.json" "settings.json") {
    $instalados++
}

# 4. Chaves SSH (permissao restrita: o SSH recusa chave "publica demais")
Write-Host ""
Write-Host "4. Chaves SSH da VPS" -ForegroundColor White
foreach ($k in @('id_rsa_optimizer', 'id_rsa_optimizer.pub')) {
    if (Instalar "$Origem\ssh\$k" "$env:USERPROFILE\.ssh\$k" $k) {
        $instalados++
        if ($k -notlike '*.pub') {
            $caminho = "$env:USERPROFILE\.ssh\$k"
            icacls $caminho /inheritance:r | Out-Null
            icacls $caminho /grant:r "$($env:USERNAME):(R)" | Out-Null
            Write-Host "          permissoes restritas aplicadas" -ForegroundColor DarkGray
        }
    }
}

Write-Host ""
Write-Host "=== Concluido: $instalados item(ns) instalado(s) ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "PROXIMOS PASSOS" -ForegroundColor White
Write-Host "  1. pip install -r requirements.txt"
Write-Host "  2. pip install cryptography"
Write-Host "  3. py -3 test_cruzamento_logic.py     (deve imprimir SUCCESS)"
Write-Host "  4. Abrir o Claude Code nesta pasta e pedir: 'leia a memoria do projeto'"
Write-Host ""
Write-Host "Depois de validar, APAGUE a pasta de origem: ela contem credenciais." -ForegroundColor Red
Write-Host ""
