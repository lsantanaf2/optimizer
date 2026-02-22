# Guia de Diagnóstico de Infraestrutura (VPS)

Execute estes comandos no terminal da VPS e me envie o resultado para identificarmos o "assassino de conexões" de 60 segundos.

### 1. Testar Conectividade Direta (CURL)
Isso testa se o problema é o Python ou a rede da VPS.
```bash
curl -v -I https://graph.facebook.com/v22.0/
```

### 2. Testar Fragmentação de Pacotes (MTU)
Se o pacote for grande demais para o roteador, a conexão cai. Teste com um pacote de 1472 bytes.
```bash
ping -M do -s 1472 -c 4 graph.facebook.com
```
*Se der "Frag needed", o seu MTU está configurado errado na rede da Vivo/VPS.*

### 3. Verificar IPv6 (O grande vilão)
Muitas vezes o IPv6 está configurado mas a rota está quebrada. Tente forçar IPv4:
```bash
curl -4 -v -I https://graph.facebook.com/v22.0/
```

### 4. Rastrear a Rota (MTR)
Verifica onde o pacote "morre" no caminho para a Meta.
```bash
mtr -rw graph.facebook.com
```
*(Se não tiver mtr instalado, use `traceroute graph.facebook.com`)*

### 5. Verificar logs do Docker
Veja se o container está cuspindo algum erro de baixo nível:
```bash
docker logs --tail 50 meta-optimizer-sniper
```
