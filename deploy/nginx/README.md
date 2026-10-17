# Subdomínio de captura — `cadastro.tráfego.com`

Passo a passo para colocar a landing page do CRM num subdomínio próprio.
Ordem importa: **DNS → nginx → certbot**. O certbot só emite o certificado
depois que o DNS já resolve e a porta 80 responde.

`tráfego.com` é um domínio com acento, então em qualquer painel ou comando o
host real é a forma punycode: **`cadastro.xn--trfego-qta.com`**.

---

## 1. DNS (painel da avatar4040)

Os nameservers do domínio são `ns1.avatar4040.com.br` / `ns2.avatar4040.com.br`,
então o registro é criado lá (não na Cloudflare).

| Campo | Valor |
|-------|-------|
| Tipo  | `A` |
| Nome / Host | `cadastro` (só isso — o painel completa o domínio) |
| Aponta para | `187.77.34.92` (mesmo IP de `optimizer`) |
| TTL | o menor disponível (300s) enquanto testa |

Conferir a propagação antes de seguir:

```bash
nslookup cadastro.xn--trfego-qta.com
```

Tem que devolver `187.77.34.92`. Se ainda devolver o IP antigo ou nada, esperar
o TTL — seguir para o certbot antes disso só gera erro.

---

## 2. nginx (na VPS)

```bash
cd /var/www/optimizer && git pull origin main
sudo cp deploy/nginx/cadastro.conf /etc/nginx/sites-available/cadastro.conf
sudo ln -sf /etc/nginx/sites-available/cadastro.conf /etc/nginx/sites-enabled/cadastro.conf
sudo nginx -t && sudo systemctl reload nginx
```

> `nginx -t` vai reclamar de `ssl_certificate` faltando no bloco 443. É esperado
> antes do certbot. Se o reload falhar por isso, comente o bloco `server` do 443
> inteiro, rode o certbot (passo 3) e descomente depois — ou use o comando do
> passo 3, que cria o bloco sozinho.

---

## 3. Certificado (Let's Encrypt)

```bash
sudo certbot --nginx -d cadastro.xn--trfego-qta.com
```

O certbot valida pela porta 80, edita o arquivo inserindo `ssl_certificate` e
recarrega o nginx. A renovação automática já existe na máquina (o mesmo timer
que renova `optimizer.xn--trfego-qta.com`).

Conferir:

```bash
curl -I https://cadastro.xn--trfego-qta.com
sudo certbot certificates | grep -A2 cadastro
```

---

## 4. O que fica disponível nesse host

O roteamento por host está em `modules/crm.py` (`LP_HOSTS` / `_lp_por_host`):

| URL | Resultado |
|-----|-----------|
| `https://cadastro.tráfego.com/` | LP do form `sk_captura_01` (slug `sk`) |
| `https://cadastro.tráfego.com/<slug>` | LP de qualquer outro `crm_forms.slug` ativo |
| `https://cadastro.tráfego.com/obrigado` | página de agradecimento |
| `POST /api/crm/lead` | captura (mesma origem, sem CORS) |
| `/crm`, `/login`, qualquer outra coisa | **404** — o painel não é exposto neste host |

Criar uma LP nova é só inserir uma linha em `crm_forms` com um `slug`; não
precisa tocar em DNS, nginx nem código:

```sql
INSERT INTO crm_forms (form_key, name, slug)
VALUES ('black_2026', 'Captura Black 2026', 'black');
-- fica em https://cadastro.tráfego.com/black
```

Para mudar o slug que aparece na raiz, ou servir a LP em mais de um host, use as
env vars no `deploy.sh` da VPS (ambas opcionais):

```bash
-e CRM_LP_HOSTS="cadastro.xn--trfego-qta.com,lp.xn--trfego-qta.com" \
-e CRM_LP_DEFAULT_SLUG="sk" \
```

---

## 5. URL para usar nos anúncios

```text
https://cadastro.xn--trfego-qta.com/?utm_source=meta&utm_medium=paid&utm_campaign={{campaign.name}}&utm_content={{ad.name}}&ad_id={{ad.id}}&adset_id={{adset.id}}&campaign_id={{campaign.id}}
```

Pode digitar `cadastro.tráfego.com` no gerenciador — a Meta converte para
punycode sozinha. As macros são o que amarra cada lead ao anúncio específico e
o que vai permitir devolver a conversão pela CAPI depois.
