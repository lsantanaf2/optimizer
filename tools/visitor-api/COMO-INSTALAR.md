# Visitor API propria (substitui a visitorapi.com)

Custo: R$ 0 - Cloudflare Workers gratis (100 mil requisicoes/dia).

**No ar em: https://visitor.ads-lsantanaf.workers.dev/** (deployado em 2026-11-03)

Redeploy depois de mexer no `worker.js`:

    cd tools/visitor-api
    npx wrangler deploy

Regerar o minificado depois de mexer no `visitorapi.js`:

    npx esbuild visitorapi.js --minify --target=es2017 --outfile=visitorapi.min.js

O `ALLOWED_ORIGINS` esta comentado no `wrangler.toml`: com a lista preenchida,
request sem header Origin (curl) volta 403.

## 1. Subir o Worker (5 min)
1. Crie conta grátis em https://dash.cloudflare.com
2. Menu **Workers & Pages** → **Create** → **Create Worker** → nome `visitor` → **Deploy**
3. **Edit code** → apague tudo → cole o conteúdo de `worker.js` → **Deploy**
4. Copie a URL (ex.: `https://visitor.ads-lsantanaf.workers.dev`) e abra no navegador — deve aparecer o JSON com sua cidade.

Opcional (evitar que outros usem sua API): **Settings → Variables** → `ALLOWED_ORIGINS` = `https://site1.com.br,https://site2.com.br`

## 2. Trocar nas páginas
No `visitorapi.js`, troque `ENDPOINT` pela URL do seu Worker.

Onde hoje tem o script da VisitorAPI, coloque:
```html
<script>/* cole aqui o conteúdo do visitorapi.js */</script>
```
O resto do seu código continua igual — a função `VisitorAPI(id, sucesso, erro)` e a versão com `.then()` funcionam do mesmo jeito.

## 3. GTM
O script já dispara o evento `visitor_data` no dataLayer com as variáveis:
`visitor_city`, `visitor_region`, `visitor_countryCode`, `visitor_ipAddress`, `visitor_os`, `visitor_browser`, `visitor_deviceBrand`, `visitor_deviceModel` etc.

- Tag HTML personalizada com o script, disparando em **All Pages**
- Crie variáveis "Variável da camada de dados" com esses nomes
- Use o acionador "Evento personalizado" = `visitor_data`

Se você usava o template oficial da VisitorAPI no GTM, os nomes das variáveis/evento lá eram outros — ajuste os acionadores pra `visitor_data`.

## Campos
Mesmos da VisitorAPI: `ipAddress, countryCode, countryName, currencies, languages, region, city, cityLatLong, browser, browserVersion, deviceBrand, deviceModel, deviceFamily, os, osVersion`

Extras grátis: `regionName` (ex.: "São Paulo"), `postalCode`, `timezone`, `isp`, `asn`.
Bônus: detecta navegador interno do **Instagram, Facebook e TikTok** (campo `browser`).

## Diferenças
- `city` vem em minúsculo igual à VisitorAPI ("são vicente"); se quiser capitalizado, use `regionName`/trate no front.
- Lista de moeda/idioma cobre ~60 países (todo LATAM, Europa, EUA, principais da Ásia). País fora da lista retorna moeda vazia.
- Precisão de cidade por IP é parecida com qualquer serviço de IP (no celular 4G às vezes cai na cidade da operadora).
