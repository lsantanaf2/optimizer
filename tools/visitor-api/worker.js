/**
 * Visitor API própria — Cloudflare Worker
 * Retorna o mesmo JSON da visitorapi.com usando a geolocalização nativa da Cloudflare (request.cf).
 *
 * Endpoint: GET https://SEU-WORKER.workers.dev/  (ou /?pid=qualquercoisa)
 *
 * Variável opcional (Settings > Variables):
 *   ALLOWED_ORIGINS = "https://seusite.com.br,https://lp.cliente.com.br"
 *   (vazio = libera qualquer site)
 */

const COUNTRY = {
  BR: ["Brazil", ["BRL"], ["por"]],
  PT: ["Portugal", ["EUR"], ["por"]],
  US: ["United States", ["USD"], ["eng"]],
  CA: ["Canada", ["CAD"], ["eng", "fra"]],
  MX: ["Mexico", ["MXN"], ["spa"]],
  AR: ["Argentina", ["ARS"], ["spa", "grn"]],
  CL: ["Chile", ["CLP"], ["spa"]],
  CO: ["Colombia", ["COP"], ["spa"]],
  PE: ["Peru", ["PEN"], ["spa", "aym", "que"]],
  UY: ["Uruguay", ["UYU"], ["spa"]],
  PY: ["Paraguay", ["PYG"], ["spa", "grn"]],
  BO: ["Bolivia", ["BOB"], ["spa", "aym", "que"]],
  EC: ["Ecuador", ["USD"], ["spa"]],
  VE: ["Venezuela", ["VES"], ["spa"]],
  CR: ["Costa Rica", ["CRC"], ["spa"]],
  PA: ["Panama", ["PAB", "USD"], ["spa"]],
  DO: ["Dominican Republic", ["DOP"], ["spa"]],
  GT: ["Guatemala", ["GTQ"], ["spa"]],
  ES: ["Spain", ["EUR"], ["spa"]],
  FR: ["France", ["EUR"], ["fra"]],
  DE: ["Germany", ["EUR"], ["deu"]],
  IT: ["Italy", ["EUR"], ["ita"]],
  NL: ["Netherlands", ["EUR"], ["nld"]],
  BE: ["Belgium", ["EUR"], ["nld", "fra", "deu"]],
  IE: ["Ireland", ["EUR"], ["eng", "gle"]],
  AT: ["Austria", ["EUR"], ["deu"]],
  CH: ["Switzerland", ["CHF"], ["deu", "fra", "ita", "roh"]],
  GB: ["United Kingdom", ["GBP"], ["eng"]],
  SE: ["Sweden", ["SEK"], ["swe"]],
  NO: ["Norway", ["NOK"], ["nor"]],
  DK: ["Denmark", ["DKK"], ["dan"]],
  FI: ["Finland", ["EUR"], ["fin", "swe"]],
  PL: ["Poland", ["PLN"], ["pol"]],
  CZ: ["Czechia", ["CZK"], ["ces"]],
  GR: ["Greece", ["EUR"], ["ell"]],
  RO: ["Romania", ["RON"], ["ron"]],
  HU: ["Hungary", ["HUF"], ["hun"]],
  UA: ["Ukraine", ["UAH"], ["ukr"]],
  RU: ["Russia", ["RUB"], ["rus"]],
  TR: ["Turkey", ["TRY"], ["tur"]],
  IL: ["Israel", ["ILS"], ["heb", "ara"]],
  AE: ["United Arab Emirates", ["AED"], ["ara"]],
  SA: ["Saudi Arabia", ["SAR"], ["ara"]],
  EG: ["Egypt", ["EGP"], ["ara"]],
  ZA: ["South Africa", ["ZAR"], ["eng", "afr", "zul", "xho"]],
  NG: ["Nigeria", ["NGN"], ["eng"]],
  AO: ["Angola", ["AOA"], ["por"]],
  MZ: ["Mozambique", ["MZN"], ["por"]],
  CV: ["Cape Verde", ["CVE"], ["por"]],
  IN: ["India", ["INR"], ["eng", "hin"]],
  CN: ["China", ["CNY"], ["zho"]],
  JP: ["Japan", ["JPY"], ["jpn"]],
  KR: ["South Korea", ["KRW"], ["kor"]],
  HK: ["Hong Kong", ["HKD"], ["eng", "zho"]],
  TW: ["Taiwan", ["TWD"], ["zho"]],
  SG: ["Singapore", ["SGD"], ["eng", "zho", "msa", "tam"]],
  TH: ["Thailand", ["THB"], ["tha"]],
  ID: ["Indonesia", ["IDR"], ["ind"]],
  PH: ["Philippines", ["PHP"], ["eng", "fil"]],
  VN: ["Vietnam", ["VND"], ["vie"]],
  MY: ["Malaysia", ["MYR"], ["msa"]],
  AU: ["Australia", ["AUD"], ["eng"]],
  NZ: ["New Zealand", ["NZD"], ["eng", "mri"]],
};

function parseUA(ua = "") {
  const r = { browser: "", browserVersion: "", deviceBrand: "", deviceModel: "", deviceFamily: "", os: "", osVersion: "" };
  const m = (re) => ua.match(re);
  let x;

  // ---- Sistema operacional
  if ((x = m(/(?:iPhone|iPad|iPod).*?OS (\d+)[_\d]*/))) { r.os = "iOS"; r.osVersion = x[1]; }
  else if ((x = m(/Android (\d+(?:\.\d+)?)/))) { r.os = "Android"; r.osVersion = x[1]; }
  else if ((x = m(/Windows NT (\d+\.\d+)/))) {
    r.os = "Windows";
    r.osVersion = { "10.0": "10", "6.3": "8.1", "6.2": "8", "6.1": "7" }[x[1]] || x[1];
  }
  else if (/CrOS/.test(ua)) { r.os = "Chrome OS"; }
  else if ((x = m(/Mac OS X (\d+)[_.](\d+)/))) { r.os = "Mac OS X"; r.osVersion = `${x[1]}.${x[2]}`; }
  else if (/Linux/.test(ua)) { r.os = "Linux"; }

  const mobile = /Mobile|iPhone|iPod|Android/.test(ua);

  // ---- Navegador (ordem importa)
  const browsers = [
    [/Instagram (\d+)/, "Instagram"],
    [/FBAV\/(\d+)/, "Facebook"],
    [/FBAN\/(?:FBIOS|EMA)/, "Facebook"],
    [/TikTok|musical_ly|BytedanceWebview/, "TikTok"],
    [/EdgA?\/(\d+)/, "Edge"],
    [/OPR\/(\d+)/, "Opera"],
    [/SamsungBrowser\/(\d+)/, "Samsung Internet"],
    [/CriOS\/(\d+)/, "Chrome Mobile iOS"],
    [/FxiOS\/(\d+)/, "Firefox iOS"],
    [/Firefox\/(\d+)/, mobile ? "Firefox Mobile" : "Firefox"],
    [/; wv\).*Chrome\/(\d+)/, "Chrome Mobile WebView"],
    [/Chrome\/(\d+)/, mobile ? "Chrome Mobile" : "Chrome"],
    [/Version\/(\d+).*Safari/, mobile ? "Mobile Safari" : "Safari"],
  ];
  for (const [re, name] of browsers) {
    if ((x = m(re))) { r.browser = name; r.browserVersion = x[1] || ""; break; }
  }

  // ---- Aparelho (só mobile/tablet)
  if ((x = m(/(iPhone|iPad|iPod)/))) {
    r.deviceBrand = "Apple"; r.deviceModel = x[1]; r.deviceFamily = x[1];
  } else if (r.os === "Android" && (x = m(/Android [\d.]+;(.*?)\)\s+(?:AppleWebKit|Gecko|Version)/))) {
    const parts = x[1].split(";").map((s) => s.replace(/\s*Build\/.*$/, "").trim())
      .filter((s) => s && s !== "wv" && s !== "U" && !/^[a-z]{2}[-_][a-z]{2}$/i.test(s));
    let model = parts[0] || "";
    if (model === "K") model = ""; // UA reduzido do Chrome não informa modelo
    const brands = [
      [/^SM-|^GT-|Galaxy|Samsung/i, "Samsung"],
      [/moto|XT\d{4}/i, "Motorola"],
      [/Redmi|POCO|^Mi |^M\d{4}|^\d{4,5}[A-Z0-9]{3,}$|Xiaomi/i, "Xiaomi"],
      [/^LM-|LG/i, "LG"],
      [/ASUS|ZenFone/i, "Asus"],
      [/^CPH|OPPO/i, "OPPO"],
      [/^RMX|realme/i, "Realme"],
      [/^V\d{4}|vivo/i, "Vivo"],
      [/Pixel/i, "Google"],
      [/Nokia/i, "Nokia"],
      [/Infinix/i, "Infinix"],
      [/^TECNO/i, "Tecno"],
      [/HUAWEI|^[A-Z]{3}-L\d{2}/i, "Huawei"],
      [/OnePlus/i, "OnePlus"],
      [/Positivo/i, "Positivo"],
      [/Multilaser|^M7|^M8/i, "Multilaser"],
    ];
    if (model) {
      r.deviceModel = model;
      r.deviceFamily = model;
      for (const [re, b] of brands) if (re.test(model)) { r.deviceBrand = b; break; }
    }
  }
  return r;
}

function cors(request, env) {
  const origin = request.headers.get("Origin") || "";
  const list = (env.ALLOWED_ORIGINS || "").split(",").map((s) => s.trim()).filter(Boolean);
  const allowed = !list.length || list.includes(origin);
  return {
    allowed,
    headers: {
      "Access-Control-Allow-Origin": list.length ? (allowed ? origin : "null") : "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
      "Vary": "Origin",
    },
  };
}

/**
 * Reporta o hit para o painel do optimizer. Roda em ctx.waitUntil(), ou seja,
 * DEPOIS que o visitante ja recebeu a resposta: a VPS nunca entra no caminho
 * critico da pagina do cliente. Falha aqui perde metrica, nunca servico.
 */
async function reportarHit(request, env, ctx) {
  const url = env.REPORT_URL;
  const segredo = env.VISITOR_API_SECRET;
  if (!url || !segredo) return; // sem config, nao reporta

  const origin = request.headers.get("Origin") || "";
  ctx.waitUntil(
    fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Visitor-Secret": segredo },
      body: JSON.stringify({ origin }),
      signal: AbortSignal.timeout(3000),
    }).catch(() => {})
  );
}

export default {
  async fetch(request, env, ctx) {
    const c = cors(request, env);
    // Contabiliza antes de qualquer saida (inclusive a 403 de origem
    // bloqueada): dominio nao autorizado tentando usar a API e justamente
    // o que o painel precisa mostrar.
    if (request.method !== "OPTIONS") reportarHit(request, env, ctx);
    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: c.headers });
    if (!c.allowed) {
      return new Response(JSON.stringify({ status: 403, result: null, description: "Origin not allowed" }), {
        status: 403, headers: { ...c.headers, "Content-Type": "application/json" },
      });
    }

    const cf = request.cf || {};
    const cc = (cf.country || request.headers.get("CF-IPCountry") || "").toUpperCase();
    const info = COUNTRY[cc] || [cc, [], []];

    const data = {
      ipAddress: request.headers.get("CF-Connecting-IP") || "",
      countryCode: cc,
      countryName: info[0],
      currencies: info[1],
      languages: info[2],
      region: (cf.regionCode || "").toLowerCase(),
      city: (cf.city || "").toLowerCase(),
      cityLatLong: cf.latitude && cf.longitude ? `${cf.latitude},${cf.longitude}` : "",
      ...parseUA(request.headers.get("User-Agent") || ""),
      // extras que a VisitorAPI não entrega
      regionName: cf.region || "",
      postalCode: cf.postalCode || "",
      timezone: cf.timezone || "",
      asn: cf.asn || "",
      isp: cf.asOrganization || "",
    };

    // Mesmo envelope da VisitorAPI: { status, result, description }
    return new Response(JSON.stringify({ status: 200, result: data, description: "OK" }), {
      headers: { ...c.headers, "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" },
    });
  },
};
