/**
 * Optimizer Upload Service Worker
 * v2.8.0 — uploads em background que sobrevivem à navegação
 *
 * Responsabilidades:
 * - Recebe jobs via postMessage da página
 * - Persiste jobs em IndexedDB (sobrevivem reload/F5)
 * - Processa sequencialmente (Meta rate-limit safe)
 * - Faz fetch + SSE streaming do endpoint /upload
 * - Pré-duplicação de adsets é feita aqui (sobrevive navegação)
 * - Broadcasts progresso via BroadcastChannel('optimizer-uploads')
 * - Registra histórico ao final (/api/upload-history/add)
 */

const SW_VERSION = 'v2.12.0';
const DB_NAME = 'optimizer-uploads';
const DB_VERSION = 1;
const STORE_JOBS = 'jobs';
const CHANNEL_NAME = 'optimizer-uploads';

// ==================== IndexedDB helpers ====================
let _dbConn = null; // Cache de conexão para lifetime do SW (fix H1: evita 500+ aberturas por upload)
function openDb() {
    if (_dbConn) return Promise.resolve(_dbConn);
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(DB_NAME, DB_VERSION);
        req.onupgradeneeded = (e) => {
            const db = e.target.result;
            if (!db.objectStoreNames.contains(STORE_JOBS)) {
                db.createObjectStore(STORE_JOBS, { keyPath: 'id' });
            }
        };
        req.onsuccess = () => {
            _dbConn = req.result;
            // Fix #3: invalida cache se conexão for fechada (Safari background, quota, upgrade)
            _dbConn.onclose = () => { _dbConn = null; };
            _dbConn.onversionchange = () => { try { _dbConn.close(); } catch (_) {} _dbConn = null; };
            resolve(_dbConn);
        };
        req.onerror = () => reject(req.error);
    });
}

async function idbPut(job) {
    try {
        const db = await openDb();
        return await new Promise((resolve, reject) => {
            const tx = db.transaction(STORE_JOBS, 'readwrite');
            tx.objectStore(STORE_JOBS).put(job);
            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);
        });
    } catch (e) {
        // Fix M7: IDB quota excedida (Safari/iOS ~50MB) — falha graciosamente
        if (e && e.name === 'QuotaExceededError') {
            broadcast('quota-exceeded', { jobId: job.id, label: job.label });
        }
        throw e;
    }
}

async function idbGet(id) {
    const db = await openDb();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(STORE_JOBS, 'readonly');
        const req = tx.objectStore(STORE_JOBS).get(id);
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}

async function idbAll() {
    const db = await openDb();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(STORE_JOBS, 'readonly');
        const req = tx.objectStore(STORE_JOBS).getAll();
        req.onsuccess = () => resolve(req.result || []);
        req.onerror = () => reject(req.error);
    });
}

async function idbDelete(id) {
    const db = await openDb();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(STORE_JOBS, 'readwrite');
        tx.objectStore(STORE_JOBS).delete(id);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    });
}

// ==================== Broadcast ====================
const channel = new BroadcastChannel(CHANNEL_NAME);
function broadcast(type, payload) {
    try { channel.postMessage({ type, ...payload }); } catch (_) {}
}

// Fix H2: debounce de broadcasts no loop SSE — evita spam de payloads crescentes
const _broadcastDebounce = new Map();
function broadcastDebounced(jobId, job, delayMs) {
    delayMs = delayMs || 200;
    if (_broadcastDebounce.has(jobId)) clearTimeout(_broadcastDebounce.get(jobId));
    _broadcastDebounce.set(jobId, setTimeout(() => {
        _broadcastDebounce.delete(jobId);
        broadcast('job-update', { job });
    }, delayMs));
}

// ==================== Queue state ====================
let processing = false;
const queue = [];

// ==================== Job processing ====================
async function appendLog(job, message, level) {
    job.logs = job.logs || [];
    job.logs.push({ ts: Date.now(), message, level: level || 'info' });
    if (job.logs.length > 500) job.logs = job.logs.slice(-500);
}

function _preDupKey(ctx) {
    const modelo = ctx.estrategia === 'novo' ? ctx.adsetModeloNovo : ctx.adsetModelo;
    const nome = ctx.estrategia === 'novo' ? ctx.adsetNewNameNovo : ctx.adsetNewName;
    // Fix #4: inclui excludedCountries para não compartilhar adset pré-dup com exclusões diferentes
    const excl = ctx.excludedCountries || '';
    return `${ctx.accountId}|${ctx.campaignId}|${modelo}|${ctx.estrategia}|${nome}|${excl}`;
}
function _precisaPreDup(ctx) {
    return (ctx.estrategia === 'agrupado' && ctx.destino === 'duplicar') || ctx.estrategia === 'novo';
}

async function preDuplicateGroups(job) {
    const preDupMap = {};
    const seen = new Set();
    const grupos = [];

    for (const item of job.items) {
        const ctx = item.context;
        if (!_precisaPreDup(ctx)) continue;
        const k = _preDupKey(ctx);
        if (seen.has(k)) continue;
        seen.add(k);
        grupos.push({ key: k, ctx });
    }

    for (const g of grupos) {
        const ctx = g.ctx;
        const modelo = ctx.estrategia === 'novo' ? ctx.adsetModeloNovo : ctx.adsetModelo;
        const nome = ctx.estrategia === 'novo' ? ctx.adsetNewNameNovo : ctx.adsetNewName;
        if (!modelo) {
            appendLog(job, `❌ Pré-dup pulada (sem conjunto modelo) — conta ${ctx.accountId}`, 'error');
            preDupMap[g.key] = null;
            continue;
        }
        appendLog(job, `📋 Pré-duplicando conjunto para ${ctx.accountId}/${ctx.campaignName || ctx.campaignId}`, 'info');
        try {
            const fd = new FormData();
            fd.append('account_id', ctx.accountId);
            fd.append('adset_modelo', modelo);
            fd.append('adset_name', nome || '');
            fd.append('adset_status', 'PAUSED');
            if (ctx.excludedCountries) fd.append('excluded_countries', ctx.excludedCountries);
            if (ctx.adsetStartTime) fd.append('start_time', ctx.adsetStartTime);
            const r = await fetch(`/campanha/${ctx.campaignId}/duplicate-adset`, { method: 'POST', body: fd, credentials: 'include' });
            const data = await r.json();
            if (data.success && data.adset_id) {
                preDupMap[g.key] = data.adset_id;
                appendLog(job, `✅ Conjunto duplicado: ${data.adset_id}${nome ? ' — ' + nome : ''}`, 'success');
            } else {
                preDupMap[g.key] = null;
                appendLog(job, `❌ Falha pré-dup: ${data.error}`, 'error');
                // Despeja logs detalhados do uploader (Meta error_user_msg, params, etc.)
                if (Array.isArray(data.logs)) {
                    data.logs.slice(-30).forEach(l => appendLog(job, l, classifyLevel(l)));
                }
            }
        } catch (e) {
            preDupMap[g.key] = null;
            appendLog(job, `❌ Erro rede pré-dup: ${e.message}`, 'error');
        }
        await idbPut(job);
        broadcast('job-update', { job });
    }

    return preDupMap;
}

async function duplicarGarimpo(job, item) {
    const ctx = item.context;
    const modelo = ctx.adsetModeloGarimpo;
    if (!modelo) {
        appendLog(job, `❌ "${item.adName}": conjunto modelo garimpo não definido`, 'error');
        return null;
    }
    try {
        const fd = new FormData();
        fd.append('account_id', ctx.accountId);
        fd.append('adset_modelo', modelo);
        fd.append('adset_name', item.adName);
        fd.append('adset_status', 'PAUSED');
        if (ctx.excludedCountries) fd.append('excluded_countries', ctx.excludedCountries);
        if (ctx.adsetStartTime) fd.append('start_time', ctx.adsetStartTime);
        const r = await fetch(`/campanha/${ctx.campaignId}/duplicate-adset`, { method: 'POST', body: fd, credentials: 'include' });
        const data = await r.json();
        if (data.success && data.adset_id) {
            appendLog(job, `✅ Conjunto garimpo "${item.adName}": ${data.adset_id}`, 'success');
            return data.adset_id;
        }
        appendLog(job, `❌ Garimpo falhou para "${item.adName}": ${data.error}`, 'error');
        if (Array.isArray(data.logs)) {
            data.logs.slice(-30).forEach(l => appendLog(job, l, classifyLevel(l)));
        }
        return null;
    } catch (e) {
        appendLog(job, `❌ Erro rede garimpo: ${e.message}`, 'error');
        return null;
    }
}

// v2.11.0: Sobe UM arquivo para o staging da VPS e devolve o stage_id.
// Desacopla o transfer browser→VPS do request SSE de criação do anúncio,
// evitando o transfer duplo dentro do timeout do Gunicorn (300s).
async function stageFile(campaignId, file, displayName) {
    const fd = new FormData();
    fd.append('file', file, displayName || 'media');

    // v2.11.1: watchdog no staging (o SSE já tinha; o staging não). Sem isso, um
    // upload browser→VPS que estagna ficava pendurado pra sempre, sem erro nem
    // progresso. Timeout dimensionado pelo tamanho (mín 3min, +1min por 50MB).
    const sizeMb = (file && file.size ? file.size : 0) / (1024 * 1024);
    const timeoutMs = Math.max(180000, Math.ceil(sizeMb / 50) * 60000 + 120000);
    const controller = new AbortController();
    const timerId = setTimeout(() => { try { controller.abort(); } catch (_) {} }, timeoutMs);

    let r;
    try {
        r = await fetch(`/campanha/${campaignId}/upload/stage`, {
            method: 'POST',
            body: fd,
            credentials: 'include',
            redirect: 'manual',
            signal: controller.signal,
        });
    } catch (netErr) {
        clearTimeout(timerId);
        if (netErr && netErr.name === 'AbortError') {
            throw new Error(`staging abortado por inatividade (${Math.round(timeoutMs / 1000)}s)`);
        }
        throw netErr;
    }
    clearTimeout(timerId);

    if (r.status === 401 || r.status === 403 || r.type === 'opaqueredirect'
        || (r.status >= 300 && r.status < 400)
        || (r.redirected && /\/login/i.test(r.url))) {
        const e = new Error('Sessão expirada — faça login novamente');
        e.authExpired = true;
        throw e;
    }
    if (!r.ok) throw new Error(`stage HTTP ${r.status}`);
    const data = await r.json();
    if (!data || !data.success || !data.stage_id) {
        throw new Error((data && data.error) || 'staging falhou');
    }
    return data.stage_id;
}

async function uploadItem(job, item, preDupMap, index) {
    const ctx = item.context;
    if (!ctx.pageId) {
        appendLog(job, `❌ "${item.adName}": sem page_id no contexto`, 'error');
        return { success: false, error: 'sem page_id' };
    }

    // Resolver adsetId final (garimpo > pré-dup > contexto)
    let adsetIdParaEsteAnuncio = null;
    if (ctx.estrategia === 'garimpo') {
        adsetIdParaEsteAnuncio = await duplicarGarimpo(job, item);
        if (!adsetIdParaEsteAnuncio) return { success: false, error: 'duplicacao garimpo falhou' };
    }

    const fd = new FormData();
    fd.append('account_id', ctx.accountId);
    fd.append('estrategia', ctx.estrategia);

    if (adsetIdParaEsteAnuncio) {
        fd.append('destino_conjunto', 'existente');
        fd.append('adset_existente', adsetIdParaEsteAnuncio);
        fd.append('adset_modelo', '');
    } else if (_precisaPreDup(ctx)) {
        const dupId = preDupMap[_preDupKey(ctx)];
        if (!dupId) {
            appendLog(job, `❌ "${item.adName}": pré-dup do grupo falhou — pulando`, 'error');
            return { success: false, error: 'pre-dup group falhou' };
        }
        fd.append('destino_conjunto', 'existente');
        fd.append('adset_existente', dupId);
        fd.append('adset_modelo', '');
    } else {
        fd.append('destino_conjunto', ctx.destino || '');
        fd.append('adset_existente', ctx.adsetExistente || '');
        fd.append('adset_modelo', ctx.adsetModelo || '');
    }

    fd.append('ad_name', item.adName);
    fd.append('ad_status', 'PAUSED');
    fd.append('url_destino', ctx.url);
    fd.append('utm_pattern', ctx.utms || '');
    fd.append('cta', ctx.cta || '');
    (ctx.textos || []).forEach(t => fd.append('primary_text[]', t));
    (ctx.titulos || []).forEach(t => fd.append('headline[]', t));
    if (ctx.excludedCountries) fd.append('excluded_countries', ctx.excludedCountries);

    // v2.11.0: STAGING — sobe cada arquivo UMA vez pra VPS antes do SSE.
    // O request SSE de criação só referencia stage_id (sem carregar o vídeo de novo).
    try {
        if (item.tipo === 'carousel' && Array.isArray(item.cards)) {
            fd.append('tipo_criativo', 'carousel');
            fd.append('card_count', String(item.cards.length));
            for (let ci = 0; ci < item.cards.length; ci++) {
                const c = item.cards[ci];
                if (c.file) {
                    appendLog(job, `📥 Enviando card ${ci + 1} de "${item.adName}" para a VPS...`, 'info');
                    const sid = await stageFile(ctx.campaignId, c.file, c.name || `card_${ci}`);
                    fd.append(`card_${ci}_stage_id`, sid);
                }
                if (c.url) fd.append(`card_${ci}_url_remote`, c.url);
            }
        } else {
            fd.append('tipo_criativo', 'single');
            if (item.feedFile) {
                appendLog(job, `📥 Enviando feed de "${item.adName}" para a VPS...`, 'info');
                const sid = await stageFile(ctx.campaignId, item.feedFile, item.feedFileName || 'feed');
                fd.append('feed_stage_id', sid);
            }
            if (item.storiesFile) {
                appendLog(job, `📥 Enviando stories de "${item.adName}" para a VPS...`, 'info');
                const sid = await stageFile(ctx.campaignId, item.storiesFile, item.storiesFileName || 'stories');
                fd.append('stories_stage_id', sid);
            }
            if (item.urlFeed) fd.append('url_feed_remote', item.urlFeed);
            if (item.urlStories) fd.append('url_stories_remote', item.urlStories);
        }
    } catch (stageErr) {
        if (stageErr && stageErr.authExpired) {
            broadcast('auth-expired', { jobId: job.id });
            job.status = 'cancelled';
            appendLog(job, `⏸ "${item.adName}": sessão expirada no envio à VPS. Faça login e re-enfileire.`, 'warning');
            await idbPut(job);
            return { success: false, error: 'auth expirada', authExpired: true };
        }
        appendLog(job, `❌ "${item.adName}": falha ao enviar mídia para a VPS — ${stageErr.message}`, 'error');
        return { success: false, error: `staging: ${stageErr.message}` };
    }

    fd.append('page_id', ctx.pageId);
    if (ctx.igId) fd.append('instagram_actor_id', ctx.igId);
    if (ctx.pixelId) fd.append('pixel_id', ctx.pixelId);
    if (ctx.leadFormId) fd.append('lead_gen_form_id', ctx.leadFormId);

    // v2.12.0: idempotência — chave estável por item (sobrevive à morte do SW).
    // Um retry com a mesma job_key NUNCA recria o anúncio (servidor devolve 'done').
    const jobKey = `${job.id}__${index}`;
    fd.append('job_key', jobKey);

    const tStart = Date.now();
    appendLog(job, `📤 Iniciando "${item.adName}" (${index + 1}/${job.items.length})`, 'info');
    await idbPut(job);
    broadcast('job-update', { job });

    let uploadResult = null;
    let authExpired = false; // Fix #1: sinaliza para parar cascata

    const isAuthFail = (r) => r.status === 401 || r.status === 403
        || r.type === 'opaqueredirect'
        || (r.status >= 300 && r.status < 400)
        || (r.redirected && /\/login/i.test(r.url));

    try {
        // 1) START — dispara o job no servidor e recebe job_id NA HORA.
        // v2.12.0: não há mais stream de minutos preso a esta conexão. O servidor
        // roda em background gravando estado em disco; nós só fazemos polling curto.
        const startResp = await fetch(`/campanha/${ctx.campaignId}/upload/start`, {
            method: 'POST',
            body: fd,
            credentials: 'include',
            redirect: 'manual',
        });
        if (isAuthFail(startResp)) {
            authExpired = true;
            broadcast('auth-expired', { jobId: job.id });
            throw new Error('Sessão expirada — faça login novamente');
        }
        if (!startResp.ok) throw new Error(`start HTTP ${startResp.status}`);
        const startData = await startResp.json();
        if (!startData || !startData.success) {
            throw new Error((startData && startData.error) || 'start falhou');
        }
        const serverJobId = startData.job_id;

        if (startData.status === 'done' && startData.ad_id) {
            // Idempotência: item já concluído (retry/resume) — não recria.
            uploadResult = { success: true, ad_id: startData.ad_id };
            appendLog(job, `✅ "${item.adName}" já criado — ID: ${startData.ad_id}`, 'success');
        } else {
            // 2) POLL — requests curtos que sobrevivem à morte do SW.
            uploadResult = await pollUploadStatus(job, item, index, ctx.campaignId, serverJobId);
            if (uploadResult && uploadResult.authExpired) authExpired = true;
        }
    } catch (e) {
        uploadResult = { success: false, error: e.message };
        appendLog(job, `❌ Erro "${item.adName}": ${e.message}`, 'error');
    }

    // Flush do debounce pendente — garante que último update chegue antes de job-finished
    if (_broadcastDebounce.has(job.id)) {
        clearTimeout(_broadcastDebounce.get(job.id));
        _broadcastDebounce.delete(job.id);
        broadcast('job-update', { job });
    }

    // Fix #1: se auth expirou, marca o job como cancelado para parar cascata de 401s
    if (authExpired) {
        job.status = 'cancelled';
        appendLog(job, `⏸ Fila pausada por sessão expirada. Faça login e re-enfileire os itens restantes.`, 'warning');
        await idbPut(job);
    }

    const dur = formatDur(Date.now() - tStart);
    // Registra histórico
    try {
        await fetch('/api/upload-history/add', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({
                campaign_id: ctx.campaignId,
                ad_name: item.adName,
                status: (uploadResult && uploadResult.success) ? 'ok' : 'erro',
                ad_id: (uploadResult && uploadResult.ad_id) || '',
                erro: (uploadResult && !uploadResult.success) ? (uploadResult.error || 'Falha') : '',
                duracao: dur,
            })
        });
    } catch (_) {}

    return uploadResult || { success: false, error: 'sem resposta' };
}

// v2.12.0: polling do estado do job no servidor. Requests curtos (~2s) — o que
// mantém o SW vivo MUITO melhor que um único stream de minutos, e sobrevive à
// morte do SW: o estado mora no servidor (arquivo em disco), então ao reacordar
// retomamos o polling do mesmo job_id (resumeOnStart re-enfileira o job).
async function pollUploadStatus(job, item, index, campaignId, serverJobId) {
    const POLL_INTERVAL_MS = 2000;
    const STALE_MS = 300000; // 5min sem progresso no servidor → desiste deste item
    let lastLogCount = 0;
    let lastProgressAt = Date.now();
    let lastSeenUpdatedAt = 0;

    while (true) {
        // job pode ter sido cancelado externamente
        const fresh = await idbGet(job.id);
        if (fresh && fresh.status === 'cancelled') {
            return { success: false, error: 'cancelado' };
        }

        await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));

        let resp;
        try {
            resp = await fetch(`/campanha/${campaignId}/upload/status/${serverJobId}`, {
                method: 'GET',
                credentials: 'include',
                redirect: 'manual',
            });
        } catch (netErr) {
            // erro de rede transitório → continua tentando até STALE_MS
            if (Date.now() - lastProgressAt > STALE_MS) {
                return { success: false, error: 'sem resposta do servidor (5min)' };
            }
            continue;
        }

        if (resp.status === 401 || resp.status === 403 || resp.type === 'opaqueredirect'
            || (resp.status >= 300 && resp.status < 400)
            || (resp.redirected && /\/login/i.test(resp.url))) {
            broadcast('auth-expired', { jobId: job.id });
            return { success: false, error: 'sessão expirada', authExpired: true };
        }
        if (resp.status === 404) {
            // arquivo de estado sumiu (sweep ou worker reiniciado sem o job)
            if (Date.now() - lastProgressAt > STALE_MS) {
                return { success: false, error: 'job perdido no servidor' };
            }
            continue;
        }
        if (!resp.ok) continue;

        let st;
        try { st = await resp.json(); } catch (_) { continue; }

        // Novas linhas de log (delta sobre o que já mostramos)
        if (Array.isArray(st.logs) && st.logs.length > lastLogCount) {
            for (let k = lastLogCount; k < st.logs.length; k++) {
                appendLog(job, st.logs[k], classifyLevel(st.logs[k]));
            }
            lastLogCount = st.logs.length;
        }

        // Progresso na barra
        if (typeof st.percent === 'number') {
            const base = index / job.items.length;
            const step = 1 / job.items.length;
            job.progress = Math.floor((base + (st.percent / 100) * step) * 100);
            job.currentMessage = st.message;
            job.currentItemIndex = index;
            await idbPut(job);
            broadcastDebounced(job.id, job);
        }

        // Heartbeat do servidor: updated_at avançou → thread viva. Congelou por
        // 5min → thread morta no servidor (raro) → desiste deste item.
        if (st.updated_at && st.updated_at !== lastSeenUpdatedAt) {
            lastSeenUpdatedAt = st.updated_at;
            lastProgressAt = Date.now();
        } else if (Date.now() - lastProgressAt > STALE_MS) {
            return { success: false, error: 'servidor sem progresso há 5min' };
        }

        if (st.status === 'done') {
            appendLog(job, `✅ "${item.adName}" criado — ID: ${st.ad_id}`, 'success');
            return { success: true, ad_id: st.ad_id };
        }
        if (st.status === 'error') {
            if (st.geo) {
                broadcast('geo-compliance-error', { jobId: job.id, ...st.geo });
            }
            appendLog(job, `❌ "${item.adName}": ${st.error || 'erro'}`, 'error');
            return { success: false, error: st.error || 'erro' };
        }
        // status 'running' → continua o loop de polling
    }
}

function classifyLevel(msg) {
    if (!msg) return 'info';
    const l = msg.toLowerCase();
    if (l.includes('❌') || l.includes('erro') || l.includes('falha')) return 'error';
    if (l.includes('✅') || l.includes('sucesso') || l.includes('criado')) return 'success';
    if (l.includes('⚠') || l.includes('aviso')) return 'warning';
    return 'info';
}

function formatDur(ms) {
    const s = Math.floor(ms / 1000);
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    return `${m}m${s % 60}s`;
}

async function processJob(jobId) {
    let job = await idbGet(jobId);
    if (!job) return;
    if (job.status === 'cancelled' || job.status === 'completed') return;

    job.status = 'running';
    job.startedAt = job.startedAt || Date.now();
    await idbPut(job);
    broadcast('job-update', { job });

    // 1. Pré-duplicação por grupo (v2.9.14: try/catch impede travamento da fila)
    let preDupMap = {};
    try {
        preDupMap = await preDuplicateGroups(job);
    } catch (e) {
        appendLog(job, `❌ Erro fatal na pré-duplicação: ${e && e.message ? e.message : e} — fila continuará (ads podem usar conjuntos originais)`, 'error');
        await idbPut(job);
        broadcast('job-update', { job });
    }

    // 2. Loop de upload
    let enviados = 0, erros = 0;
    for (let i = 0; i < job.items.length; i++) {
        job = await idbGet(jobId);
        if (!job) break; // Fix C1: job deletado externamente durante loop — TypeError seguro
        if (job.status === 'cancelled') {
            appendLog(job, `⏹ Cancelado. ${enviados} OK, ${job.items.length - i} restante(s)`, 'warning');
            await idbPut(job);
            break;
        }

        // v2.9.14: try/catch garante que exceção isolada NÃO mate a fila inteira
        let res;
        try {
            res = await uploadItem(job, job.items[i], preDupMap, i);
        } catch (e) {
            const msg = e && e.message ? e.message : String(e);
            appendLog(job, `❌ Exceção fatal em "${job.items[i].adName}": ${msg} — pulando para o próximo`, 'error');
            await idbPut(job);
            broadcast('job-update', { job });
            res = { success: false, error: msg };
        }
        if (res && res.success) enviados++; else erros++;

        // Delay entre ads
        if (i < job.items.length - 1 && job.status !== 'cancelled') {
            await new Promise(r => setTimeout(r, 2000));
        }
    }

    // 3. Finalizar
    job = await idbGet(jobId);
    if (job) {
        job.status = job.status === 'cancelled' ? 'cancelled' : 'completed';
        job.completedAt = Date.now();
        job.progress = 100;
        job.enviados = enviados;
        job.erros = erros;
        appendLog(job, `🏁 Job ${job.label}: ${enviados} ✅ / ${erros} ❌`, enviados > 0 ? 'success' : 'error');
        await idbPut(job);
        broadcast('job-update', { job });
        broadcast('job-finished', { job });
    }
}

async function processNext() {
    if (processing) return;
    processing = true;
    try {
        while (queue.length > 0) {
            const jobId = queue.shift();
            await processJob(jobId);
        }
    } finally {
        processing = false;
    }
}

let _resumed = false; // garante que a retomada rode no máximo 1x por lifetime do SW

async function ensureResumed() {
    if (_resumed) return;
    _resumed = true;
    try { await resumeOnStart(); } catch (_) {}
}

async function resumeOnStart() {
    // v2.12.0: o SW pode ter sido MORTO pelo Chrome no meio de um lote (o vetor de
    // travamento que perseguíamos). Como o estado real do upload agora mora no
    // SERVIDOR e cada item é idempotente (job_key), ao reacordar nós re-enfileiramos
    // o job em andamento e re-processamos do início: itens já concluídos retornam
    // 'done' na hora (sem recriar o anúncio), e o item interrompido é retomado via
    // polling. NÃO mexemos no job que ESTE lifetime já está processando (processing).
    const all = await idbAll();
    for (const j of all) {
        if (j.status === 'queued') {
            if (!queue.includes(j.id)) queue.push(j.id);
        } else if (j.status === 'running' && !processing && !queue.includes(j.id)) {
            appendLog(j, `🔄 Retomando upload após reativação do SW (${j.currentItemIndex || 0}/${j.items.length})...`, 'info');
            j.status = 'queued';
            await idbPut(j);
            broadcast('job-update', { job: j });
            queue.push(j.id);
        }
    }
    if (queue.length > 0) processNext();
}

// ==================== SW lifecycle ====================
self.addEventListener('install', (e) => {
    self.skipWaiting();
});

self.addEventListener('activate', (e) => {
    e.waitUntil((async () => {
        await self.clients.claim();
        await ensureResumed();
    })());
});

// ==================== Message handler ====================
self.addEventListener('message', async (e) => {
    // v2.12.0: qualquer mensagem da página (ping/get-jobs/enqueue) acorda o SW —
    // aproveitamos para retomar jobs órfãos de um lifetime anterior morto pelo Chrome.
    // waitUntil estende a vida do SW durante o resume (best-effort) para o batch
    // retomado não morrer de novo no mesmo evento.
    if (e.waitUntil) {
        e.waitUntil(ensureResumed());
    } else {
        ensureResumed();
    }

    const { type, payload } = e.data || {};

    if (type === 'enqueue') {
        const job = {
            ...payload,
            status: 'queued',
            createdAt: Date.now(),
            progress: 0,
            currentItemIndex: 0,
            logs: [],
            enviados: 0,
            erros: 0,
        };
        await idbPut(job);
        queue.push(job.id);
        broadcast('job-enqueued', { job });
        broadcast('job-update', { job });
        // v2.12.0: waitUntil estende a vida do SW pelo lote inteiro (best-effort).
        // Se o Chrome matar mesmo assim, resumeOnStart retoma na próxima mensagem.
        if (e.waitUntil) {
            e.waitUntil(processNext());
        } else {
            processNext();
        }
        if (e.source) e.source.postMessage({ type: 'enqueued', jobId: job.id });

    } else if (type === 'get-jobs') {
        const jobs = await idbAll();
        if (e.source) e.source.postMessage({ type: 'jobs-list', jobs });

    } else if (type === 'cancel-job') {
        const j = await idbGet(payload.id);
        if (j && (j.status === 'queued' || j.status === 'running')) {
            j.status = 'cancelled';
            await idbPut(j);
            broadcast('job-update', { job: j });
        }

    } else if (type === 'delete-job') {
        await idbDelete(payload.id);
        broadcast('job-deleted', { id: payload.id });

    } else if (type === 'clear-finished') {
        const all = await idbAll();
        for (const j of all) {
            if (j.status === 'completed' || j.status === 'cancelled' || j.status === 'interrupted') {
                await idbDelete(j.id);
            }
        }
        broadcast('jobs-cleared', {});

    } else if (type === 'ping') {
        if (e.source) e.source.postMessage({ type: 'pong', version: SW_VERSION });
    }
});
