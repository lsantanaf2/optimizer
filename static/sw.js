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

const SW_VERSION = 'v2.9.9';
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

    if (item.tipo === 'carousel' && Array.isArray(item.cards)) {
        fd.append('tipo_criativo', 'carousel');
        fd.append('card_count', String(item.cards.length));
        item.cards.forEach((c, ci) => {
            if (c.file) fd.append(`card_${ci}_arquivo`, c.file, c.name || `card_${ci}`);
            if (c.url) fd.append(`card_${ci}_url_remote`, c.url);
        });
    } else {
        fd.append('tipo_criativo', 'single');
        if (item.feedFile) fd.append('arquivo_feed', item.feedFile, item.feedFileName || 'feed');
        if (item.storiesFile) fd.append('arquivo_stories', item.storiesFile, item.storiesFileName || 'stories');
        if (item.urlFeed) fd.append('url_feed_remote', item.urlFeed);
        if (item.urlStories) fd.append('url_stories_remote', item.urlStories);
    }

    fd.append('page_id', ctx.pageId);
    if (ctx.igId) fd.append('instagram_actor_id', ctx.igId);
    if (ctx.pixelId) fd.append('pixel_id', ctx.pixelId);
    if (ctx.leadFormId) fd.append('lead_gen_form_id', ctx.leadFormId);

    const tStart = Date.now();
    appendLog(job, `📤 Iniciando "${item.adName}" (${index + 1}/${job.items.length})`, 'info');
    await idbPut(job);
    broadcast('job-update', { job });

    let uploadResult = null;
    let authExpired = false; // Fix #1: sinaliza para parar cascata
    try {
        const r = await fetch(`/campanha/${ctx.campaignId}/upload`, { method: 'POST', body: fd, credentials: 'include', redirect: 'manual' });
        // Fix #1: detecta 401/403/redirect para login (Flask @require_login normalmente devolve 302)
        const isAuthFail = r.status === 401 || r.status === 403
            || r.type === 'opaqueredirect'
            || (r.status >= 300 && r.status < 400)
            || (r.redirected && /\/login/i.test(r.url));
        if (isAuthFail) {
            authExpired = true;
            broadcast('auth-expired', { jobId: job.id });
            throw new Error('Sessão expirada — faça login novamente');
        }
        if (!r.ok || !r.body) throw new Error(`HTTP ${r.status}`);

        const reader = r.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const blocks = buffer.split('\n\n');
            buffer = blocks.pop();
            for (const block of blocks) {
                const line = block.split('\n').find(l => l.startsWith('data: '));
                if (!line) continue;
                let evt;
                try { evt = JSON.parse(line.slice(6)); } catch { continue; }

                if (evt.type === 'progress') {
                    const base = index / job.items.length;
                    const step = 1 / job.items.length;
                    job.progress = Math.floor((base + (evt.percent / 100) * step) * 100);
                    job.currentMessage = evt.message;
                    job.currentItemIndex = index;
                } else if (evt.type === 'log') {
                    appendLog(job, evt.message, classifyLevel(evt.message));
                } else if (evt.type === 'done') {
                    uploadResult = evt;
                    if (evt.logs) evt.logs.forEach(l => appendLog(job, l, classifyLevel(l)));
                    appendLog(job, `✅ "${item.adName}" criado — ID: ${evt.ad_id}`, 'success');
                } else if (evt.type === 'error') {
                    uploadResult = { success: false, error: evt.message };
                    if (evt.logs) evt.logs.forEach(l => appendLog(job, l, classifyLevel(l)));
                    appendLog(job, `❌ "${item.adName}": ${evt.message}`, 'error');
                }
                await idbPut(job);
                broadcastDebounced(job.id, job); // Fix H2: debounce evita spam no loop SSE
            }
        }
    } catch (e) {
        uploadResult = { success: false, error: e.message };
        appendLog(job, `❌ Erro rede "${item.adName}": ${e.message}`, 'error');
    }

    // Fix #7: flush do debounce pendente — garante que último update chegue antes de job-finished
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

    // 1. Pré-duplicação por grupo
    const preDupMap = await preDuplicateGroups(job);

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

        const res = await uploadItem(job, job.items[i], preDupMap, i);
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

async function resumeOnStart() {
    // Ao reativar o SW, verifica se havia jobs em "running" (interrompidos)
    const all = await idbAll();
    for (const j of all) {
        if (j.status === 'queued') {
            queue.push(j.id);
        } else if (j.status === 'running') {
            // Marca como interrompido — SW foi terminado mid-upload
            j.status = 'interrupted';
            j.interruptedAt = Date.now();
            appendLog(j, `⚠️ Upload interrompido (SW terminou). ${j.currentItemIndex || 0}/${j.items.length} processados.`, 'warning');
            await idbPut(j);
            broadcast('job-update', { job: j });
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
        await resumeOnStart();
    })());
});

// ==================== Message handler ====================
self.addEventListener('message', async (e) => {
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
        processNext();
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
