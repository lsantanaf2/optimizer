/**
 * Optimizer Upload Manager — v2.9.0
 * Interface client-side para o Service Worker de uploads.
 * - Registra SW, envia jobs, escuta progresso via BroadcastChannel
 * - Renderiza painel flutuante global (sobrevive navegação)
 *
 * Fixes v2.9.0:
 *  C2 — aguarda controllerchange antes de postMessage (race condition primeira visita)
 *  H3 — debounce de _refresh() + render incremental (sem flicker)
 *  H4 — painel expande logs por job
 *  H5 — notificação de nova versão disponível (SW updatefound)
 *  M3 — painel em bottom:80px para não sobrepor toast (bottom:24px)
 *  M6 — modal de sessão expirada ao receber auth-expired
 */
(function(global) {
    const CHANNEL_NAME = 'optimizer-uploads';
    const channel = new BroadcastChannel(CHANNEL_NAME);
    const listeners = [];

    channel.addEventListener('message', (e) => {
        listeners.forEach(l => { try { l(e.data); } catch (_) {} });
    });

    let swRegPromise = null;

    async function registerSw() {
        if (swRegPromise) return swRegPromise;
        if (!('serviceWorker' in navigator)) {
            return Promise.reject(new Error('Service Worker não suportado'));
        }
        swRegPromise = (async () => {
            const reg = await navigator.serviceWorker.register('/sw.js', { scope: '/' });

            // Fix H5: notifica quando nova versão do SW é instalada
            reg.addEventListener('updatefound', () => {
                const newSw = reg.installing;
                if (!newSw) return;
                newSw.addEventListener('statechange', () => {
                    if (newSw.state === 'installed' && navigator.serviceWorker.controller) {
                        const toast = global.mostrarToast;
                        if (typeof toast === 'function') {
                            toast('🔄 Nova versão disponível. Recarregue a página para atualizar.');
                        }
                    }
                });
            });

            await navigator.serviceWorker.ready;

            // Fix C2: na primeira visita, controller é null até claim() rodar no activate
            // Aguarda controllerchange antes de tentar postMessage
            if (!navigator.serviceWorker.controller) {
                await new Promise(resolve => {
                    navigator.serviceWorker.addEventListener('controllerchange', resolve, { once: true });
                });
            }
            return navigator.serviceWorker.controller;
        })();
        return swRegPromise;
    }

    function _sendToSw(message, transferable) {
        return navigator.serviceWorker.ready.then(reg => {
            const target = navigator.serviceWorker.controller || reg.active;
            if (!target) throw new Error('Service Worker sem controller');
            if (transferable && transferable.length) {
                target.postMessage(message, transferable);
            } else {
                target.postMessage(message);
            }
        });
    }

    function _request(message, responseType, timeoutMs) {
        return new Promise((resolve, reject) => {
            let done = false;
            const handler = (e) => {
                if (e.data && e.data.type === responseType && !done) {
                    done = true;
                    navigator.serviceWorker.removeEventListener('message', handler);
                    resolve(e.data);
                }
            };
            navigator.serviceWorker.addEventListener('message', handler);
            _sendToSw(message).catch(reject);
            if (timeoutMs) {
                setTimeout(() => {
                    if (!done) {
                        done = true;
                        navigator.serviceWorker.removeEventListener('message', handler);
                        reject(new Error('SW timeout'));
                    }
                }, timeoutMs);
            }
        });
    }

    // Fix M4: flag para prevenir double-click no enqueue
    let _enqueueing = false;

    async function enqueue(job) {
        if (_enqueueing) throw new Error('Enqueue já em andamento');
        _enqueueing = true;
        try {
            await registerSw();
            const id = 'job_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
            const payload = { ...job, id };
            const res = await _request({ type: 'enqueue', payload }, 'enqueued', 10000);
            return res.jobId || id;
        } finally {
            _enqueueing = false;
        }
    }

    async function getJobs() {
        await registerSw();
        const res = await _request({ type: 'get-jobs' }, 'jobs-list', 5000);
        return res.jobs || [];
    }

    async function cancelJob(id) {
        await registerSw();
        await _sendToSw({ type: 'cancel-job', payload: { id } });
    }

    async function deleteJob(id) {
        await registerSw();
        await _sendToSw({ type: 'delete-job', payload: { id } });
    }

    async function clearFinished() {
        await registerSw();
        await _sendToSw({ type: 'clear-finished' });
    }

    function onEvent(cb) {
        listeners.push(cb);
        return () => {
            const i = listeners.indexOf(cb);
            if (i >= 0) listeners.splice(i, 1);
        };
    }

    // ==================== Global Panel ====================
    // Fix H4: rastreia quais jobs estão com logs expandidos
    const _expandedJobs = new Set();

    const PANEL_HTML = `
<div id="upload-panel-global" style="display:none;">
    <div class="upg-header">
        <span class="upg-title">📤 <span id="upg-count">0</span> upload(s)</span>
        <div class="upg-actions">
            <button class="upg-btn-clear" onclick="UploadManager.clearFinished()" title="Limpar concluídos">🧹</button>
            <button class="upg-btn-toggle" onclick="UploadManager.togglePanel()" title="Minimizar">–</button>
        </div>
    </div>
    <div class="upg-body" id="upg-body"></div>
</div>`;

    const PANEL_CSS = `
#upload-panel-global {
    position: fixed;
    /* Fix M3: bottom 80px para não sobrepor toast (bottom:24px + altura do toast) */
    bottom: 80px; right: 20px; z-index: 9999;
    width: 380px; max-height: 65vh;
    background: #161925; border: 1px solid #2a2f40; border-radius: 12px;
    box-shadow: 0 12px 40px rgba(0,0,0,0.45);
    font-family: 'Lexend', system-ui, sans-serif; color: #e0e0e0;
    overflow: hidden;
    display: flex; flex-direction: column;
}
#upload-panel-global.minimized .upg-body { display: none; }
#upload-panel-global.minimized { width: auto; }
.upg-header {
    padding: 10px 14px; background: linear-gradient(90deg, #7f5af0 0%, #4f9cf9 100%);
    display: flex; align-items: center; justify-content: space-between;
    font-weight: 600; font-size: 13px; color: #fff;
    cursor: default;
}
.upg-title { flex: 1; }
.upg-actions { display: flex; gap: 4px; }
.upg-actions button {
    background: rgba(255,255,255,0.15); border: none; color: #fff;
    width: 24px; height: 24px; border-radius: 4px; cursor: pointer;
    font-size: 12px; display: flex; align-items: center; justify-content: center;
}
.upg-actions button:hover { background: rgba(255,255,255,0.28); }
.upg-body {
    padding: 10px; overflow-y: auto; flex: 1;
}
.upg-job {
    background: #1f2333; border: 1px solid #2d3346; border-radius: 8px;
    padding: 10px 12px; margin-bottom: 8px; font-size: 12px;
}
.upg-job-head {
    display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;
}
.upg-job-label { font-weight: 600; color: #e0e0e0; font-size: 12px; flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.upg-job-status {
    padding: 2px 8px; border-radius: 10px; font-size: 10px; font-weight: 600; margin-left: 8px;
}
.upg-st-queued { background: #3a2f1a; color: #fbbf24; }
.upg-st-running { background: #1a2a3a; color: #60a5fa; }
.upg-st-completed { background: #1a3a2a; color: #10b981; }
.upg-st-cancelled { background: #3a1a1a; color: #f87171; }
.upg-st-interrupted { background: #3a2a1a; color: #f59e0b; }
.upg-bar {
    height: 4px; background: #0f1218; border-radius: 2px; overflow: hidden; margin: 6px 0;
}
.upg-bar-fill { height: 100%; background: linear-gradient(90deg, #7f5af0, #4f9cf9); transition: width 0.3s; }
.upg-job-meta { color: #9ba3b8; font-size: 11px; display: flex; justify-content: space-between; }
.upg-job-msg { color: #cbd5e1; margin-top: 4px; font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.upg-job-actions { display: flex; gap: 6px; margin-top: 6px; align-items: center; }
.upg-job-actions button {
    background: #2a2f40; border: 1px solid #3a4056; color: #cbd5e1;
    padding: 3px 8px; border-radius: 4px; font-size: 10px; cursor: pointer;
}
.upg-job-actions button:hover { background: #3a4056; }
/* Fix H4: logs expandidos por job */
.upg-logs {
    margin-top: 8px; max-height: 160px; overflow-y: auto;
    background: #0f1218; border-radius: 6px; padding: 6px 8px;
    font-size: 10px; font-family: monospace; color: #9ba3b8;
}
.upg-log-line { padding: 1px 0; white-space: pre-wrap; word-break: break-all; }
.upg-log-success { color: #10b981; }
.upg-log-error { color: #f87171; }
.upg-log-warning { color: #f59e0b; }
.upg-log-info { color: #9ba3b8; }
#upload-panel-toggle {
    position: fixed; bottom: 80px; right: 20px; z-index: 9998;
    width: 50px; height: 50px; border-radius: 50%;
    background: linear-gradient(135deg, #7f5af0, #4f9cf9); color: #fff;
    border: none; cursor: pointer; font-size: 20px;
    box-shadow: 0 6px 20px rgba(127,90,240,0.5);
    display: none; align-items: center; justify-content: center;
}
#upload-panel-toggle .badge {
    position: absolute; top: -4px; right: -4px; background: #ef4444;
    color: #fff; font-size: 10px; font-weight: 700;
    min-width: 18px; height: 18px; border-radius: 9px;
    display: flex; align-items: center; justify-content: center; padding: 0 4px;
}
`;

    function _statusClass(s) {
        return 'upg-st-' + (s || 'queued');
    }

    // Fix H3: render incremental — só atualiza jobs que mudaram, sem substituir DOM inteiro
    function _render(jobs) {
        const panel = document.getElementById('upload-panel-global');
        if (!panel) return;
        const body = document.getElementById('upg-body');
        const count = document.getElementById('upg-count');

        const visible = jobs.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
        count.textContent = visible.length;

        if (visible.length === 0) {
            panel.style.display = 'none';
            return;
        }
        panel.style.display = 'flex';

        // Mapeia jobs existentes no DOM pelo id
        const existingEls = {};
        body.querySelectorAll('.upg-job[data-job-id]').forEach(el => {
            existingEls[el.dataset.jobId] = el;
        });

        // Remove jobs que sumiram da lista
        const visibleIds = new Set(visible.map(j => j.id));
        Object.keys(existingEls).forEach(id => {
            if (!visibleIds.has(id)) existingEls[id].remove();
        });

        // Cria ou atualiza cada job
        visible.forEach((j, idx) => {
            const html = _jobHtml(j);
            if (existingEls[j.id]) {
                // Atualiza in-place (evita perder scroll e estado de expand)
                const el = existingEls[j.id];
                // Só regera se alguma coisa mudou
                const newProgress = Math.min(100, Math.max(0, j.progress || 0));
                const fill = el.querySelector('.upg-bar-fill');
                if (fill) fill.style.width = newProgress + '%';

                const statusEl = el.querySelector('.upg-job-status');
                if (statusEl) {
                    statusEl.className = 'upg-job-status ' + _statusClass(j.status);
                    statusEl.textContent = _stLabel(j.status);
                }

                const metaEl = el.querySelector('.upg-job-meta');
                if (metaEl) {
                    const enviados = j.enviados || 0, erros = j.erros || 0;
                    metaEl.innerHTML = `<span>${j.items ? j.items.length : 0} ads · ✅ ${enviados} / ❌ ${erros}</span><span>${newProgress}%</span>`;
                }

                const msgEl = el.querySelector('.upg-job-msg');
                if (msgEl) msgEl.textContent = j.currentMessage || '';

                // Atualiza logs se expandido
                if (_expandedJobs.has(j.id)) {
                    _updateLogs(el, j);
                }

                // Reposiciona se necessário (sort por data)
                const refChild = body.children[idx];
                if (refChild && refChild !== el) body.insertBefore(el, refChild);
            } else {
                // Novo job — insere no DOM
                const wrap = document.createElement('div');
                wrap.innerHTML = html;
                const el = wrap.firstElementChild;
                const refChild = body.children[idx];
                if (refChild) body.insertBefore(el, refChild);
                else body.appendChild(el);
            }
        });
    }

    function _stLabel(s) {
        return { queued: 'Na fila', running: 'Enviando', completed: 'Concluído',
                 cancelled: 'Cancelado', interrupted: 'Interrompido' }[s] || s;
    }

    function _jobHtml(j) {
        const prog = Math.min(100, Math.max(0, j.progress || 0));
        const msg = j.currentMessage || '';
        const enviados = j.enviados || 0, erros = j.erros || 0;
        const isLive = j.status === 'running' || j.status === 'queued';
        const isExpanded = _expandedJobs.has(j.id);
        const logsHtml = isExpanded ? _logsHtml(j) : '';
        return `
            <div class="upg-job" data-job-id="${j.id}">
                <div class="upg-job-head">
                    <span class="upg-job-label" title="${_esc(j.label || j.id)}">${_esc(j.label || j.id)}</span>
                    <span class="upg-job-status ${_statusClass(j.status)}">${_stLabel(j.status)}</span>
                </div>
                <div class="upg-bar"><div class="upg-bar-fill" style="width:${prog}%;"></div></div>
                <div class="upg-job-meta">
                    <span>${j.items ? j.items.length : 0} ads · ✅ ${enviados} / ❌ ${erros}</span>
                    <span>${prog}%</span>
                </div>
                ${msg ? `<div class="upg-job-msg">${_esc(msg)}</div>` : ''}
                <div class="upg-job-actions">
                    ${isLive ? `<button onclick="UploadManager.cancelJob('${j.id}')">⏹ Cancelar</button>` : ''}
                    ${!isLive ? `<button onclick="UploadManager.deleteJob('${j.id}')">🗑 Remover</button>` : ''}
                    <button onclick="UploadManager._toggleLogs('${j.id}')">${isExpanded ? '🔼 Ocultar logs' : '🔽 Ver logs'}</button>
                </div>
                ${logsHtml}
            </div>`;
    }

    function _logsHtml(j) {
        const logs = (j.logs || []).slice(-100); // últimas 100 linhas
        if (!logs.length) return '<div class="upg-logs"><span style="color:#555">Sem logs ainda.</span></div>';
        const lines = logs.map(l =>
            `<div class="upg-log-line upg-log-${l.level || 'info'}">${_esc(l.message)}</div>`
        ).join('');
        return `<div class="upg-logs" id="upg-logs-${j.id}">${lines}</div>`;
    }

    function _updateLogs(jobEl, j) {
        const existing = jobEl.querySelector('.upg-logs');
        const newHtml = _logsHtml(j);
        if (existing) {
            const wasAtBottom = existing.scrollTop >= existing.scrollHeight - existing.clientHeight - 10;
            existing.outerHTML = newHtml;
            // Re-selecionar após substituição
            const updated = jobEl.querySelector('.upg-logs');
            if (updated && wasAtBottom) updated.scrollTop = updated.scrollHeight;
        } else {
            jobEl.insertAdjacentHTML('beforeend', newHtml);
        }
    }

    // Fix H4: toggle logs de um job específico
    function _toggleLogs(jobId) {
        if (_expandedJobs.has(jobId)) {
            _expandedJobs.delete(jobId);
        } else {
            _expandedJobs.add(jobId);
        }
        _refresh();
    }

    function _esc(s) {
        const d = document.createElement('div');
        d.textContent = String(s == null ? '' : s);
        return d.innerHTML;
    }

    function togglePanel() {
        const p = document.getElementById('upload-panel-global');
        if (p) p.classList.toggle('minimized');
    }

    // Fix H3: debounce de _refresh para evitar flicker
    let _refreshTimer = null;
    function _scheduleRefresh() {
        if (_refreshTimer) clearTimeout(_refreshTimer);
        _refreshTimer = setTimeout(_refresh, 150);
    }

    async function _refresh() {
        _refreshTimer = null;
        try {
            const jobs = await getJobs();
            _render(jobs);
        } catch (_) {}
    }

    // Injeção do painel global
    function _injectPanel() {
        if (document.getElementById('upload-panel-global')) return;
        const style = document.createElement('style');
        style.textContent = PANEL_CSS;
        document.head.appendChild(style);
        const wrap = document.createElement('div');
        wrap.innerHTML = PANEL_HTML;
        document.body.appendChild(wrap.firstElementChild);
    }

    // Escuta eventos do SW e atualiza painel
    onEvent((msg) => {
        if (msg.type === 'job-update' || msg.type === 'job-enqueued' ||
            msg.type === 'job-finished' || msg.type === 'job-deleted' ||
            msg.type === 'jobs-cleared') {
            _scheduleRefresh(); // Fix H3: debounce
        }

        if (msg.type === 'job-finished' && msg.job) {
            const toast = global.mostrarToast;
            if (typeof toast === 'function') {
                const j = msg.job;
                if (j.enviados > 0 && j.erros === 0) {
                    toast(`🎉 ${j.enviados} anúncio(s) enviados com sucesso!`);
                } else if (j.erros > 0) {
                    toast(`⚠️ ${j.enviados} OK, ${j.erros} erro(s) — veja logs no painel`);
                }
            }
        }

        // Fix M6: sessão expirada durante upload em background
        if (msg.type === 'auth-expired') {
            const toast = global.mostrarToast;
            if (typeof toast === 'function') {
                toast('⚠️ Sessão expirada durante upload. Recarregue e faça login novamente.');
            }
            // Tenta mostrar modal de login se disponível na página
            const loginBtn = document.querySelector('[data-action="logout"], [href*="login"]');
            if (!loginBtn) {
                // Fallback: banner persistente
                let banner = document.getElementById('upg-auth-banner');
                if (!banner) {
                    banner = document.createElement('div');
                    banner.id = 'upg-auth-banner';
                    banner.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:99999;background:#ef4444;color:#fff;text-align:center;padding:12px 16px;font-size:14px;font-weight:600;';
                    banner.innerHTML = '⚠️ Sessão expirada. <a href="/login" style="color:#fff;text-decoration:underline;">Clique aqui para fazer login</a> e os uploads serão retomados.';
                    document.body.prepend(banner);
                }
            }
        }

        // Fix M7: IDB quota excedida
        if (msg.type === 'quota-exceeded') {
            const toast = global.mostrarToast;
            if (typeof toast === 'function') {
                toast(`❌ Armazenamento local cheio. Limpe uploads concluídos e tente novamente.`);
            }
        }
    });

    // Inicializa no load
    async function init() {
        _injectPanel();
        try {
            await registerSw();
            await _refresh();
        } catch (e) {
            console.warn('[UploadManager] SW não disponível:', e.message);
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    global.UploadManager = {
        enqueue, getJobs, cancelJob, deleteJob, clearFinished,
        onEvent, togglePanel, registerSw,
        _toggleLogs, // Fix H4: exposto para o HTML inline dos botões
    };
})(window);
