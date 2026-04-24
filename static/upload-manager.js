/**
 * Optimizer Upload Manager
 * Interface client-side para o Service Worker de uploads.
 * - Registra SW, envia jobs, escuta progresso via BroadcastChannel
 * - Renderiza painel flutuante global (sobrevive navegação)
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
            await navigator.serviceWorker.register('/sw.js', { scope: '/' });
            await navigator.serviceWorker.ready;
            return navigator.serviceWorker.controller || (await navigator.serviceWorker.ready).active;
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

    async function enqueue(job) {
        await registerSw();
        const id = 'job_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
        const payload = { ...job, id };
        const res = await _request({ type: 'enqueue', payload }, 'enqueued', 10000);
        return res.jobId || id;
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
    position: fixed; bottom: 20px; right: 20px; z-index: 9999;
    width: 380px; max-height: 70vh;
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
    padding: 10px; overflow-y: auto; flex: 1; max-height: 60vh;
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
.upg-job-actions { display: flex; gap: 6px; margin-top: 6px; }
.upg-job-actions button {
    background: #2a2f40; border: 1px solid #3a4056; color: #cbd5e1;
    padding: 3px 8px; border-radius: 4px; font-size: 10px; cursor: pointer;
}
.upg-job-actions button:hover { background: #3a4056; }
#upload-panel-toggle {
    position: fixed; bottom: 20px; right: 20px; z-index: 9998;
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

    function _render(jobs) {
        let panel = document.getElementById('upload-panel-global');
        if (!panel) return;
        const body = document.getElementById('upg-body');
        const count = document.getElementById('upg-count');

        // Filtra jobs com status relevante (mostra todos até user limpar)
        const visible = jobs.sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));
        count.textContent = visible.length;

        if (visible.length === 0) {
            panel.style.display = 'none';
            const fab = document.getElementById('upload-panel-toggle');
            if (fab) fab.style.display = 'none';
            return;
        }

        panel.style.display = 'flex';

        body.innerHTML = visible.map(j => {
            const stLabel = {
                queued: 'Na fila', running: 'Enviando', completed: 'Concluído',
                cancelled: 'Cancelado', interrupted: 'Interrompido'
            }[j.status] || j.status;
            const prog = Math.min(100, Math.max(0, j.progress || 0));
            const msg = j.currentMessage || '';
            const enviados = j.enviados || 0, erros = j.erros || 0;
            const isLive = j.status === 'running' || j.status === 'queued';
            return `
                <div class="upg-job" data-job-id="${j.id}">
                    <div class="upg-job-head">
                        <span class="upg-job-label" title="${_esc(j.label || j.id)}">${_esc(j.label || j.id)}</span>
                        <span class="upg-job-status ${_statusClass(j.status)}">${stLabel}</span>
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
                    </div>
                </div>`;
        }).join('');
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

    async function _refresh() {
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
            _refresh();
        }
        if (msg.type === 'job-finished' && msg.job) {
            const toast = global.mostrarToast;
            if (typeof toast === 'function') {
                const j = msg.job;
                if (j.enviados > 0 && j.erros === 0) {
                    toast(`🎉 ${j.enviados} anúncio(s) enviados!`);
                } else if (j.erros > 0) {
                    toast(`⚠️ ${j.enviados} OK, ${j.erros} erro(s)`);
                }
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
    };
})(window);
