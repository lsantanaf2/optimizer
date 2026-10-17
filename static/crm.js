/* ============================================================
   CRM — kanban, ficha do lead e alerta de lead novo.
   Vanilla JS, sem dependências (mesmo padrão do resto do optimizer).
   ============================================================ */

const CRM = (() => {
    const CFG = window.CRM_CONFIG || {};
    const ETAPAS = CFG.etapas || [];
    const SLA_META = CFG.slaMetaMin || 15;

    // Intervalo do polling. 12s é o compromisso: rápido o bastante para o
    // alerta ser útil, leve o bastante para não ocupar worker do Gunicorn
    // (os 4 workers já atendem as SSE das dashes).
    const POLL_MS = 12000;

    let leads = [];
    let leadAberto = null;
    let saidaPendente = null;      // { status, leadId }
    let lastSeen = null;           // relógio do SERVIDOR, nunca do browser
    let pollTimer = null;
    let alertasAtivos = false;
    let audioCtx = null;
    let tituloOriginal = document.title;
    let piscaTimer = null;
    let naoLidos = 0;

    // ───────────────────────────────────────────────────────────
    // Util
    // ───────────────────────────────────────────────────────────
    const $ = (sel, root = document) => root.querySelector(sel);
    const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g,
        c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

    function fmtData(iso, comHora = true) {
        if (!iso) return '—';
        const d = new Date(iso);
        const opts = comHora
            ? { day: '2-digit', month: '2-digit', year: '2-digit', hour: '2-digit', minute: '2-digit' }
            : { day: '2-digit', month: '2-digit', year: '2-digit' };
        return d.toLocaleString('pt-BR', opts);
    }

    function fmtDuracao(min) {
        if (min == null) return '—';
        if (min < 60) return `${Math.round(min)}min`;
        if (min < 1440) return `${(min / 60).toFixed(1)}h`;
        return `${(min / 1440).toFixed(1)}d`;
    }

    function fmtMoeda(v) {
        if (v == null || v === '') return '—';
        return Number(v).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
    }

    async function api(url, opts = {}) {
        const resp = await fetch(url, {
            headers: { 'Content-Type': 'application/json' },
            ...opts,
        });
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) throw new Error(data.error || `Erro ${resp.status}`);
        return data;
    }

    // ───────────────────────────────────────────────────────────
    // Kanban
    // ───────────────────────────────────────────────────────────
    async function carregar() {
        const periodo = $('#filtro-periodo').value;
        const status = $('#filtro-status').value;
        const arquivados = $('#filtro-arquivados').checked ? '1' : '0';
        try {
            const data = await api(`/api/crm/leads?periodo=${periodo}&status=${status}&arquivados=${arquivados}`);
            leads = data.leads || [];
            render();
        } catch (e) {
            mostrarToast('❌ ' + e.message);
        }
    }

    function slaClasse(lead) {
        // Lead sem contato: urgência cresce com a espera.
        if (lead.primeiro_contato_em) return lead.sla_ok ? 'ok' : 'atrasado';
        const esperando = lead.aguardando_min;
        if (esperando == null) return '';
        if (esperando > SLA_META) return 'atrasado';
        if (esperando > SLA_META / 2) return 'correndo';
        return '';
    }

    function cardHtml(lead) {
        const sla = slaClasse(lead);
        const tags = [];

        if (lead.primeiro_contato_em) {
            tags.push(`<span class="crm-tag sla-${lead.sla_ok ? 'ok' : 'atrasado'}">
                1º contato ${fmtDuracao(lead.sla_min)}</span>`);
        } else if (lead.aguardando_min != null) {
            tags.push(`<span class="crm-tag sla-${sla || 'correndo'}">
                aguardando ${fmtDuracao(lead.aguardando_min)}</span>`);
        }
        if (lead.submissions_count > 1) {
            tags.push(`<span class="crm-tag recorrente">${lead.submissions_count}x cadastrou</span>`);
        }
        if (lead.qualificado === true) tags.push('<span class="crm-tag qualificado">qualificado</span>');
        if (lead.status !== 'ativo') {
            tags.push(`<span class="crm-tag">${esc(lead.status_label)}</span>`);
        }
        if (lead.tarefas_pendentes > 0) {
            tags.push(`<span class="crm-tag tarefa">⏰ ${fmtData(lead.proxima_tarefa_em)}</span>`);
        }
        if (lead.valor_fechado) {
            tags.push(`<span class="crm-tag qualificado">${fmtMoeda(lead.valor_fechado)}</span>`);
        }

        const origem = [lead.utm_source, lead.utm_campaign].filter(Boolean).join(' · ') || 'direto';

        return `
        <div class="lead-card" draggable="true" data-id="${lead.id}"
             data-sla="${sla}" data-status="${lead.status}"
             onclick="CRM.abrirLead('${lead.id}')">
            <div class="lead-card-nome">${esc(lead.nome || 'Sem nome')}</div>
            <div class="lead-card-meta">
                <span>${esc(lead.whatsapp_display || '')}</span>
            </div>
            <div class="lead-card-meta" style="margin-top:3px;">
                <span>📍 ${esc(origem)}</span>
                <span>· ${fmtData(lead.created_at)}</span>
            </div>
            <div class="lead-card-tags">${tags.join('')}</div>
        </div>`;
    }

    function render() {
        ETAPAS.forEach(etapa => {
            const body = $(`[data-body="${etapa}"]`);
            const doEtapa = leads.filter(l => l.etapa === etapa);
            $(`[data-count="${etapa}"]`).textContent = doEtapa.length;
            body.innerHTML = doEtapa.length
                ? doEtapa.map(cardHtml).join('')
                : '<div class="kanban-empty">—</div>';
        });
        ligarDragDrop();
    }

    // ───────────────────────────────────────────────────────────
    // Drag & drop (HTML5 nativo — sem biblioteca)
    // ───────────────────────────────────────────────────────────
    function ligarDragDrop() {
        document.querySelectorAll('.lead-card').forEach(card => {
            card.addEventListener('dragstart', e => {
                e.dataTransfer.setData('text/plain', card.dataset.id);
                card.classList.add('dragging');
            });
            card.addEventListener('dragend', () => card.classList.remove('dragging'));
        });

        document.querySelectorAll('.kanban-col').forEach(col => {
            if (col.dataset.dndReady) return;
            col.dataset.dndReady = '1';
            col.addEventListener('dragover', e => {
                e.preventDefault();
                col.classList.add('drag-over');
            });
            col.addEventListener('dragleave', () => col.classList.remove('drag-over'));
            col.addEventListener('drop', async e => {
                e.preventDefault();
                col.classList.remove('drag-over');
                const id = e.dataTransfer.getData('text/plain');
                const etapa = col.dataset.etapa;
                const lead = leads.find(l => l.id === id);
                if (!lead || lead.etapa === etapa) return;
                await mover(id, { etapa });
            });
        });
    }

    async function mover(id, payload) {
        try {
            const data = await api(`/api/crm/leads/${id}/mover`, {
                method: 'POST', body: JSON.stringify(payload),
            });
            substituirLead(data.lead);
            render();
            mostrarToast('✅ Lead movido');
            return data;
        } catch (e) {
            mostrarToast('❌ ' + e.message);
            throw e;
        }
    }

    function substituirLead(lead) {
        const i = leads.findIndex(l => l.id === lead.id);
        if (i >= 0) leads[i] = lead; else leads.unshift(lead);
    }

    // ───────────────────────────────────────────────────────────
    // Modal do lead
    // ───────────────────────────────────────────────────────────
    async function abrirLead(id) {
        try {
            const data = await api(`/api/crm/leads/${id}`);
            leadAberto = data;
            preencherModal(data);
            $('#modal-lead').classList.add('open');
            document.body.style.overflow = 'hidden';
        } catch (e) {
            mostrarToast('❌ ' + e.message);
        }
    }

    function fecharLead() {
        $('#modal-lead').classList.remove('open');
        document.body.style.overflow = '';
        leadAberto = null;
    }

    function preencherModal(data) {
        const l = data.lead;
        $('#ml-nome').textContent = l.nome || 'Sem nome';
        $('#ml-sub').textContent =
            `${l.whatsapp_display || ''} · ${l.email || 'sem e-mail'} · ${l.etapa_label}` +
            (l.status !== 'ativo' ? ` · ${l.status_label}` : '');
        $('#ml-whats').href = l.whatsapp_link || '#';
        $('#btn-arquivar').textContent = l.arquivado_em ? '📦 Desarquivar' : '📦 Arquivar';

        // Ficha
        ['nome', 'email', 'empresa_segmento', 'faturamento_anual',
            'investimento_midia', 'valor_proposta', 'valor_fechado'].forEach(c => {
                $('#f-' + c).value = l[c] == null ? '' : l[c];
            });
        $('#f-ja_fez_trafego').value = l.ja_fez_trafego == null ? '' : String(l.ja_fez_trafego);
        $('#f-qualificado').value = l.qualificado == null ? '' : String(l.qualificado);
        $('#f-urgencia').value = l.urgencia || '';
        $('#m-etapa').value = l.etapa;
        $('#ml-save-status').textContent = '';

        // Origem e cadastros — o histórico que responde "quantas vezes e de onde"
        const subs = data.submissions || [];
        $('#ml-subs-count').textContent = subs.length > 1 ? `(${subs.length})` : '';
        $('#ml-origem-resumo').textContent = subs.length > 1
            ? `Esta pessoa se cadastrou ${subs.length} vezes. Cada cadastro abaixo com a origem daquela vez.`
            : 'Um cadastro.';
        $('#ml-subs').innerHTML = subs.map((s, i) => {
            const vez = subs.length - i;
            const linha = (rotulo, valor) => valor
                ? `<div><b>${rotulo}</b><br>${esc(valor)}</div>` : '';
            return `
            <div class="crm-sub">
                <div class="crm-sub-head">
                    <span class="crm-sub-vez">Cadastro #${vez}</span>
                    <span class="crm-sub-when">${fmtData(s.created_at)}</span>
                </div>
                <div class="crm-sub-grid">
                    ${linha('source', s.utm_source || 'direto')}
                    ${linha('medium', s.utm_medium)}
                    ${linha('campaign', s.utm_campaign)}
                    ${linha('content', s.utm_content)}
                    ${linha('term', s.utm_term)}
                    ${linha('1º contato com a marca', s.first_utm_source
                    ? `${s.first_utm_source} / ${s.first_utm_campaign || '—'}` : '')}
                    ${linha('anúncio (ad_id)', s.meta_ad_id)}
                    ${linha('fbclid', s.fbclid ? s.fbclid.slice(0, 22) + '…' : '')}
                    ${linha('gclid', s.gclid ? s.gclid.slice(0, 22) + '…' : '')}
                    ${linha('referrer', s.referrer)}
                    ${linha('página', s.landing_url)}
                </div>
            </div>`;
        }).join('') || '<div class="kanban-empty">Sem cadastros registrados.</div>';

        // Histórico de eventos
        const LABEL_EVENTO = {
            lead_criado: '🆕 Lead criado', recadastro: '🔁 Cadastrou de novo',
            etapa_mudou: '➡️ Mudou de etapa', status_mudou: '🏷️ Mudou de status',
            contato_tentado: '📞 Tentativa de contato', contato_efetivo: '✅ Contato efetivo',
            call_agendada: '📅 Call agendada', no_show: '🚫 No-show',
            call_realizada: '🎙️ Call realizada', proposta_enviada: '📄 Proposta enviada',
            ganho: '🏆 Ganho', perdido: '✖ Perdido',
            qualificacao_atualizada: '✏️ Ficha atualizada', nota_adicionada: '📝 Nota',
            tarefa_criada: '⏰ Tarefa criada', tarefa_concluida: '☑️ Tarefa concluída',
        };
        $('#ml-eventos').innerHTML = (data.eventos || []).map(e => {
            let detalhe = '';
            if (e.tipo === 'etapa_mudou') {
                detalhe = `${CFG.etapaLabels[e.de_etapa] || e.de_etapa} → ${CFG.etapaLabels[e.para_etapa] || e.para_etapa}`;
            } else if (e.tipo === 'status_mudou') {
                detalhe = `${CFG.statusLabels[e.de_status] || e.de_status} → ${CFG.statusLabels[e.para_status] || e.para_status}`;
            }
            const p = e.payload || {};
            const extras = [p.canal, p.resultado, p.motivo, p.observacao, p.preview]
                .filter(Boolean).join(' · ');
            const destaque = ['lead_criado', 'recadastro', 'ganho', 'perdido'].includes(e.tipo);
            return `
            <li class="${destaque ? 'destaque' : ''}">
                <div class="quando">${fmtData(e.created_at)}${e.autor_email ? ' · ' + esc(e.autor_email) : ''}</div>
                <div class="oque">${LABEL_EVENTO[e.tipo] || e.tipo}</div>
                ${detalhe ? `<div class="detalhe">${esc(detalhe)}</div>` : ''}
                ${extras ? `<div class="detalhe">${esc(extras)}</div>` : ''}
            </li>`;
        }).join('') || '<div class="kanban-empty">Sem histórico.</div>';

        // Notas
        $('#ml-notas').innerHTML = (data.notas || []).map(n => `
            <li>
                <div class="quando">${fmtData(n.created_at)}${n.autor_email ? ' · ' + esc(n.autor_email) : ''}</div>
                <div class="oque" style="white-space:pre-wrap">${esc(n.texto)}</div>
            </li>`).join('') || '<div class="kanban-empty">Sem notas.</div>';

        // Tarefas
        $('#ml-tarefas').innerHTML = (data.tarefas || []).map(t => `
            <li class="${t.status === 'pendente' ? 'destaque' : ''}">
                <div class="quando">${fmtData(t.due_at)} · ${esc(t.tipo)} · ${esc(t.status)}</div>
                <div class="oque">${esc(t.motivo || t.titulo || '—')}</div>
                ${t.status === 'pendente'
                ? `<button class="crm-btn" style="margin-top:5px;padding:3px 9px;font-size:11px"
                             onclick="CRM.concluirTarefa('${t.id}')">Concluir</button>` : ''}
            </li>`).join('') || '<div class="kanban-empty">Sem tarefas.</div>';
    }

    async function salvarFicha() {
        if (!leadAberto) return;
        const id = leadAberto.lead.id;
        const tri = v => v === '' ? null : v === 'true';
        const payload = {
            nome: $('#f-nome').value,
            email: $('#f-email').value,
            empresa_segmento: $('#f-empresa_segmento').value,
            faturamento_anual: $('#f-faturamento_anual').value,
            investimento_midia: $('#f-investimento_midia').value,
            valor_proposta: $('#f-valor_proposta').value,
            valor_fechado: $('#f-valor_fechado').value,
            ja_fez_trafego: tri($('#f-ja_fez_trafego').value),
            qualificado: tri($('#f-qualificado').value),
            urgencia: $('#f-urgencia').value,
        };
        $('#ml-save-status').textContent = 'Salvando...';
        try {
            const data = await api(`/api/crm/leads/${id}`, {
                method: 'PATCH', body: JSON.stringify(payload),
            });
            substituirLead(data.lead);
            render();
            $('#ml-save-status').textContent = '✅ Salvo';
            setTimeout(() => { $('#ml-save-status').textContent = ''; }, 2500);
        } catch (e) {
            $('#ml-save-status').textContent = '❌ ' + e.message;
        }
    }

    async function recarregarAberto() {
        if (leadAberto) await abrirLead(leadAberto.lead.id);
        await carregar();
    }

    async function registrarContato() {
        if (!leadAberto) return;
        try {
            await api(`/api/crm/leads/${leadAberto.lead.id}/contato`, {
                method: 'POST',
                body: JSON.stringify({
                    canal: $('#c-canal').value,
                    resultado: $('#c-resultado').value,
                }),
            });
            mostrarToast('✅ Contato registrado');
            await recarregarAberto();
        } catch (e) { mostrarToast('❌ ' + e.message); }
    }

    async function marcarNoShow() {
        if (!leadAberto) return;
        await api(`/api/crm/leads/${leadAberto.lead.id}/no-show`, { method: 'POST', body: '{}' });
        mostrarToast('Registrado no-show');
        await recarregarAberto();
    }

    async function marcarCallRealizada() {
        if (!leadAberto) return;
        await api(`/api/crm/leads/${leadAberto.lead.id}/call-realizada`, { method: 'POST', body: '{}' });
        mostrarToast('✅ Call realizada');
        await recarregarAberto();
    }

    async function moverPeloModal() {
        if (!leadAberto) return;
        await mover(leadAberto.lead.id, { etapa: $('#m-etapa').value });
        await recarregarAberto();
    }

    async function reativar() {
        if (!leadAberto) return;
        await mover(leadAberto.lead.id, { status: 'ativo' });
        await recarregarAberto();
    }

    async function arquivar() {
        if (!leadAberto) return;
        const arquivar = !leadAberto.lead.arquivado_em;
        await api(`/api/crm/leads/${leadAberto.lead.id}/arquivar`, {
            method: 'POST', body: JSON.stringify({ arquivar }),
        });
        fecharLead();
        await carregar();
    }

    async function salvarNota() {
        if (!leadAberto) return;
        const texto = $('#n-texto').value.trim();
        if (!texto) return;
        try {
            await api(`/api/crm/leads/${leadAberto.lead.id}/notas`, {
                method: 'POST', body: JSON.stringify({ texto }),
            });
            $('#n-texto').value = '';
            await recarregarAberto();
        } catch (e) { mostrarToast('❌ ' + e.message); }
    }

    async function criarTarefa() {
        if (!leadAberto) return;
        const due = $('#t-due').value;
        if (!due) { mostrarToast('Informe a data'); return; }
        try {
            await api(`/api/crm/leads/${leadAberto.lead.id}/tarefas`, {
                method: 'POST',
                body: JSON.stringify({
                    due_at: due, tipo: $('#t-tipo').value, motivo: $('#t-motivo').value,
                }),
            });
            $('#t-motivo').value = '';
            await recarregarAberto();
        } catch (e) { mostrarToast('❌ ' + e.message); }
    }

    async function concluirTarefa(id) {
        await api(`/api/crm/tarefas/${id}/concluir`, { method: 'POST', body: '{}' });
        await recarregarAberto();
    }

    // ───────────────────────────────────────────────────────────
    // Saída do fluxo — motivo obrigatório
    // ───────────────────────────────────────────────────────────
    function abrirSaida(status) {
        if (!leadAberto) return;
        saidaPendente = { status, leadId: leadAberto.lead.id };
        const timing = status === 'timing_errado';
        $('#ms-titulo').textContent = timing ? '⏳ Timing errado' : '✖ Marcar como perdido';
        $('#ms-sub').textContent = timing
            ? 'Agende o recontato para o lead não ser esquecido.'
            : 'O motivo alimenta o relatório de motivos de perda.';
        $('#ms-data-wrap').style.display = timing ? 'block' : 'none';
        $('#ms-motivo').value = '';
        if (timing) {
            // Sugestão: 30 dias à frente, no mesmo horário.
            const d = new Date(Date.now() + 30 * 86400000);
            d.setMinutes(d.getMinutes() - d.getTimezoneOffset());
            $('#ms-data').value = d.toISOString().slice(0, 16);
        }
        $('#modal-saida').classList.add('open');
    }

    function fecharSaida() {
        $('#modal-saida').classList.remove('open');
        saidaPendente = null;
    }

    async function confirmarSaida() {
        if (!saidaPendente) return;
        const motivo = $('#ms-motivo').value.trim();
        if (!motivo) { mostrarToast('Informe o motivo'); return; }
        const payload = { status: saidaPendente.status, motivo };
        if (saidaPendente.status === 'timing_errado') {
            const data = $('#ms-data').value;
            if (!data) { mostrarToast('Informe a data de recontato'); return; }
            payload.data_recontato = data;
        }
        try {
            await mover(saidaPendente.leadId, payload);
            fecharSaida();
            fecharLead();
            await carregar();
        } catch (e) { /* mover já avisou */ }
    }

    // ───────────────────────────────────────────────────────────
    // Alerta de lead novo
    // ───────────────────────────────────────────────────────────
    // Um clique em "Ativar alertas" faz as duas coisas que exigem gesto do
    // usuário: pede permissão de notificação e cria o AudioContext (o browser
    // bloqueia áudio sem interação). Depois disso o alerta é automático.
    async function ativarAlertas() {
        try {
            audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            if (audioCtx.state === 'suspended') await audioCtx.resume();
        } catch (e) {
            console.warn('Áudio indisponível', e);
        }

        let permissao = 'default';
        if ('Notification' in window) {
            permissao = Notification.permission;
            if (permissao === 'default') permissao = await Notification.requestPermission();
        }

        alertasAtivos = true;
        localStorage.setItem('crm-alertas', '1');
        const btn = $('#btn-alertas');
        btn.classList.add('on');
        btn.textContent = permissao === 'granted' ? '🔔 Alertas ativos' : '🔔 Alertas (só som)';
        beep();   // confirma que o som funciona AGORA, não na hora do lead
        if (permissao !== 'granted') {
            mostrarToast('Som ativo. Permita notificações para ser avisado com a aba em segundo plano.');
        }
    }

    // Som gerado na hora (Web Audio): nada para baixar, nada para hospedar.
    // Dois tons curtos, perceptíveis sem ser irritantes.
    function beep() {
        if (!audioCtx) return;
        const agora = audioCtx.currentTime;
        [0, 0.18].forEach((offset, i) => {
            const osc = audioCtx.createOscillator();
            const gain = audioCtx.createGain();
            osc.type = 'sine';
            osc.frequency.value = i === 0 ? 880 : 1180;
            gain.gain.setValueAtTime(0, agora + offset);
            gain.gain.linearRampToValueAtTime(0.28, agora + offset + 0.02);
            gain.gain.exponentialRampToValueAtTime(0.001, agora + offset + 0.16);
            osc.connect(gain).connect(audioCtx.destination);
            osc.start(agora + offset);
            osc.stop(agora + offset + 0.18);
        });
    }

    function notificar(lead) {
        if (!('Notification' in window) || Notification.permission !== 'granted') return;
        const origem = [lead.utm_source, lead.utm_campaign].filter(Boolean).join(' · ') || 'direto';
        const n = new Notification(
            lead.tipo === 'recadastro' ? '🔁 Lead voltou!' : '🎯 LEAD NOVO!',
            {
                body: `${lead.nome || 'Sem nome'}\n${lead.whatsapp}\n${origem}`,
                tag: 'crm-lead-' + lead.lead_id,
                requireInteraction: true,   // fica na tela até você olhar
            }
        );
        n.onclick = () => { window.focus(); abrirLead(lead.lead_id); n.close(); };
    }

    // Título piscando + contador: funciona mesmo sem permissão nenhuma.
    function piscarTitulo() {
        naoLidos++;
        if (piscaTimer) return;
        let on = false;
        piscaTimer = setInterval(() => {
            on = !on;
            document.title = on
                ? `🔴 (${naoLidos}) LEAD NOVO!`
                : `(${naoLidos}) ${tituloOriginal}`;
        }, 900);
    }

    function pararPisca() {
        if (piscaTimer) { clearInterval(piscaTimer); piscaTimer = null; }
        naoLidos = 0;
        document.title = tituloOriginal;
    }

    function banner(lead) {
        const wrap = $('#crm-alertas');
        const el = document.createElement('div');
        el.className = 'crm-alert';
        const origem = [lead.utm_source, lead.utm_campaign].filter(Boolean).join(' · ') || 'direto';
        el.innerHTML = `
            <div class="crm-alert-title">${lead.tipo === 'recadastro'
                ? `🔁 LEAD VOLTOU (${lead.submissions_count}ª vez)` : '🎯 LEAD NOVO'}</div>
            <div class="crm-alert-nome">${esc(lead.nome || 'Sem nome')}</div>
            <div class="crm-alert-meta">${esc(lead.whatsapp)} · ${esc(origem)}</div>
            <div class="crm-alert-meta">clique para abrir</div>`;
        el.onclick = () => { abrirLead(lead.lead_id); el.remove(); if (!wrap.children.length) wrap.classList.remove('show'); };
        wrap.appendChild(el);
        wrap.classList.add('show');
        setTimeout(() => {
            el.remove();
            if (!wrap.children.length) wrap.classList.remove('show');
        }, 60000);
    }

    async function poll() {
        try {
            const data = await api(`/api/crm/novos?since=${encodeURIComponent(lastSeen)}`);
            // Avança pelo relógio do servidor: o do browser pode estar torto e
            // isso faria o alerta repetir ou pular leads.
            lastSeen = data.server_time;
            $('#crm-status-poll').textContent = '● ' + new Date().toLocaleTimeString('pt-BR');

            const novos = data.novos || [];
            if (novos.length) {
                novos.forEach(lead => {
                    banner(lead);
                    if (alertasAtivos) { beep(); notificar(lead); }
                });
                document.body.classList.add('crm-lead-chegou');
                setTimeout(() => document.body.classList.remove('crm-lead-chegou'), 3200);
                if (document.hidden) piscarTitulo();
                await carregar();
                // Destaca os cards que acabaram de chegar
                novos.forEach(l => {
                    const card = document.querySelector(`.lead-card[data-id="${l.lead_id}"]`);
                    if (card) card.classList.add('novo-flash');
                });
            }
        } catch (e) {
            $('#crm-status-poll').textContent = '○ sem conexão';
        }
    }

    // ───────────────────────────────────────────────────────────
    // Init
    // ───────────────────────────────────────────────────────────
    function init() {
        tituloOriginal = document.title;
        lastSeen = new Date().toISOString();

        ['filtro-periodo', 'filtro-status', 'filtro-arquivados'].forEach(id => {
            $('#' + id).addEventListener('change', carregar);
        });

        document.querySelectorAll('.crm-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('.crm-tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.crm-tabpanel').forEach(p => p.classList.remove('active'));
                tab.classList.add('active');
                $(`[data-panel="${tab.dataset.tab}"]`).classList.add('active');
            });
        });

        document.addEventListener('keydown', e => {
            if (e.key === 'Escape') { fecharSaida(); fecharLead(); }
        });

        // Voltar para a aba limpa o pisca do título
        document.addEventListener('visibilitychange', () => {
            if (!document.hidden) pararPisca();
        });

        // Se já ativou antes, o botão mostra o estado — mas o AudioContext
        // só volta a funcionar com um clique novo (regra do browser).
        if (localStorage.getItem('crm-alertas') === '1') {
            const btn = $('#btn-alertas');
            btn.textContent = '🔔 Reativar alertas (1 clique)';
            btn.classList.add('on');
        }

        carregar();
        poll();
        pollTimer = setInterval(poll, POLL_MS);
    }

    document.addEventListener('DOMContentLoaded', init);

    return {
        carregar, abrirLead, fecharLead, salvarFicha, registrarContato,
        marcarNoShow, marcarCallRealizada, moverPeloModal, reativar, arquivar,
        salvarNota, criarTarefa, concluirTarefa,
        abrirSaida, fecharSaida, confirmarSaida,
        ativarAlertas,
    };
})();
