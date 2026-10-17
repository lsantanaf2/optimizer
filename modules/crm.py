"""
Blueprint /crm — CRM de leads (braço do optimizer).

Duas superfícies:
  PÚBLICA   — captura de lead. Único endpoint aberto, com form_key, honeypot,
              validação server-side e rate limit. Nunca lê dado, só escreve.
  INTERNA   — kanban, ficha do lead e métricas. Tudo atrás de @login_required:
              dado pessoal de lead não passa por link com token nem por anon key.

Rotas:
  POST   /api/crm/lead                      — captura (público, CORS por form)
  OPTIONS/api/crm/lead                      — preflight CORS
  GET    /lp/<slug>                         — landing page servida pelo Flask
  GET    /obrigado                          — página de agradecimento

  GET    /crm                               — kanban
  GET    /crm/metricas                      — painel de métricas e SLA
  GET    /api/crm/leads                     — lista (JSON) para o kanban
  GET    /api/crm/leads/<id>                — ficha completa + histórico
  PATCH  /api/crm/leads/<id>                — qualificação/ficha
  POST   /api/crm/leads/<id>/mover          — etapa/status
  POST   /api/crm/leads/<id>/contato        — tentativa de contato
  POST   /api/crm/leads/<id>/notas          — nota
  POST   /api/crm/leads/<id>/tarefas        — tarefa (recontato)
  POST   /api/crm/leads/<id>/no-show        — marca no-show
  POST   /api/crm/leads/<id>/call-realizada — marca call realizada
  POST   /api/crm/leads/<id>/arquivar       — arquiva/desarquiva
  POST   /api/crm/tarefas/<id>/concluir     — conclui tarefa
  GET    /api/crm/tarefas                   — tarefas pendentes
  GET    /api/crm/novos?since=<iso>         — polling do alerta de lead novo
  GET    /api/crm/metricas                  — métricas (JSON)
"""

import logging
import re
from datetime import datetime, timedelta
from functools import wraps

from flask import (
    Blueprint, abort, jsonify, make_response, redirect, render_template,
    request, session, url_for,
)

from modules import crm_core as core
from modules.crm_core import BR_TZ, ETAPA_LABELS, ETAPAS, STATUS_LABELS, TelefoneInvalido
from modules.database import fetch_one
from modules.rate_limiter import _client_ip, check_rate_limit, is_allowed

logger = logging.getLogger(__name__)

crm_bp = Blueprint('crm', __name__)

# Rate limit da captura, em três camadas com papéis diferentes.
#
# TENTATIVAS (qualquer POST, mesmo inválido): generoso de propósito. Um limite
# apertado aqui pune quem errou o DDD e tenta de novo em 10 segundos — o lead
# recebe "aguarde" e vai embora. 12/min corta martelada de bot sem atrapalhar
# ninguém corrigindo o próprio telefone.
CAPTURA_TENTATIVAS_MAX, CAPTURA_TENTATIVAS_WINDOW = 12, 60
# CADASTROS GRAVADOS: aí sim apertado. Um humano preenche uma vez.
CAPTURA_BURST_MAX, CAPTURA_BURST_WINDOW = 1, 60
CAPTURA_HORA_MAX, CAPTURA_HORA_WINDOW = 5, 3600
# Teto por formulário (soma de todos os IPs): barra flood distribuído.
CAPTURA_FORM_MAX, CAPTURA_FORM_WINDOW = 60, 3600

# Nome do campo-armadilha. Fica invisível no HTML: humano não vê, não preenche;
# bot que preenche o formulário todo cai aqui.
HONEYPOT_FIELD = 'empresa_website'


def _login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user_id'):
            if request.path.startswith('/api/'):
                return jsonify({'error': 'não autenticado'}), 401
            return redirect(url_for('auth.login_page'))
        return f(*args, **kwargs)
    return decorated


def _autor():
    return session.get('user_id')


# ─────────────────────────────────────────────────────────────────────────────
# Captura pública
# ─────────────────────────────────────────────────────────────────────────────

def _get_form(form_key):
    if not form_key:
        return None
    return fetch_one(
        "SELECT * FROM crm_forms WHERE form_key = %s AND enabled IS TRUE",
        (form_key,),
    )


def _cors_headers(resp, form):
    """Libera CORS só para as origens cadastradas no formulário."""
    origin = request.headers.get('Origin')
    permitidas = (form or {}).get('allowed_origins') or []
    if origin and origin in permitidas:
        resp.headers['Access-Control-Allow-Origin'] = origin
        resp.headers['Vary'] = 'Origin'
        resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        resp.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        resp.headers['Access-Control-Max-Age'] = '600'
    return resp


@crm_bp.route('/api/crm/lead', methods=['OPTIONS'])
def captura_preflight():
    form = _get_form((request.args.get('form_key') or '').strip())
    return _cors_headers(make_response('', 204), form)


@crm_bp.route('/api/crm/lead', methods=['POST'])
def captura_lead():
    dados = request.get_json(silent=True) or request.form.to_dict() or {}
    form_key = (dados.get('form_key') or request.args.get('form_key') or '').strip()
    form = _get_form(form_key)

    def responder(payload, status=200):
        resp = make_response(jsonify(payload), status)
        return _cors_headers(resp, form)

    if not form:
        return responder({'ok': False, 'error': 'Formulário inválido. [CRM-404]'}, 404)

    # CORS: se vier de outra origem, ela precisa estar cadastrada.
    origin = request.headers.get('Origin')
    if origin and origin not in (form.get('allowed_origins') or []):
        if origin.rstrip('/') != request.host_url.rstrip('/'):
            logger.warning("CRM captura: origem não autorizada %s (form %s)", origin, form_key)
            return responder({'ok': False, 'error': 'Origem não autorizada. [CRM-403]'}, 403)

    # Honeypot: responde 200 para o bot não aprender que foi detectado.
    if (dados.get(HONEYPOT_FIELD) or '').strip():
        logger.info("CRM captura: honeypot acionado (ip=%s form=%s)", _client_ip(), form_key)
        return responder({'ok': True, 'redirect': form.get('redirect_url') or '/obrigado'})

    # Camada 1 — tentativas: só freia martelada, não atrapalha correção.
    if not is_allowed(f'crm-captura-try|{_client_ip()}',
                      CAPTURA_TENTATIVAS_MAX, CAPTURA_TENTATIVAS_WINDOW):
        return responder({'ok': False, 'error': 'Muitas tentativas. Aguarde um momento. [CRM-429]'}, 429)

    # Validação server-side (o front valida também, mas quem decide é o servidor).
    # Vem ANTES do limite de gravação: quem errou o telefone pode corrigir na hora.
    try:
        e164, pais = core.normalizar_telefone(dados.get('whatsapp') or dados.get('telefone'))
        email = core.validar_email(dados.get('email'))
    except TelefoneInvalido as e:
        return responder({'ok': False, 'error': str(e)}, 422)

    nome = core.limpar_texto(dados.get('nome'), 200)
    if not nome or len(nome) < 2:
        return responder({'ok': False, 'error': 'Informe seu nome.'}, 422)

    # Camadas 2 e 3 — cadastros que de fato vão para o banco.
    if not is_allowed(f'crm-captura-burst|{_client_ip()}', CAPTURA_BURST_MAX, CAPTURA_BURST_WINDOW):
        return responder({'ok': False, 'error': 'Já recebemos seu contato. Aguarde que falamos com você. [CRM-429]'}, 429)
    if not is_allowed(f'crm-captura-hora|{_client_ip()}', CAPTURA_HORA_MAX, CAPTURA_HORA_WINDOW):
        return responder({'ok': False, 'error': 'Muitos envios deste dispositivo. [CRM-429]'}, 429)
    if not is_allowed(f'crm-captura-form:{form_key}', CAPTURA_FORM_MAX, CAPTURA_FORM_WINDOW):
        logger.warning("CRM captura: teto do formulário %s atingido", form_key)
        return responder({'ok': False, 'error': 'Formulário temporariamente indisponível. [CRM-429]'}, 429)

    atribuicao = {
        'utm_source': dados.get('utm_source'),
        'utm_medium': dados.get('utm_medium'),
        'utm_campaign': dados.get('utm_campaign'),
        'utm_content': dados.get('utm_content'),
        'utm_term': dados.get('utm_term'),
        'first_utm_source': dados.get('first_utm_source'),
        'first_utm_medium': dados.get('first_utm_medium'),
        'first_utm_campaign': dados.get('first_utm_campaign'),
        'first_utm_content': dados.get('first_utm_content'),
        'first_utm_term': dados.get('first_utm_term'),
        'fbclid': dados.get('fbclid'),
        'gclid': dados.get('gclid'),
        # _fbp/_fbc vêm do cookie quando a LP é same-origin; o JS manda no body
        # quando é cross-origin (cookie de outro domínio não chega aqui).
        'fbp': dados.get('fbp') or request.cookies.get('_fbp'),
        'fbc': dados.get('fbc') or request.cookies.get('_fbc'),
        'ttclid': dados.get('ttclid'),
        'meta_ad_id': dados.get('meta_ad_id') or dados.get('ad_id'),
        'meta_adset_id': dados.get('meta_adset_id') or dados.get('adset_id'),
        'meta_campaign_id': dados.get('meta_campaign_id') or dados.get('campaign_id'),
        'referrer': dados.get('referrer') or request.referrer,
        'landing_url': dados.get('landing_url'),
        'ip': _client_ip(),
        'user_agent': request.headers.get('User-Agent'),
    }

    first_seen_at = None
    if dados.get('first_seen_at'):
        try:
            first_seen_at = datetime.fromisoformat(str(dados['first_seen_at']).replace('Z', '+00:00'))
        except ValueError:
            pass

    # Campos extras do form vão para payload jsonb — adicionar pergunta na LP
    # não exige migration.
    conhecidos = set(atribuicao) | {
        'nome', 'email', 'whatsapp', 'telefone', 'empresa_segmento', 'form_key',
        'first_seen_at', 'ad_id', 'adset_id', 'campaign_id', HONEYPOT_FIELD,
    }
    extras = {k: core.limpar_texto(v, 500) for k, v in dados.items()
              if k not in conhecidos and v not in (None, '')}

    try:
        res = core.registrar_captura(
            whatsapp_e164=e164,
            pais=pais,
            nome=nome,
            email=email,
            empresa_segmento=core.limpar_texto(dados.get('empresa_segmento'), 200),
            form_key=form_key,
            atribuicao=atribuicao,
            first_seen_at=first_seen_at,
            payload=extras,
        )
    except Exception:
        logger.exception("CRM captura: falha ao gravar lead (form=%s)", form_key)
        return responder({'ok': False, 'error': 'Erro ao registrar. Tente novamente. [CRM-500]'}, 500)

    logger.info("CRM captura: %s lead=%s (vez %s) via %s",
                'novo' if res['is_novo'] else 'recadastro',
                res['lead_id'], res['submissions_count'], atribuicao.get('utm_source') or 'direto')

    return responder({
        'ok': True,
        'is_novo': res['is_novo'],
        'redirect': form.get('redirect_url') or url_for('crm.obrigado'),
    })


@crm_bp.route('/lp/<slug>')
def landing_page(slug):
    form = fetch_one(
        "SELECT * FROM crm_forms WHERE slug = %s AND enabled IS TRUE",
        (slug,),
    )
    if not form:
        abort(404)
    return render_template('crm_lp.html', form=form, honeypot_field=HONEYPOT_FIELD)


@crm_bp.route('/obrigado')
def obrigado():
    return render_template('crm_obrigado.html')


# ─────────────────────────────────────────────────────────────────────────────
# Kanban
# ─────────────────────────────────────────────────────────────────────────────

# Janelas do filtro de período, em dias. Sem isto o kanban cresce para sempre.
# A conta de "últimos N dias" é feita no Postgres (ver crm_core.periodo_relativo):
# o relógio da VPS pode estar deslocado em relação ao do banco, e aí um lead
# recém-criado sumiria do filtro.
PERIODOS = {
    '7d': 7, '30d': 30, '90d': 90, 'tudo': None,
}


@crm_bp.route('/crm')
@_login_required
def crm_page():
    return render_template(
        'crm_kanban.html',
        etapas=ETAPAS,
        etapa_labels=ETAPA_LABELS,
        status_labels=STATUS_LABELS,
        sla_meta_min=core.SLA_PRIMEIRO_CONTATO_MIN,
    )


@crm_bp.route('/crm/metricas')
@_login_required
def crm_metricas_page():
    return render_template('crm_metricas.html', sla_meta_min=core.SLA_PRIMEIRO_CONTATO_MIN)


@crm_bp.route('/api/crm/leads')
@_login_required
def api_listar_leads():
    periodo = request.args.get('periodo', '30d')
    leads = core.listar_leads(
        dias=PERIODOS.get(periodo, 30),
        incluir_arquivados=request.args.get('arquivados') == '1',
        status=request.args.get('status') or None,
    )
    return jsonify({
        'periodo': periodo,
        'total': len(leads),
        'leads': [_lead_json(l) for l in leads],
    })


@crm_bp.route('/api/crm/leads/<lead_id>')
@_login_required
def api_lead_detalhe(lead_id):
    lead = core.obter_lead(lead_id)
    if not lead:
        return jsonify({'error': 'lead não encontrado'}), 404
    return jsonify({
        'lead': _lead_json(lead),
        # Histórico de cadastros: quantas vezes e de onde, cada vez.
        'submissions': [core._row(s) for s in core.listar_submissions(lead_id)],
        'eventos': [core._row(e) for e in core.listar_eventos(lead_id)],
        'notas': [core._row(n) for n in core.listar_notas(lead_id)],
        'tarefas': [core._row(t) for t in core.listar_tarefas(lead_id)],
    })


@crm_bp.route('/api/crm/leads/<lead_id>', methods=['PATCH'])
@_login_required
def api_atualizar_lead(lead_id):
    dados = request.get_json(silent=True) or {}
    try:
        lead = core.atualizar_lead(lead_id, dados, autor_id=_autor())
    except (ValueError, TelefoneInvalido) as e:
        return jsonify({'error': str(e)}), 422
    if not lead:
        return jsonify({'error': 'lead não encontrado'}), 404
    return jsonify({'lead': _lead_json(lead)})


@crm_bp.route('/api/crm/leads/<lead_id>/mover', methods=['POST'])
@_login_required
def api_mover_lead(lead_id):
    dados = request.get_json(silent=True) or {}
    etapa = dados.get('etapa')
    status = dados.get('status')
    motivo = dados.get('motivo')

    # Sair do fluxo ativo exige motivo — senão a métrica de perda fica inútil.
    if status in ('perdido', 'timing_errado') and not (motivo or '').strip():
        return jsonify({'error': 'Informe o motivo.'}), 422

    try:
        lead = core.mover_lead(lead_id, etapa=etapa, status=status,
                              motivo=motivo, autor_id=_autor())
    except ValueError as e:
        return jsonify({'error': str(e)}), 422
    if not lead:
        return jsonify({'error': 'lead não encontrado'}), 404

    # Timing errado sem tarefa de recontato é lead esquecido. Cria junto.
    tarefa = None
    if status == 'timing_errado' and dados.get('data_recontato'):
        try:
            due = datetime.fromisoformat(str(dados['data_recontato']).replace('Z', '+00:00'))
            tarefa = core.criar_tarefa(lead_id, due_at=due, tipo='recontato',
                                       motivo=motivo, owner_id=_autor(), autor_id=_autor())
        except ValueError:
            return jsonify({'error': 'Data de recontato inválida.'}), 422

    return jsonify({'lead': _lead_json(lead),
                    'tarefa': core._row(tarefa) if tarefa else None})


@crm_bp.route('/api/crm/leads/<lead_id>/contato', methods=['POST'])
@_login_required
def api_registrar_contato(lead_id):
    dados = request.get_json(silent=True) or {}
    canal = dados.get('canal') or 'whatsapp'
    resultado = dados.get('resultado') or 'sem_resposta'
    if canal not in ('whatsapp', 'ligacao', 'email', 'outro'):
        return jsonify({'error': 'Canal inválido.'}), 422
    if resultado not in ('atendeu', 'respondeu', 'nao_atendeu', 'sem_resposta', 'numero_errado'):
        return jsonify({'error': 'Resultado inválido.'}), 422
    lead = core.registrar_contato(lead_id, canal=canal, resultado=resultado,
                                  autor_id=_autor(), observacao=dados.get('observacao'))
    if not lead:
        return jsonify({'error': 'lead não encontrado'}), 404
    return jsonify({'lead': _lead_json(lead)})


@crm_bp.route('/api/crm/leads/<lead_id>/notas', methods=['POST'])
@_login_required
def api_adicionar_nota(lead_id):
    dados = request.get_json(silent=True) or {}
    try:
        nota = core.adicionar_nota(lead_id, dados.get('texto'), autor_id=_autor())
    except ValueError as e:
        return jsonify({'error': str(e)}), 422
    return jsonify({'nota': core._row(nota)})


@crm_bp.route('/api/crm/leads/<lead_id>/tarefas', methods=['POST'])
@_login_required
def api_criar_tarefa(lead_id):
    dados = request.get_json(silent=True) or {}
    if not dados.get('due_at'):
        return jsonify({'error': 'Informe a data.'}), 422
    try:
        due = datetime.fromisoformat(str(dados['due_at']).replace('Z', '+00:00'))
    except ValueError:
        return jsonify({'error': 'Data inválida.'}), 422
    tarefa = core.criar_tarefa(lead_id, due_at=due, tipo=dados.get('tipo') or 'recontato',
                               titulo=dados.get('titulo'), motivo=dados.get('motivo'),
                               owner_id=_autor(), autor_id=_autor())
    return jsonify({'tarefa': core._row(tarefa)})


@crm_bp.route('/api/crm/leads/<lead_id>/no-show', methods=['POST'])
@_login_required
def api_no_show(lead_id):
    dados = request.get_json(silent=True) or {}
    lead = core.registrar_no_show(lead_id, autor_id=_autor(),
                                  observacao=dados.get('observacao'))
    return jsonify({'lead': _lead_json(lead)})


@crm_bp.route('/api/crm/leads/<lead_id>/call-realizada', methods=['POST'])
@_login_required
def api_call_realizada(lead_id):
    dados = request.get_json(silent=True) or {}
    lead = core.registrar_call_realizada(lead_id, autor_id=_autor(),
                                         observacao=dados.get('observacao'))
    return jsonify({'lead': _lead_json(lead)})


@crm_bp.route('/api/crm/leads/<lead_id>/arquivar', methods=['POST'])
@_login_required
def api_arquivar(lead_id):
    dados = request.get_json(silent=True) or {}
    lead = core.arquivar_lead(lead_id, arquivar=dados.get('arquivar', True),
                              autor_id=_autor())
    return jsonify({'lead': _lead_json(lead)})


@crm_bp.route('/api/crm/tarefas')
@_login_required
def api_tarefas():
    return jsonify({'tarefas': [core._row(t)
                                for t in core.listar_tarefas(apenas_pendentes=True)]})


@crm_bp.route('/api/crm/tarefas/<tarefa_id>/concluir', methods=['POST'])
@_login_required
def api_concluir_tarefa(tarefa_id):
    tarefa = core.concluir_tarefa(tarefa_id, autor_id=_autor())
    if not tarefa:
        return jsonify({'error': 'tarefa não encontrada ou já concluída'}), 404
    return jsonify({'tarefa': core._row(tarefa)})


# ─────────────────────────────────────────────────────────────────────────────
# Alerta de lead novo — polling leve
# ─────────────────────────────────────────────────────────────────────────────
# Polling (12s) em vez de SSE de propósito: o deploy é Gunicorn com 4 workers
# sync e as dashes já mantêm SSE aberto. Uma conexão permanente a mais no CRM
# prenderia um worker o dia inteiro.

@crm_bp.route('/api/crm/novos')
@_login_required
def api_novos():
    check_rate_limit('crm-poll', max_requests=60, window=60)
    since = request.args.get('since')
    if not since:
        return jsonify({'error': 'since obrigatório'}), 422
    # "+00:00" vira " 00:00" quando o '+' não é percent-encoded na query string.
    # O front encoda, mas um teste com curl ou uma chamada manual não encodaria,
    # e aí o alerta pararia de funcionar sem explicação aparente.
    # Só o espaço no FINAL vira '+': um "2020-01-01 10:00:00" (espaço separando
    # data e hora) continua válido e não é tocado.
    since = re.sub(r'\s(\d{2}:\d{2})$', r'+\1', since.strip().replace('Z', '+00:00'))
    try:
        since_dt = datetime.fromisoformat(since)
    except ValueError:
        return jsonify({'error': 'since inválido'}), 422

    novos = core.leads_desde(since_dt)
    agora = datetime.now(BR_TZ)
    return jsonify({
        'server_time': agora.isoformat(),
        'novos': [{
            'lead_id': str(n['lead_id']),
            'tipo': n['tipo'],
            'nome': n['nome'],
            'whatsapp': core.telefone_display(n['whatsapp_e164']),
            'utm_source': n['utm_source'],
            'utm_campaign': n['utm_campaign'],
            'submissions_count': n['submissions_count'],
            'created_at': n['created_at'].isoformat(),
        } for n in novos],
    })


# ─────────────────────────────────────────────────────────────────────────────
# Métricas
# ─────────────────────────────────────────────────────────────────────────────

@crm_bp.route('/api/crm/metricas')
@_login_required
def api_metricas():
    periodo = request.args.get('periodo', '30d')
    desde_param = request.args.get('desde')
    ate_param = request.args.get('ate')

    if desde_param and ate_param:
        # Intervalo de calendário escolhido pelo usuário: datas explícitas,
        # fim do dia inclusivo (+1 dia no limite superior exclusivo).
        try:
            desde = datetime.fromisoformat(desde_param).replace(tzinfo=BR_TZ)
            ate = datetime.fromisoformat(ate_param).replace(tzinfo=BR_TZ) + timedelta(days=1)
        except ValueError:
            return jsonify({'error': 'datas inválidas'}), 422
        janela = core.periodo_fixo(desde, ate)
    else:
        # "Últimos N dias": janela medida pelo relógio do banco, sem limite
        # superior — lead criado há 1 segundo entra.
        janela = core.periodo_relativo(PERIODOS.get(periodo, 30) or 3650)

    return jsonify(core.metricas(janela))


# ─────────────────────────────────────────────────────────────────────────────
# Serialização
# ─────────────────────────────────────────────────────────────────────────────

def _lead_json(lead):
    d = core._row(lead)
    d['id'] = str(lead['id'])
    d['whatsapp_display'] = core.telefone_display(lead.get('whatsapp_e164'))
    # wa.me não aceita o "+"
    d['whatsapp_link'] = 'https://wa.me/' + (lead.get('whatsapp_e164') or '').lstrip('+')
    d['etapa_label'] = ETAPA_LABELS.get(lead.get('etapa'), lead.get('etapa'))
    d['status_label'] = STATUS_LABELS.get(lead.get('status'), lead.get('status'))

    # Minutos até o 1º contato: é o SLA do card, calculado aqui para o kanban
    # poder pintar o atraso sem fazer conta no front.
    criado = lead.get('created_at')
    contato = lead.get('primeiro_contato_em')
    if criado and contato:
        d['sla_min'] = round((contato - criado).total_seconds() / 60, 1)
        d['sla_ok'] = d['sla_min'] <= core.SLA_PRIMEIRO_CONTATO_MIN
    elif criado:
        d['sla_min'] = None
        d['aguardando_min'] = round((datetime.now(criado.tzinfo) - criado).total_seconds() / 60, 1)
        d['sla_ok'] = None
    return d
