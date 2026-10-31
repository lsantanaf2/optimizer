"""
Blueprint /crm — CRM de leads (braço do optimizer).

Duas superfícies:
  PÚBLICA   — captura de lead. Único endpoint aberto, com form_key, honeypot,
              validação server-side e rate limit. Nunca lê dado, só escreve.
  INTERNA   — kanban, ficha do lead e métricas. Tudo atrás de @login_required:
              dado pessoal de lead não passa por link com token nem por anon key.

Rotas:
  POST   /api/crm/lead                      — captura (público, CORS por form)
  POST   /api/crm/lead/parcial              — lead de abandono (público, sempre 204)
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
  DELETE /api/crm/leads/<id>?confirmar=1    — exclusão definitiva (irreversível)
  POST   /api/crm/tarefas/<id>/concluir     — conclui tarefa
  GET    /api/crm/tarefas                   — tarefas pendentes
  GET    /api/crm/novos?since=<iso>         — polling do alerta de lead novo
  GET    /api/crm/push/chave                — chave VAPID pública
  POST   /api/crm/push/inscrever            — registra o navegador para Web Push
  POST   /api/crm/push/cancelar             — remove a inscrição
  GET    /api/crm/metricas                  — métricas (JSON)
"""

import logging
import os
import re
from datetime import datetime, timedelta
from functools import wraps
from urllib.parse import urlparse

from flask import (
    Blueprint, abort, jsonify, make_response, redirect, render_template,
    request, session, url_for,
)

from modules import crm_core as core
from modules import crm_push
from modules import meta_capi
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


def _data_do_formulario(valor):
    """Interpreta data vinda de `<input type="datetime-local">`.

    Esse input manda "2026-10-23T15:00", SEM fuso. Um datetime naive entrando
    numa coluna timestamptz é carimbado pelo Postgres com o fuso da sessão —
    que aqui é UTC. Resultado: a pessoa agenda 15h, grava 15:00 UTC e a tela
    mostra 12:00. Três horas a menos, silenciosamente.

    O horário digitado é o do operador, então datetime sem fuso assume
    America/Sao_Paulo. Valor que já venha com fuso (ISO com Z ou offset) é
    respeitado como está.
    """
    d = datetime.fromisoformat(str(valor).strip().replace('Z', '+00:00'))
    return d if d.tzinfo is not None else d.replace(tzinfo=BR_TZ)


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


def _mesma_origem(origin):
    """A requisição veio da própria página servida por este app?

    Compara só o HOST, nunca a URL inteira. Atrás do nginx o TLS termina no
    proxy e o Flask enxerga `http://`, enquanto o browser manda
    `Origin: https://...`. Comparar com request.host_url (que inclui o scheme)
    fazia o app recusar o próprio formulário com CRM-403 em produção — não
    aparecia em teste local porque ali não existe proxy no meio.
    """
    if not origin:
        return False
    try:
        netloc = urlparse(origin).netloc.lower()
    except ValueError:
        return False
    return bool(netloc) and netloc == (request.host or '').lower()


def _origem_permitida(origin, form):
    """Origem própria, ou uma das cadastradas em crm_forms.allowed_origins."""
    if _mesma_origem(origin):
        return True
    permitidas = [str(o).rstrip('/').lower() for o in ((form or {}).get('allowed_origins') or [])]
    return origin.rstrip('/').lower() in permitidas


def _cors_headers(resp, form):
    """Libera CORS só para as origens cadastradas no formulário."""
    origin = request.headers.get('Origin')
    permitidas = [str(o).rstrip('/').lower() for o in ((form or {}).get('allowed_origins') or [])]
    if origin and origin.rstrip('/').lower() in permitidas:
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

    # CORS: a própria página passa; outra origem precisa estar cadastrada.
    origin = request.headers.get('Origin')
    if origin and not _origem_permitida(origin, form):
        logger.warning("CRM captura: origem não autorizada %s (host=%s form=%s)",
                       origin, request.host, form_key)
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
    # O faturamento declarado no formulário é texto ("Até 20 mil") e fica no
    # payload da submissão, exatamente como a pessoa marcou. Não é convertido
    # em número: a coluna crm_leads.faturamento_anual é do operador, para o
    # valor que ele apurar na call.
    conhecidos = set(atribuicao) | {
        'nome', 'email', 'whatsapp', 'telefone', 'empresa_segmento', 'form_key',
        'first_seen_at', 'ad_id', 'adset_id', 'campaign_id',
        HONEYPOT_FIELD,
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

    # Conversions API: mesmo event_id que o Pixel do browser usa, para a Meta
    # deduplicar. O id da submissão serve bem — é um UUID por cadastro, então
    # um recadastro vira uma conversão nova, e um reenvio da mesma submissão
    # não duplica. Roda em thread: o lead já está salvo e a resposta não espera
    # a Meta.
    event_id = res['submission_id']
    try:
        meta_capi.enviar_lead(
            event_id=event_id,
            email=email,
            telefone_e164=e164,
            nome=nome,
            fbp=atribuicao.get('fbp'),
            fbc=atribuicao.get('fbc'),
            ip=atribuicao.get('ip'),
            user_agent=atribuicao.get('user_agent'),
            event_source_url=atribuicao.get('landing_url'),
            custom={
                'lead_event_source': 'optimizer-crm',
                'utm_source': atribuicao.get('utm_source'),
                'utm_campaign': atribuicao.get('utm_campaign'),
            },
        )
    except Exception:
        # Falha de CAPI nunca pode derrubar a captura do lead.
        logger.exception("CRM captura: erro ao disparar CAPI do lead %s", res['lead_id'])

    # Push: é o único caminho que avisa com a aba do CRM fechada. Em thread,
    # depois do lead já estar salvo — falha de push nunca derruba a captura.
    try:
        crm_push.avisar_lead(
            nome=nome,
            whatsapp=core.telefone_display(e164),
            origem=' · '.join(x for x in (atribuicao.get('utm_source'),
                                          atribuicao.get('utm_campaign')) if x) or 'direto',
            lead_id=res['lead_id'],
            recadastro=not res['is_novo'],
        )
    except Exception:
        logger.exception("CRM captura: erro ao disparar push do lead %s", res['lead_id'])

    return responder({
        'ok': True,
        'is_novo': res['is_novo'],
        # O browser dispara fbq('track','Lead') com ESTE id; sem ele os dois
        # caminhos virariam duas conversões e o CPL no gerenciador cairia pela
        # metade.
        'event_id': event_id,
        'redirect': form.get('redirect_url') or url_for('crm.obrigado'),
    })


@crm_bp.route('/api/crm/lead/parcial', methods=['POST', 'OPTIONS'])
def captura_parcial():
    """Salva quem digitou o telefone e abandonou o formulário.

    Chamado pelo próprio navegador do visitante, inclusive via sendBeacon no
    fechamento da página — por isso responde sempre 204, sem corpo: beacon
    ignora a resposta e um erro aqui não pode aparecer para o visitante.

    NÃO dispara Pixel nem CAPI: abandono não é conversão, e reportá-lo à Meta
    como Lead estragaria a otimização da campanha.
    """
    form_key = ''
    try:
        dados = request.get_json(silent=True) or request.form.to_dict() or {}
        form_key = (dados.get('form_key') or '').strip()
        form = _get_form(form_key)
        if not form:
            return '', 204

        origin = request.headers.get('Origin')
        if origin and not _origem_permitida(origin, form):
            return '', 204

        if (dados.get(HONEYPOT_FIELD) or '').strip():
            return '', 204

        # Teto generoso: o disparo acontece no blur e no fechamento, então um
        # visitante sozinho pode gerar alguns. Só freia abuso real.
        if not is_allowed(f'crm-parcial|{_client_ip()}', 20, 60):
            return '', 204

        # Sem telefone válido não há lead: whatsapp_e164 é a chave da tabela.
        try:
            e164, pais = core.normalizar_telefone(dados.get('whatsapp') or dados.get('telefone'))
        except TelefoneInvalido:
            return '', 204
        try:
            email = core.validar_email(dados.get('email'))
        except TelefoneInvalido:
            email = None

        res = core.registrar_parcial(
            whatsapp_e164=e164,
            pais=pais,
            nome=core.limpar_texto(dados.get('nome'), 200),
            email=email,
            form_key=form_key,
            atribuicao={
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
                'fbclid': dados.get('fbclid'), 'gclid': dados.get('gclid'),
                'fbp': dados.get('fbp') or request.cookies.get('_fbp'),
                'fbc': dados.get('fbc') or request.cookies.get('_fbc'),
                'ttclid': dados.get('ttclid'),
                'meta_ad_id': dados.get('ad_id'),
                'meta_adset_id': dados.get('adset_id'),
                'meta_campaign_id': dados.get('campaign_id'),
                'referrer': dados.get('referrer') or request.referrer,
                'landing_url': dados.get('landing_url'),
                'ip': _client_ip(),
                'user_agent': request.headers.get('User-Agent'),
            },
            payload={'motivo': core.limpar_texto(dados.get('motivo'), 40)},
        )
        if res:
            logger.info("CRM: lead PARCIAL %s via %s", res['lead_id'],
                        dados.get('utm_source') or 'direto')
    except Exception:
        # Abandono é melhor-esforço: nunca pode estourar erro no visitante.
        logger.exception("CRM parcial: falha ao gravar (form=%s)", form_key)
    return '', 204


@crm_bp.route('/lp/<slug>')
def landing_page(slug):
    form = _get_form_por_slug(slug)
    if not form:
        abort(404)
    return _render_lp(form)


def _get_form_por_slug(slug):
    return fetch_one(
        "SELECT * FROM crm_forms WHERE slug = %s AND enabled IS TRUE",
        (slug,),
    )


def _render_lp(form):
    # Template por convenção: crm_lp_<slug>.html quando existir, senão a LP
    # padrão. Uma oferta nova com copy própria só precisa do arquivo — sem
    # coluna nova no banco e sem if por slug espalhado no código.
    from jinja2 import TemplateNotFound
    slug = (form.get('slug') or '').strip().lower()

    # O slug vem do banco e vira nome de arquivo: aceita só o que casa com o
    # formato esperado, para não virar caminho relativo.
    if slug and re.fullmatch(r'[a-z0-9_-]{1,64}', slug):
        try:
            return render_template(f'crm_lp_{slug}.html', form=form,
                                   honeypot_field=HONEYPOT_FIELD,
                                   pixel_id=meta_capi.PIXEL_ID)
        except TemplateNotFound:
            pass

    # O pixel vem do mesmo lugar que a CAPI usa, para os dois caminhos nunca
    # apontarem para pixels diferentes.
    return render_template('crm_lp.html', form=form, honeypot_field=HONEYPOT_FIELD,
                           pixel_id=meta_capi.PIXEL_ID)


# ─────────────────────────────────────────────────────────────────────────────
# Subdomínio de captura (cadastro.tráfego.com)
# ─────────────────────────────────────────────────────────────────────────────
# Nesse host a raiz é a LP padrão e /<slug> serve as demais LPs cadastradas em
# crm_forms — criar uma oferta nova não exige mexer em DNS nem em nginx.
#
# O host vem em punycode: 'tráfego.com' é 'xn--trfego-qta.com'. O default já
# inclui o subdomínio para o deploy não depender de editar o deploy.sh da VPS
# (que é gitignored); CRM_LP_HOSTS sobrescreve se precisar.
LP_HOSTS = {
    h.strip().lower()
    for h in os.environ.get('CRM_LP_HOSTS', 'cadastro.xn--trfego-qta.com').split(',')
    if h.strip()
}
LP_SLUG_PADRAO = os.environ.get('CRM_LP_DEFAULT_SLUG', 'sk')

# Nesse host o app serve SÓ a captura. O resto (incluindo /crm e o login)
# responde 404: o subdomínio vai em anúncio, não tem por que expor o painel.
# '/lp/' entra aqui para o link canônico antigo continuar valendo nos dois
# hosts — um /lp/sk que dá 404 só no subdomínio é armadilha na hora de
# compartilhar o link.
LP_HOST_PREFIXOS_LIVRES = ('/api/crm/lead', '/static/', '/obrigado', '/lp/', '/favicon.ico')


@crm_bp.before_app_request
def _lp_por_host():
    host = (request.host or '').lower().split(':')[0]
    if host not in LP_HOSTS:
        return None

    caminho = request.path
    if caminho.startswith(LP_HOST_PREFIXOS_LIVRES):
        return None

    enxuto = caminho.rstrip('/')
    if enxuto in ('', '/'):
        form = _get_form_por_slug(LP_SLUG_PADRAO)
        if not form:
            logger.error("CRM LP: slug padrão '%s' não existe em crm_forms", LP_SLUG_PADRAO)
            abort(404)
        return _render_lp(form)

    # /<slug> — um nível só; nada de /a/b nem de slug fora do formato
    slug = enxuto[1:]
    if '/' not in slug and re.fullmatch(r'[a-z0-9-]{1,64}', slug):
        form = _get_form_por_slug(slug)
        if form:
            return _render_lp(form)

    abort(404)


@crm_bp.route('/obrigado')
def obrigado():
    return render_template('crm_obrigado.html', pixel_id=meta_capi.PIXEL_ID)


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
        incluir_parciais=request.args.get('parciais') == '1',
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
            due = _data_do_formulario(dados['data_recontato'])
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
        due = _data_do_formulario(dados['due_at'])
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


@crm_bp.route('/api/crm/leads/<lead_id>', methods=['DELETE'])
@_login_required
def api_excluir_lead(lead_id):
    """Exclusão definitiva. Não tem desfazer.

    Exige ?confirmar=1: um DELETE disparado por engano (link, prefetch, script
    de teste) não apaga nada sem a intenção explícita.
    """
    if request.args.get('confirmar') != '1':
        return jsonify({'error': 'confirmação ausente'}), 400

    apagado = core.excluir_lead(lead_id)
    if not apagado:
        return jsonify({'error': 'lead não encontrado'}), 404

    # Fica no log porque a linha some do banco: é o único rastro de que esse
    # lead existiu, quem apagou e quanto foi junto.
    logger.warning(
        "CRM: EXCLUSAO DEFINITIVA do lead %s por user=%s — '%s' (%s), etapa=%s, "
        "%s linha(s): %s submissao(oes), %s evento(s), %s nota(s), %s tarefa(s)",
        lead_id, _autor(), apagado.get('nome'), apagado.get('whatsapp_e164'),
        apagado.get('etapa'), apagado['linhas_removidas'], apagado['submissoes'],
        apagado['eventos'], apagado['notas'], apagado['tarefas'],
    )
    return jsonify({'ok': True, 'apagado': core._row(apagado)})


@crm_bp.route('/api/crm/push/chave')
@_login_required
def api_push_chave():
    """Chave pública VAPID, para o navegador se inscrever."""
    return jsonify({'chave': crm_push.VAPID_PUBLIC_KEY,
                    'habilitado': crm_push.habilitado()})


@crm_bp.route('/api/crm/push/inscrever', methods=['POST'])
@_login_required
def api_push_inscrever():
    """Guarda a inscrição deste navegador."""
    d = request.get_json(silent=True) or {}
    endpoint = (d.get('endpoint') or '').strip()
    keys = d.get('keys') or {}
    if not endpoint or not keys.get('p256dh') or not keys.get('auth'):
        return jsonify({'error': 'inscrição incompleta'}), 422
    crm_push.registrar(endpoint, keys['p256dh'], keys['auth'],
                       user_id=_autor(), user_agent=request.headers.get('User-Agent'))
    logger.info("Push: navegador inscrito (user=%s)", _autor())
    return jsonify({'ok': True})


@crm_bp.route('/api/crm/push/cancelar', methods=['POST'])
@_login_required
def api_push_cancelar():
    d = request.get_json(silent=True) or {}
    endpoint = (d.get('endpoint') or '').strip()
    if endpoint:
        crm_push.remover(endpoint)
    return jsonify({'ok': True})


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
    # O relógio do BANCO, não o da aplicação. Os eventos são carimbados com
    # NOW() do Postgres, que está adiantado em relação à VPS; devolver a hora
    # da aplicação fazia o lastSeen do navegador ficar sempre atrás dos
    # eventos — e o MESMO lead era redescoberto a cada poll, para sempre.
    # Na prática: telefone tocando sem parar mesmo depois de atender.
    agora = fetch_one('SELECT NOW() AS agora')['agora']
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

    return jsonify(core.metricas(
        janela, incluir_parciais=request.args.get('parciais') == '1'))


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

    # O card tem TRÊS estados, não dois. O erro anterior era ter só
    # "contatado" e "aguardando": quem mandava mensagem e não era respondido
    # caía em "aguardando" e o painel mostrava como se nada tivesse sido feito.
    #   nao_abordado → ninguém falou com ele ainda (o relógio do SLA corre)
    #   sem_resposta → você abordou, ele não respondeu (SLA cumprido)
    #   respondeu    → conversa iniciada
    criado = lead.get('created_at')
    tentativa = lead.get('primeira_tentativa_em')
    resposta = lead.get('primeiro_contato_em')

    def _min(chave):
        # Os *_db vêm da query do kanban, já medidos pelo relógio do banco.
        v = lead.get(chave)
        return round(float(v), 1) if v is not None else None

    if criado and tentativa:
        d['sla_min'] = _min('sla_min_db') if 'sla_min_db' in lead else             round((tentativa - criado).total_seconds() / 60, 1)
        d['sla_ok'] = d['sla_min'] is not None and d['sla_min'] <= core.SLA_PRIMEIRO_CONTATO_MIN
        if resposta:
            d['contato_estado'] = 'respondeu'
            d['resposta_min'] = round((resposta - criado).total_seconds() / 60, 1)
        else:
            d['contato_estado'] = 'sem_resposta'
            # Há quanto tempo a mensagem está sem resposta — é isso que diz
            # quando vale insistir.
            d['aguardando_resposta_min'] = _min('sem_resposta_min_db')
    elif criado:
        d['contato_estado'] = 'nao_abordado'
        d['sla_min'] = None
        d['sla_ok'] = None
        d['aguardando_min'] = _min('aguardando_min_db')
    return d
