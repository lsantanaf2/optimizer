"""
CRM — núcleo de dados e regras.

Separado do blueprint (modules/crm.py) porque as regras aqui são testáveis sem
request context: normalização de telefone, dedupe por pessoa, registro de
eventos e cálculo de métricas.

Modelo "pessoa + submissões" (ver database/migrations/2026-10-17-add-crm.sql):
o mesmo lead cadastrando 3x gera 1 linha em crm_leads e 3 em
crm_lead_submissions — o card não duplica e o histórico de origem fica inteiro.
"""

import logging
import re
import unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo

from modules.database import fetch_all, fetch_one, get_cursor

logger = logging.getLogger(__name__)

BR_TZ = ZoneInfo('America/Sao_Paulo')

# Funil enxuto. A ordem importa: é usada no kanban e no cálculo de conversão
# etapa a etapa.
ETAPAS = ['novo', 'contatado', 'call_agendada', 'proposta', 'fechado']
ETAPA_LABELS = {
    'novo': 'Novo',
    'contatado': 'Contatado',
    'call_agendada': 'Call Agendada',
    'proposta': 'Proposta',
    'fechado': 'Fechado',
}
STATUS_LABELS = {
    'ativo': 'Ativo',
    'perdido': 'Perdido',
    'timing_errado': 'Timing Errado',
}

# Meta de SLA do 1º contato, em minutos. Lead de tráfego pago esfria rápido:
# 15min é o padrão de mercado para inbound.
SLA_PRIMEIRO_CONTATO_MIN = 15


# ─────────────────────────────────────────────────────────────────────────────
# Telefone — validação internacional
# ─────────────────────────────────────────────────────────────────────────────
# Requisito: DDI + DDD + número, tudo validado, MAS sem descartar estrangeiros.
# A lib `phonenumbers` (base offline do Google) é a única forma honesta de
# validar número de qualquer país. Se ela não estiver instalada, cai no
# fallback: valida Brasil com rigor e aceita o resto só com checagem de formato
# E.164 — nunca rejeita um estrangeiro válido por falta da lib.

try:
    import phonenumbers as _pn
    _HAS_PN = True
except ImportError:  # pragma: no cover
    _pn = None
    _HAS_PN = False
    logger.warning(
        "phonenumbers não instalado — validação de telefone em modo fallback "
        "(Brasil rigoroso, internacional só formato)"
    )

# DDDs brasileiros válidos (não existe 10, 20, 23, 25, 26, 29, 30, 36, 39, 40,
# 50, 52, 56, 57, 58, 59, 60, 70, 72, 76, 78, 80, 90)
_DDD_BR = {
    11, 12, 13, 14, 15, 16, 17, 18, 19,
    21, 22, 24, 27, 28,
    31, 32, 33, 34, 35, 37, 38,
    41, 42, 43, 44, 45, 46, 47, 48, 49,
    51, 53, 54, 55,
    61, 62, 63, 64, 65, 66, 67, 68, 69,
    71, 73, 74, 75, 77, 79,
    81, 82, 83, 84, 85, 86, 87, 88, 89,
    91, 92, 93, 94, 95, 96, 97, 98, 99,
}


class TelefoneInvalido(ValueError):
    """Telefone não passou na validação. A mensagem é exibida ao lead."""


def normalizar_telefone(raw, pais_padrao='BR'):
    """Valida e normaliza para E.164.

    Retorna (e164, iso2_pais). Levanta TelefoneInvalido com mensagem
    apresentável ao usuário final.

    Aceita: "+55 11 98765-4321", "5511987654321", "(11) 98765-4321",
            "+351 912 345 678".
    Sem DDI explícito, assume `pais_padrao` (BR) — a LP é brasileira.
    """
    if not raw or not str(raw).strip():
        raise TelefoneInvalido('Informe seu WhatsApp com DDD.')

    texto = str(raw).strip()
    tinha_mais = texto.lstrip().startswith('+')
    digitos = re.sub(r'\D', '', texto)

    if not digitos:
        raise TelefoneInvalido('WhatsApp inválido: informe apenas números com DDD.')

    # "00" como prefixo internacional (ex.: 0055...) vira "+"
    if digitos.startswith('00') and len(digitos) > 10:
        digitos = digitos[2:]
        tinha_mais = True

    if _HAS_PN:
        return _normalizar_com_lib(digitos, tinha_mais, pais_padrao)
    return _normalizar_fallback(digitos, tinha_mais)


def _normalizar_com_lib(digitos, tinha_mais, pais_padrao):
    candidatos = []
    if tinha_mais:
        candidatos.append('+' + digitos)
    else:
        # Sem "+" é ambíguo: "11987654321" é BR local, "5511987654321" já tem DDI.
        # Tenta como nacional do país padrão e como internacional.
        candidatos.append(digitos)          # parse com region=pais_padrao
        candidatos.append('+' + digitos)    # já veio com DDI, sem o "+"

    for cand in candidatos:
        try:
            num = _pn.parse(cand, None if cand.startswith('+') else pais_padrao)
        except _pn.NumberParseException:
            continue
        if not _pn.is_valid_number(num):
            continue
        e164 = _pn.format_number(num, _pn.PhoneNumberFormat.E164)
        iso2 = _pn.region_code_for_number(num) or ''
        return e164, iso2

    raise TelefoneInvalido(
        'WhatsApp inválido. Use DDI + DDD + número — ex.: +55 11 98765-4321.'
    )


def _normalizar_fallback(digitos, tinha_mais):
    """Sem phonenumbers: Brasil com rigor, internacional só formato E.164."""
    # Caso 1: número brasileiro local (DDD + 8/9 dígitos)
    if not tinha_mais and len(digitos) in (10, 11):
        ddd = int(digitos[:2])
        if ddd not in _DDD_BR:
            raise TelefoneInvalido(f'DDD {digitos[:2]} não existe no Brasil.')
        if len(digitos) == 11 and digitos[2] != '9':
            raise TelefoneInvalido('Celular brasileiro de 11 dígitos deve começar com 9 após o DDD.')
        return '+55' + digitos, 'BR'

    # Caso 2: já veio com DDI do Brasil
    if digitos.startswith('55') and len(digitos) in (12, 13):
        resto = digitos[2:]
        ddd = int(resto[:2])
        if ddd not in _DDD_BR:
            raise TelefoneInvalido(f'DDD {resto[:2]} não existe no Brasil.')
        if len(resto) == 11 and resto[2] != '9':
            raise TelefoneInvalido('Celular brasileiro de 11 dígitos deve começar com 9 após o DDD.')
        return '+' + digitos, 'BR'

    # Caso 3: estrangeiro — aceita no formato E.164 sem fingir validar o plano
    # de numeração do país (seria errado rejeitar um número válido por isso).
    if 7 <= len(digitos) <= 15 and digitos[0] != '0':
        return '+' + digitos, ''

    raise TelefoneInvalido(
        'WhatsApp inválido. Use DDI + DDD + número — ex.: +55 11 98765-4321.'
    )


def telefone_display(e164):
    """Formata para leitura humana. +5511987654321 -> +55 (11) 98765-4321."""
    if not e164:
        return ''
    if _HAS_PN:
        try:
            num = _pn.parse(e164, None)
            return _pn.format_number(num, _pn.PhoneNumberFormat.INTERNATIONAL)
        except Exception:
            pass
    m = re.match(r'^\+55(\d{2})(\d{4,5})(\d{4})$', e164)
    if m:
        return f'+55 ({m.group(1)}) {m.group(2)}-{m.group(3)}'
    return e164


# ─────────────────────────────────────────────────────────────────────────────
# Sanitização de texto
# ─────────────────────────────────────────────────────────────────────────────

def limpar_texto(valor, max_len=200):
    """Normaliza, remove control chars e corta. None se vazio."""
    if valor is None:
        return None
    s = unicodedata.normalize('NFC', str(valor)).strip()
    s = ''.join(ch for ch in s if ch == '\n' or ord(ch) >= 32)
    s = re.sub(r'[ \t]{2,}', ' ', s)
    if not s:
        return None
    return s[:max_len]


_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s.]+\.[^@\s]{2,}$')


def validar_email(valor):
    """Retorna email em minúsculas, ou None se vazio. Levanta se inválido."""
    email = limpar_texto(valor, 200)
    if not email:
        return None
    email = email.lower()
    if not _EMAIL_RE.match(email):
        raise TelefoneInvalido('E-mail inválido.')
    return email


# ─────────────────────────────────────────────────────────────────────────────
# Captura — dedupe por pessoa + submissão sempre nova
# ─────────────────────────────────────────────────────────────────────────────

_SUB_CAMPOS = [
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term',
    'first_utm_source', 'first_utm_medium', 'first_utm_campaign',
    'first_utm_content', 'first_utm_term',
    'fbclid', 'gclid', 'fbp', 'fbc', 'ttclid',
    'meta_ad_id', 'meta_adset_id', 'meta_campaign_id',
    'referrer', 'landing_url', 'ip', 'user_agent',
]


def registrar_parcial(*, whatsapp_e164, pais, nome, email, form_key,
                      atribuicao, payload=None):
    """Grava quem digitou o telefone e foi embora sem enviar.

    Nasce com `captura_parcial = TRUE` e `submissions_count = 0`: ele ainda
    não submeteu nada. Esse zero é o que faz a conta fechar depois — quando a
    pessoa volta e envia de verdade, o upsert soma 1 e ela conta como primeiro
    cadastro, não como recadastro.

    Se o telefone já existe (parcial anterior ou lead completo), NÃO mexe em
    nada: um abandono não pode sobrescrever quem já se cadastrou direito.
    """
    import json

    atrib = {k: limpar_texto(atribuicao.get(k), 500) for k in _SUB_CAMPOS}

    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO crm_leads (
                whatsapp_e164, whatsapp_pais, nome, email,
                captura_parcial, submissions_count
            ) VALUES (%s, %s, %s, %s, TRUE, 0)
            ON CONFLICT (whatsapp_e164) DO NOTHING
            RETURNING id
            """,
            (whatsapp_e164, pais or None, nome, email),
        )
        row = cur.fetchone()
        if not row:
            return None          # já existia — nada a fazer

        lead_id = row['id']

        # A submissão parcial guarda a atribuição: é dela que sai de qual
        # anúncio veio o abandono, que é justamente o que o usuário quer medir.
        cols = ['lead_id', 'form_key', 'payload'] + _SUB_CAMPOS
        vals = [lead_id, form_key,
                json.dumps(dict(payload or {}, parcial=True), ensure_ascii=False)] + \
               [atrib[k] for k in _SUB_CAMPOS]
        cur.execute(
            f"INSERT INTO crm_lead_submissions ({', '.join(cols)}) "
            f"VALUES ({', '.join(['%s'] * len(cols))}) RETURNING id",
            vals,
        )
        submission_id = cur.fetchone()['id']

        registrar_evento(cur, lead_id, 'lead_parcial',
                         payload={'submission_id': str(submission_id),
                                  'form_key': form_key,
                                  'utm_source': atrib.get('utm_source'),
                                  'utm_campaign': atrib.get('utm_campaign')})

    return {'lead_id': str(lead_id), 'submission_id': str(submission_id)}


def registrar_captura(*, whatsapp_e164, pais, nome, email, empresa_segmento,
                      form_key, atribuicao, first_seen_at=None, payload=None):
    """Grava um cadastro. Tudo numa transação.

    Retorna dict com lead_id, submission_id, is_novo e submissions_count.
    - lead já existe (mesmo whatsapp): NÃO cria card novo. Cria submission,
      incrementa contador, registra evento 'recadastro' e completa campos que
      estavam vazios (nunca sobrescreve dado já preenchido pelo operador).
    - lead novo: cria card em 'novo' + evento 'lead_criado'.
    """
    import json

    atrib = {k: limpar_texto(atribuicao.get(k), 500) for k in _SUB_CAMPOS}
    payload_json = json.dumps(payload or {}, ensure_ascii=False)

    with get_cursor() as cur:
        # Upsert atômico em vez de SELECT-depois-INSERT: dois submits simultâneos
        # do mesmo número (duplo clique, retry do browser) colidiriam na unique
        # key e devolveriam 500. Aqui o próprio Postgres resolve o conflito.
        #
        # COALESCE(crm_leads.x, EXCLUDED.x): o recadastro só PREENCHE o que está
        # vazio. O que o operador já corrigiu no CRM nunca é sobrescrito.
        #
        # submissions_count diz se é novo: nasce 1 no insert e incrementa no
        # conflito, então count == 1 só acontece no primeiro cadastro.
        # Precisa ser lido ANTES do upsert: o RETURNING devolve a linha nova,
        # e EXCLUDED não pode ser referenciado dentro de subquery ali.
        cur.execute(
            "SELECT captura_parcial FROM crm_leads WHERE whatsapp_e164 = %s",
            (whatsapp_e164,),
        )
        anterior = cur.fetchone()
        veio_de_abandono = bool(anterior and anterior['captura_parcial'])

        cur.execute(
            """
            INSERT INTO crm_leads (
                whatsapp_e164, whatsapp_pais, nome, email, empresa_segmento,
                submissions_count, last_submission_at
            ) VALUES (%s, %s, %s, %s, %s, 1, NOW())
            ON CONFLICT (whatsapp_e164) DO UPDATE SET
                nome               = COALESCE(crm_leads.nome, EXCLUDED.nome),
                email              = COALESCE(crm_leads.email, EXCLUDED.email),
                empresa_segmento   = COALESCE(crm_leads.empresa_segmento,
                                              EXCLUDED.empresa_segmento),
                whatsapp_pais      = COALESCE(crm_leads.whatsapp_pais,
                                              EXCLUDED.whatsapp_pais),
                submissions_count  = crm_leads.submissions_count + 1,
                last_submission_at = NOW(),
                -- Enviou o formulário: deixa de ser abandono. A marca cai
                -- sozinha, senão o lead ficaria fora das métricas para sempre.
                captura_parcial    = FALSE
            RETURNING id, submissions_count
            """,
            (whatsapp_e164, pais or None, nome, email, empresa_segmento),
        )
        row = cur.fetchone()
        lead_id = row['id']
        count = row['submissions_count']
        is_novo = (count == 1)

        cols = ['lead_id', 'form_key', 'first_seen_at', 'payload'] + _SUB_CAMPOS
        vals = [lead_id, form_key, first_seen_at, payload_json] + [atrib[k] for k in _SUB_CAMPOS]
        cur.execute(
            f"INSERT INTO crm_lead_submissions ({', '.join(cols)}) "
            f"VALUES ({', '.join(['%s'] * len(cols))}) RETURNING id",
            vals,
        )
        submission_id = cur.fetchone()['id']

        cur.execute(
            """
            INSERT INTO crm_lead_events (lead_id, tipo, payload)
            VALUES (%s, %s, %s)
            """,
            (
                lead_id,
                'lead_completado' if veio_de_abandono else
                ('lead_criado' if is_novo else 'recadastro'),
                json.dumps({
                    'submission_id': str(submission_id),
                    'form_key': form_key,
                    'utm_source': atrib.get('utm_source'),
                    'utm_campaign': atrib.get('utm_campaign'),
                    'vez': count,
                }, ensure_ascii=False),
            ),
        )

    return {
        'lead_id': str(lead_id),
        'submission_id': str(submission_id),
        'is_novo': is_novo,
        'submissions_count': count,
        'veio_de_abandono': veio_de_abandono,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Leitura — kanban e detalhe
# ─────────────────────────────────────────────────────────────────────────────

def listar_leads(*, dias=None, incluir_arquivados=False, status=None, owner_id=None,
                 incluir_parciais=False):
    """Leads para o kanban, já com a origem do último cadastro.

    `dias` = janela 'últimos N dias' medida pelo relógio do banco (mesmo motivo
    de periodo_relativo: o relógio da aplicação pode estar deslocado).
    """
    where = []
    params = []
    if not incluir_arquivados:
        where.append('l.arquivado_em IS NULL')
    # Abandono fica fora por padrão: quem não enviou o formulário não deve
    # aparecer junto de quem enviou, nem no kanban nem nas contas.
    if not incluir_parciais:
        where.append('l.captura_parcial IS FALSE')
    if dias:
        where.append('l.created_at >= NOW() - make_interval(days => %s)')
        params.append(int(dias))
    if status:
        where.append('l.status = %s')
        params.append(status)
    if owner_id:
        where.append('l.owner_id = %s')
        params.append(owner_id)
    clause = ('WHERE ' + ' AND '.join(where)) if where else ''

    return fetch_all(
        f"""
        SELECT
            l.*,
            s.utm_source, s.utm_medium, s.utm_campaign, s.utm_content,
            s.faturamento_faixa,
            -- Tempos calculados com o NOW() do BANCO, nunca no Python: o
            -- relógio da aplicação estava atrasado e o card chegou a mostrar
            -- "não abordado há -1,7min" num lead recém-criado.
            GREATEST(0, EXTRACT(EPOCH FROM (NOW() - l.created_at)) / 60)           AS aguardando_min_db,
            EXTRACT(EPOCH FROM (l.primeira_tentativa_em - l.created_at)) / 60      AS sla_min_db,
            GREATEST(0, EXTRACT(EPOCH FROM (NOW() - l.primeira_tentativa_em)) / 60) AS sem_resposta_min_db,
            u.email AS owner_email,
            (SELECT COUNT(*) FROM crm_tasks t
              WHERE t.lead_id = l.id AND t.status = 'pendente') AS tarefas_pendentes,
            (SELECT MIN(t.due_at) FROM crm_tasks t
              WHERE t.lead_id = l.id AND t.status = 'pendente') AS proxima_tarefa_em
        FROM crm_leads l
        LEFT JOIN app_users u ON u.id = l.owner_id
        LEFT JOIN LATERAL (
            SELECT utm_source, utm_medium, utm_campaign, utm_content,
                   -- rótulo que a pessoa marcou ("Até 20 mil"), não o número
                   -- estimado: é o que serve para qualificar de bate-pronto
                   payload->>'faturamento_faixa' AS faturamento_faixa
            FROM crm_lead_submissions
            WHERE lead_id = l.id
            ORDER BY created_at DESC LIMIT 1
        ) s ON TRUE
        {clause}
        ORDER BY l.created_at DESC
        """,
        tuple(params) or None,
    )


def obter_lead(lead_id):
    return fetch_one(
        """
        SELECT l.*, u.email AS owner_email
        FROM crm_leads l
        LEFT JOIN app_users u ON u.id = l.owner_id
        WHERE l.id = %s
        """,
        (lead_id,),
    )


def listar_submissions(lead_id):
    """Todas as vezes que esta pessoa se cadastrou, da mais recente para a mais antiga."""
    return fetch_all(
        "SELECT * FROM crm_lead_submissions WHERE lead_id = %s ORDER BY created_at DESC",
        (lead_id,),
    )


def listar_eventos(lead_id, limite=200):
    return fetch_all(
        """
        SELECT e.*, u.email AS autor_email
        FROM crm_lead_events e
        LEFT JOIN app_users u ON u.id = e.autor_id
        WHERE e.lead_id = %s
        ORDER BY e.created_at DESC
        LIMIT %s
        """,
        (lead_id, limite),
    )


def listar_notas(lead_id):
    return fetch_all(
        """
        SELECT n.*, u.email AS autor_email
        FROM crm_notes n
        LEFT JOIN app_users u ON u.id = n.autor_id
        WHERE n.lead_id = %s
        ORDER BY n.created_at DESC
        """,
        (lead_id,),
    )


def listar_tarefas(lead_id=None, apenas_pendentes=False, limite=200):
    where = []
    params = []
    if lead_id:
        where.append('t.lead_id = %s')
        params.append(lead_id)
    if apenas_pendentes:
        where.append("t.status = 'pendente'")
    clause = ('WHERE ' + ' AND '.join(where)) if where else ''
    params.append(limite)
    return fetch_all(
        f"""
        SELECT t.*, l.nome AS lead_nome, l.whatsapp_e164
        FROM crm_tasks t
        JOIN crm_leads l ON l.id = t.lead_id
        {clause}
        ORDER BY t.due_at ASC
        LIMIT %s
        """,
        tuple(params),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Escrita — sempre com evento (senão a métrica morre)
# ─────────────────────────────────────────────────────────────────────────────

def registrar_evento(cur, lead_id, tipo, *, autor_id=None, de_etapa=None,
                     para_etapa=None, de_status=None, para_status=None, payload=None):
    import json
    cur.execute(
        """
        INSERT INTO crm_lead_events
            (lead_id, tipo, de_etapa, para_etapa, de_status, para_status, autor_id, payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (lead_id, tipo, de_etapa, para_etapa, de_status, para_status, autor_id,
         json.dumps(payload or {}, ensure_ascii=False)),
    )


# Mudar para estas etapas implica que houve contato efetivo — usado para fechar
# o SLA de 1º contato mesmo quando o operador pula o botão "marcar contato".
_ETAPAS_QUE_IMPLICAM_CONTATO = {'contatado', 'call_agendada', 'proposta', 'fechado'}


def mover_lead(lead_id, *, etapa=None, status=None, motivo=None, autor_id=None):
    """Move de etapa e/ou status, gravando os eventos correspondentes."""
    if etapa and etapa not in ETAPAS:
        raise ValueError(f'Etapa inválida: {etapa}')
    if status and status not in STATUS_LABELS:
        raise ValueError(f'Status inválido: {status}')

    with get_cursor() as cur:
        cur.execute(
            "SELECT etapa, status, primeiro_contato_em FROM crm_leads WHERE id = %s FOR UPDATE",
            (lead_id,),
        )
        atual = cur.fetchone()
        if not atual:
            return None

        nova_etapa = etapa or atual['etapa']
        novo_status = status if status is not None else atual['status']
        marca_contato = (
            atual['primeiro_contato_em'] is None
            and nova_etapa in _ETAPAS_QUE_IMPLICAM_CONTATO
        )

        cur.execute(
            """
            UPDATE crm_leads SET
                etapa               = %s,
                status              = %s,
                motivo_saida        = CASE WHEN %s <> 'ativo' THEN COALESCE(%s, motivo_saida) ELSE motivo_saida END,
                primeiro_contato_em   = CASE WHEN %s THEN NOW() ELSE primeiro_contato_em END,
                ultimo_contato_em     = CASE WHEN %s THEN NOW() ELSE ultimo_contato_em END,
                -- Arrastar o card para Contatado ou adiante é, na prática,
                -- dizer que abordou: fecha o SLA do mesmo jeito.
                primeira_tentativa_em = CASE WHEN %s THEN COALESCE(primeira_tentativa_em, NOW())
                                             ELSE primeira_tentativa_em END,
                data_fechamento     = CASE WHEN %s = 'fechado' AND data_fechamento IS NULL
                                           THEN NOW() ELSE data_fechamento END
            WHERE id = %s
            """,
            (nova_etapa, novo_status, novo_status, motivo, marca_contato,
             marca_contato, nova_etapa in _ETAPAS_QUE_IMPLICAM_CONTATO,
             nova_etapa, lead_id),
        )

        if nova_etapa != atual['etapa']:
            registrar_evento(cur, lead_id, 'etapa_mudou', autor_id=autor_id,
                             de_etapa=atual['etapa'], para_etapa=nova_etapa,
                             payload={'motivo': motivo} if motivo else None)
            if nova_etapa == 'fechado':
                registrar_evento(cur, lead_id, 'ganho', autor_id=autor_id)
            if nova_etapa == 'call_agendada':
                registrar_evento(cur, lead_id, 'call_agendada', autor_id=autor_id)
            if nova_etapa == 'proposta':
                registrar_evento(cur, lead_id, 'proposta_enviada', autor_id=autor_id)

        if novo_status != atual['status']:
            registrar_evento(cur, lead_id, 'status_mudou', autor_id=autor_id,
                             de_status=atual['status'], para_status=novo_status,
                             payload={'motivo': motivo} if motivo else None)
            if novo_status == 'perdido':
                registrar_evento(cur, lead_id, 'perdido', autor_id=autor_id,
                                 payload={'motivo': motivo} if motivo else None)

        if marca_contato:
            registrar_evento(cur, lead_id, 'contato_efetivo', autor_id=autor_id,
                             payload={'origem': 'mudanca_de_etapa'})

    return obter_lead(lead_id)


_CAMPOS_QUALIFICACAO = {
    'nome': ('texto', 200),
    'email': ('email', None),
    'empresa_segmento': ('texto', 200),
    'faturamento_anual': ('numero', None),
    'investimento_midia': ('numero', None),
    'ja_fez_trafego': ('tri', None),
    'urgencia': ('urgencia', None),
    'qualificado': ('tri', None),
    'valor_proposta': ('numero', None),
    'valor_fechado': ('numero', None),
    'owner_id': ('uuid', None),
}


def atualizar_lead(lead_id, dados, autor_id=None):
    """Atualiza ficha/qualificação. Só campos conhecidos; grava evento com o diff."""
    sets, params, diff = [], [], {}

    for campo, (tipo, max_len) in _CAMPOS_QUALIFICACAO.items():
        if campo not in dados:
            continue
        bruto = dados[campo]
        if tipo == 'texto':
            valor = limpar_texto(bruto, max_len)
        elif tipo == 'email':
            valor = validar_email(bruto)
        elif tipo == 'numero':
            valor = _para_numero(bruto)
        elif tipo == 'tri':
            valor = _para_tri(bruto)
        elif tipo == 'urgencia':
            valor = limpar_texto(bruto, 20)
            if valor and valor not in ('imediata', '30_dias', 'sem_pressa'):
                raise ValueError(f'Urgência inválida: {valor}')
        elif tipo == 'uuid':
            valor = limpar_texto(bruto, 64)
        else:
            continue
        sets.append(f'{campo} = %s')
        params.append(valor)
        diff[campo] = valor

    if not sets:
        return obter_lead(lead_id)

    params.append(lead_id)
    with get_cursor() as cur:
        cur.execute(f"UPDATE crm_leads SET {', '.join(sets)} WHERE id = %s", tuple(params))
        # valor_fechado preenchido = data_fechamento se ainda vazia
        if diff.get('valor_fechado') is not None:
            cur.execute(
                "UPDATE crm_leads SET data_fechamento = COALESCE(data_fechamento, NOW()) WHERE id = %s",
                (lead_id,),
            )
        registrar_evento(cur, lead_id, 'qualificacao_atualizada',
                         autor_id=autor_id,
                         payload={k: (str(v) if v is not None else None) for k, v in diff.items()})
    return obter_lead(lead_id)


_TRI_MAP = {
    'true': True, 'sim': True, '1': True, 'yes': True,
    'false': False, 'nao': False, 'não': False, '0': False, 'no': False,
}


def _para_tri(bruto):
    """Tri-state: True / False / None (desconhecido).

    "Não sei ainda" é um estado real no início da qualificação — por isso
    ja_fez_trafego e qualificado são nullable, não boolean com default.
    """
    if bruto is None or (isinstance(bruto, str) and bruto.strip().lower() in
                         ('', 'null', 'none', 'desconhecido')):
        return None
    if isinstance(bruto, bool):
        return bruto
    valor = _TRI_MAP.get(str(bruto).strip().lower())
    if valor is None:
        raise ValueError(f'Valor inválido (esperado sim/não/vazio): {bruto}')
    return valor


def _para_numero(bruto):
    """Aceita "12.500,50", "12500.50", 12500 ou vazio."""
    if bruto in (None, '', 'null'):
        return None
    if isinstance(bruto, (int, float)):
        return float(bruto)
    s = re.sub(r'[^\d,.\-]', '', str(bruto))
    if not s:
        return None
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')   # 12.500,50
    elif ',' in s:
        s = s.replace(',', '.')                     # 12500,50
    try:
        return float(s)
    except ValueError:
        raise ValueError(f'Valor numérico inválido: {bruto}')


def registrar_contato(lead_id, *, canal, resultado, autor_id=None, observacao=None):
    """Registra uma tentativa de contato.

    QUALQUER tentativa fecha o SLA, inclusive "não atendeu": o SLA mede o
    tempo que o operador levou para abordar, e isso ele controla. Se o lead
    responde ou não, já é outra métrica (`primeiro_contato_em`).

    A tentativa também tira o lead de "Novo": aquela coluna é a fila de quem
    ainda não foi tocado, e deixar lá quem já recebeu mensagem fazia o painel
    mentir sobre o trabalho feito.
    """
    efetivo = resultado in ('atendeu', 'respondeu')
    with get_cursor() as cur:
        cur.execute("SELECT etapa FROM crm_leads WHERE id = %s", (lead_id,))
        atual = cur.fetchone()
        if not atual:
            return None
        sai_de_novo = atual['etapa'] == 'novo'

        cur.execute(
            """
            UPDATE crm_leads SET
                ultimo_contato_em     = NOW(),
                primeira_tentativa_em = COALESCE(primeira_tentativa_em, NOW()),
                primeiro_contato_em   = CASE WHEN %s AND primeiro_contato_em IS NULL
                                             THEN NOW() ELSE primeiro_contato_em END,
                etapa                 = CASE WHEN %s THEN 'contatado' ELSE etapa END
            WHERE id = %s
            """,
            (efetivo, sai_de_novo, lead_id),
        )
        if sai_de_novo:
            registrar_evento(cur, lead_id, 'etapa_mudou', autor_id=autor_id,
                             de_etapa='novo', para_etapa='contatado',
                             payload={'origem': 'tentativa_de_contato'})
        registrar_evento(cur, lead_id, 'contato_tentado', autor_id=autor_id,
                         payload={'canal': canal, 'resultado': resultado,
                                  'observacao': observacao})
        if efetivo:
            registrar_evento(cur, lead_id, 'contato_efetivo', autor_id=autor_id,
                             payload={'canal': canal})
    return obter_lead(lead_id)


def adicionar_nota(lead_id, texto, autor_id=None):
    texto = limpar_texto(texto, 5000)
    if not texto:
        raise ValueError('Nota vazia.')
    with get_cursor() as cur:
        cur.execute(
            "INSERT INTO crm_notes (lead_id, autor_id, texto) VALUES (%s, %s, %s) RETURNING id, created_at",
            (lead_id, autor_id, texto),
        )
        nota = cur.fetchone()
        registrar_evento(cur, lead_id, 'nota_adicionada', autor_id=autor_id,
                         payload={'preview': texto[:120]})
    return nota


def criar_tarefa(lead_id, *, due_at, tipo='recontato', titulo=None, motivo=None,
                 owner_id=None, autor_id=None):
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO crm_tasks (lead_id, owner_id, tipo, titulo, motivo, due_at)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING *
            """,
            (lead_id, owner_id, tipo, limpar_texto(titulo, 200),
             limpar_texto(motivo, 2000), due_at),
        )
        tarefa = cur.fetchone()
        registrar_evento(cur, lead_id, 'tarefa_criada', autor_id=autor_id,
                         payload={'tipo': tipo, 'due_at': str(due_at), 'motivo': motivo})
    return tarefa


def concluir_tarefa(tarefa_id, autor_id=None):
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE crm_tasks SET status = 'concluido', concluido_em = NOW()
            WHERE id = %s AND status = 'pendente' RETURNING *
            """,
            (tarefa_id,),
        )
        tarefa = cur.fetchone()
        if tarefa:
            registrar_evento(cur, tarefa['lead_id'], 'tarefa_concluida',
                             autor_id=autor_id, payload={'tarefa_id': str(tarefa_id)})
    return tarefa


def arquivar_lead(lead_id, arquivar=True, autor_id=None):
    # NOW() do banco, não do Python: todos os carimbos do CRM saem do mesmo
    # relógio, senão comparações entre eles ficam incoerentes.
    with get_cursor() as cur:
        cur.execute(
            "UPDATE crm_leads SET arquivado_em = CASE WHEN %s THEN NOW() ELSE NULL END "
            "WHERE id = %s",
            (bool(arquivar), lead_id),
        )
    return obter_lead(lead_id)


def excluir_lead(lead_id):
    """Apaga o lead e TUDO que depende dele. Irreversível.

    Submissões, eventos, notas e tarefas somem pela cascata do banco
    (ON DELETE CASCADE nas quatro tabelas filhas) — não há linha órfã depois.

    É para lixo: teste, spam, bot, trote. Lembrar que isso muda as métricas do
    período retroativamente, porque o lead deixa de ser contado no funil — o
    que é o comportamento correto para algo que nunca foi um lead de verdade.

    Retorna o que foi apagado (para o log), ou None se o lead não existia.
    """
    with get_cursor() as cur:
        # Conta antes: depois do DELETE não há como saber o tamanho do estrago.
        cur.execute(
            """
            SELECT l.nome, l.whatsapp_e164, l.etapa, l.status,
                   (SELECT count(*) FROM crm_lead_submissions s WHERE s.lead_id = l.id) AS submissoes,
                   (SELECT count(*) FROM crm_lead_events e WHERE e.lead_id = l.id)      AS eventos,
                   (SELECT count(*) FROM crm_notes n WHERE n.lead_id = l.id)            AS notas,
                   (SELECT count(*) FROM crm_tasks t WHERE t.lead_id = l.id)            AS tarefas
            FROM crm_leads l WHERE l.id = %s
            """,
            (lead_id,),
        )
        antes = cur.fetchone()
        if not antes:
            return None

        cur.execute("DELETE FROM crm_leads WHERE id = %s", (lead_id,))
        apagado = dict(antes)
        apagado['linhas_removidas'] = (
            1 + antes['submissoes'] + antes['eventos'] + antes['notas'] + antes['tarefas']
        )
    return apagado


def registrar_no_show(lead_id, autor_id=None, observacao=None):
    with get_cursor() as cur:
        registrar_evento(cur, lead_id, 'no_show', autor_id=autor_id,
                         payload={'observacao': observacao})
    return obter_lead(lead_id)


def registrar_call_realizada(lead_id, autor_id=None, observacao=None):
    with get_cursor() as cur:
        registrar_evento(cur, lead_id, 'call_realizada', autor_id=autor_id,
                         payload={'observacao': observacao})
    return obter_lead(lead_id)


# ─────────────────────────────────────────────────────────────────────────────
# Alerta de lead novo — polling
# ─────────────────────────────────────────────────────────────────────────────

def leads_desde(since_iso):
    """Leads e recadastros ocorridos depois de `since_iso`. Base do alerta."""
    return fetch_all(
        """
        SELECT
            e.id            AS event_id,
            e.created_at,
            e.tipo,
            l.id            AS lead_id,
            l.nome,
            l.whatsapp_e164,
            l.submissions_count,
            s.utm_source, s.utm_campaign
        FROM crm_lead_events e
        JOIN crm_leads l ON l.id = e.lead_id
        LEFT JOIN LATERAL (
            SELECT utm_source, utm_campaign FROM crm_lead_submissions
            WHERE lead_id = l.id ORDER BY created_at DESC LIMIT 1
        ) s ON TRUE
        WHERE e.tipo IN ('lead_criado', 'recadastro')
          AND e.created_at > %s
        ORDER BY e.created_at ASC
        LIMIT 50
        """,
        (since_iso,),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Métricas — calculadas sobre crm_lead_events
# ─────────────────────────────────────────────────────────────────────────────

def periodo_relativo(dias):
    """Janela 'últimos N dias' calculada pelo relógio DO BANCO.

    Por que não `datetime.now()`: as linhas são carimbadas com NOW() do
    Postgres. Se o relógio da aplicação estiver atrasado em relação ao do banco
    — e estava, 102s na primeira medição — um lead recém-criado fica FORA de um
    `created_at < agora_da_aplicação` e some das métricas. Comparar o relógio
    do banco com ele mesmo elimina a classe inteira de erro.
    """
    return {'dias': int(dias)}


def periodo_fixo(desde, ate):
    """Janela com datas explícitas (filtro de calendário)."""
    return {'desde': desde, 'ate': ate}


def _clausula_periodo(periodo):
    """Retorna (sql, params) do recorte temporal sobre `l.created_at`."""
    if 'dias' in periodo:
        return ('l.created_at >= NOW() - make_interval(days => %s)', (periodo['dias'],))
    return ('l.created_at >= %s AND l.created_at < %s', (periodo['desde'], periodo['ate']))


def metricas(periodo, owner_id=None, incluir_parciais=False):
    """Painel de métricas do período. Tudo derivado dos eventos.

    `periodo` vem de periodo_relativo(dias) ou periodo_fixo(desde, ate).
    """
    filtro_owner = 'AND l.owner_id = %s' if owner_id else ''
    p_owner = (owner_id,) if owner_id else ()
    sql_periodo, p_periodo = _clausula_periodo(periodo)
    # Incluir abandono infla o volume e derruba todas as taxas, porque ele
    # entra no denominador sem nunca ter sido um lead de verdade. Fora por
    # padrão; o painel liga quando o usuário quer avaliar esse público.
    if not incluir_parciais:
        filtro_owner += ' AND l.captura_parcial IS FALSE'

    # ── Volume e funil (por data de CRIAÇÃO do lead: coorte honesta) ──
    funil = fetch_one(
        f"""
        SELECT
            COUNT(*)                                                      AS leads,
            COUNT(*) FILTER (WHERE l.primeira_tentativa_em IS NOT NULL)   AS abordados,
            COUNT(*) FILTER (WHERE l.primeiro_contato_em IS NOT NULL)     AS contatados,
            COUNT(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM crm_lead_events e
                WHERE e.lead_id = l.id AND e.tipo = 'call_agendada'))     AS calls_agendadas,
            COUNT(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM crm_lead_events e
                WHERE e.lead_id = l.id AND e.tipo = 'call_realizada'))    AS calls_realizadas,
            COUNT(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM crm_lead_events e
                WHERE e.lead_id = l.id AND e.tipo = 'no_show'))           AS no_shows,
            COUNT(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM crm_lead_events e
                WHERE e.lead_id = l.id AND e.tipo = 'proposta_enviada'))  AS propostas,
            COUNT(*) FILTER (WHERE l.etapa = 'fechado')                   AS fechados,
            COUNT(*) FILTER (WHERE l.status = 'perdido')                  AS perdidos,
            COUNT(*) FILTER (WHERE l.status = 'timing_errado')            AS timing_errado,
            COUNT(*) FILTER (WHERE l.qualificado IS TRUE)                 AS qualificados,
            COUNT(*) FILTER (WHERE l.qualificado IS FALSE)                AS desqualificados,
            COUNT(*) FILTER (WHERE l.submissions_count > 1)               AS recorrentes,
            COALESCE(SUM(l.valor_fechado), 0)                             AS receita,
            COALESCE(AVG(l.valor_fechado) FILTER (WHERE l.valor_fechado IS NOT NULL), 0) AS ticket_medio
        FROM crm_leads l
        WHERE {sql_periodo} {filtro_owner}
        """,
        p_periodo + p_owner,
    ) or {}

    # ── SLA do 1º contato ──
    sla = fetch_one(
        f"""
        SELECT
            COUNT(*) AS com_contato,
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY EXTRACT(EPOCH FROM (l.primeira_tentativa_em - l.created_at)) / 60
            ) AS mediana_min,
            PERCENTILE_CONT(0.9) WITHIN GROUP (
                ORDER BY EXTRACT(EPOCH FROM (l.primeira_tentativa_em - l.created_at)) / 60
            ) AS p90_min,
            AVG(EXTRACT(EPOCH FROM (l.primeira_tentativa_em - l.created_at)) / 60) AS media_min,
            COUNT(*) FILTER (
                WHERE l.primeira_tentativa_em - l.created_at <= make_interval(mins => %s)
            ) AS dentro_sla
        FROM crm_leads l
        WHERE {sql_periodo}
          AND l.primeira_tentativa_em IS NOT NULL {filtro_owner}
        """,
        (SLA_PRIMEIRO_CONTATO_MIN,) + p_periodo + p_owner,
    ) or {}

    # ── Tempo de ciclo (criação → fechamento) ──
    ciclo = fetch_one(
        f"""
        SELECT
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY EXTRACT(EPOCH FROM (l.data_fechamento - l.created_at)) / 86400
            ) AS mediana_dias,
            AVG(EXTRACT(EPOCH FROM (l.data_fechamento - l.created_at)) / 86400) AS media_dias
        FROM crm_leads l
        WHERE {sql_periodo}
          AND l.data_fechamento IS NOT NULL {filtro_owner}
        """,
        p_periodo + p_owner,
    ) or {}

    # ── Por campanha (atribuição do ÚLTIMO cadastro de cada lead) ──
    por_campanha = fetch_all(
        f"""
        SELECT
            COALESCE(NULLIF(s.utm_source, ''), 'direto')   AS utm_source,
            COALESCE(NULLIF(s.utm_campaign, ''), '(sem campanha)') AS utm_campaign,
            COUNT(*)                                                   AS leads,
            COUNT(*) FILTER (WHERE l.qualificado IS TRUE)              AS qualificados,
            COUNT(*) FILTER (WHERE l.etapa = 'fechado')                AS fechados,
            COALESCE(SUM(l.valor_fechado), 0)                          AS receita
        FROM crm_leads l
        LEFT JOIN LATERAL (
            SELECT utm_source, utm_campaign FROM crm_lead_submissions
            WHERE lead_id = l.id ORDER BY created_at DESC LIMIT 1
        ) s ON TRUE
        WHERE {sql_periodo} {filtro_owner}
        GROUP BY 1, 2
        ORDER BY leads DESC
        LIMIT 50
        """,
        p_periodo + p_owner,
    )

    # ── Série diária ──
    diario = fetch_all(
        f"""
        SELECT
            (l.created_at AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
            COUNT(*)                                     AS leads,
            COUNT(*) FILTER (WHERE l.etapa = 'fechado')  AS fechados
        FROM crm_leads l
        WHERE {sql_periodo} {filtro_owner}
        GROUP BY 1 ORDER BY 1
        """,
        p_periodo + p_owner,
    )

    # ── Motivos de perda ──
    motivos_perda = fetch_all(
        f"""
        SELECT COALESCE(NULLIF(l.motivo_saida, ''), '(não informado)') AS motivo,
               COUNT(*) AS total
        FROM crm_leads l
        WHERE {sql_periodo}
          AND l.status = 'perdido' {filtro_owner}
        GROUP BY 1 ORDER BY total DESC LIMIT 20
        """,
        p_periodo + p_owner,
    )

    leads = int(funil.get('leads') or 0)
    abordados = int(funil.get('abordados') or 0)
    contatados = int(funil.get('contatados') or 0)
    calls_ag = int(funil.get('calls_agendadas') or 0)
    calls_rz = int(funil.get('calls_realizadas') or 0)
    propostas = int(funil.get('propostas') or 0)
    fechados = int(funil.get('fechados') or 0)
    com_contato = int(sla.get('com_contato') or 0)

    def taxa(num, den):
        return round(100.0 * num / den, 1) if den else 0.0

    return {
        'periodo': {k: (v.isoformat() if hasattr(v, 'isoformat') else v)
                    for k, v in periodo.items()},
        'volume': {
            'leads': leads,
            'recorrentes': int(funil.get('recorrentes') or 0),
            'qualificados': int(funil.get('qualificados') or 0),
            'desqualificados': int(funil.get('desqualificados') or 0),
            'perdidos': int(funil.get('perdidos') or 0),
            'timing_errado': int(funil.get('timing_errado') or 0),
        },
        'sla': {
            'meta_min': SLA_PRIMEIRO_CONTATO_MIN,
            'leads_abordados': com_contato,
            'sem_abordar': leads - com_contato,
            'mediana_min': _num(sla.get('mediana_min')),
            'p90_min': _num(sla.get('p90_min')),
            'media_min': _num(sla.get('media_min')),
            'dentro_sla': int(sla.get('dentro_sla') or 0),
            'pct_dentro_sla': taxa(int(sla.get('dentro_sla') or 0), com_contato),
            'pct_abordados': taxa(abordados, leads),
        },
        'funil': {
            'leads': leads,
            'abordados': abordados,
            'contatados': contatados,
            'calls_agendadas': calls_ag,
            'calls_realizadas': calls_rz,
            'no_shows': int(funil.get('no_shows') or 0),
            'propostas': propostas,
            'fechados': fechados,
            # Resposta: dos que você abordou, quantos responderam. Separado
            # do SLA de propósito — um é seu trabalho, o outro é reação deles.
            'taxa_resposta': taxa(contatados, abordados),
            'taxa_conexao': taxa(contatados, leads),
            'taxa_agendamento': taxa(calls_ag, contatados),
            'show_rate': taxa(calls_rz, calls_ag),
            'taxa_proposta': taxa(propostas, calls_rz),
            'taxa_fechamento': taxa(fechados, propostas),
            'conversao_geral': taxa(fechados, leads),
        },
        'ciclo': {
            'mediana_dias': _num(ciclo.get('mediana_dias')),
            'media_dias': _num(ciclo.get('media_dias')),
        },
        'receita': {
            'total': _num(funil.get('receita')) or 0,
            'ticket_medio': _num(funil.get('ticket_medio')) or 0,
        },
        'por_campanha': [_row(r) for r in por_campanha],
        'diario': [{'dia': str(r['dia']), 'leads': int(r['leads']),
                    'fechados': int(r['fechados'])} for r in diario],
        'motivos_perda': [_row(r) for r in motivos_perda],
    }


def _num(v):
    if v is None:
        return None
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return None


def _row(r):
    """Converte uma linha do banco em JSON-serializável.

    Cuidado com a ordem dos testes: bool é subclasse de int, e UUID/Decimal
    precisam de tratamento próprio — um `isinstance(v, (int, str))` solto
    transformaria UUID em None.
    """
    import decimal
    import uuid as _uuid

    out = {}
    for k, v in dict(r).items():
        if v is None or isinstance(v, (bool, int, float, str)):
            out[k] = v
        elif hasattr(v, 'isoformat'):                 # datetime / date
            out[k] = v.isoformat()
        elif isinstance(v, _uuid.UUID):
            out[k] = str(v)
        elif isinstance(v, decimal.Decimal):
            out[k] = _num(v)
        elif isinstance(v, (dict, list)):             # jsonb
            out[k] = v
        else:
            out[k] = str(v)
    return out
