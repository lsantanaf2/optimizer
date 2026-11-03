"""
Módulo de controle da Visitor API própria.

A Visitor API própria é um Cloudflare Worker que devolve geolocalização e
parsing de User-Agent, substituindo a visitorapi.com (US$ 7/mês). O Worker fica
numa URL pública, então qualquer página do mundo pode chamá-lo: este módulo
existe para responder "quem está usando e quanto", que é o que decide quando
trancar o `ALLOWED_ORIGINS` e quando o teto de 100 mil requisições/dia aperta.

O Worker reporta cada hit aqui DEPOIS de já ter respondido ao visitante
(`ctx.waitUntil`), então a VPS nunca entra no caminho crítico da página do
cliente. Se a VPS estiver fora, perde-se a métrica, nunca o serviço.
"""

import os
import logging
from urllib.parse import urlparse

from flask import Blueprint, jsonify, render_template, request

from modules.auth import login_required
from modules.database import execute, fetch_all

logger = logging.getLogger(__name__)

visitor_api_bp = Blueprint('visitor_api', __name__)

# Teto do plano grátis da Cloudflare Workers.
TETO_DIARIO = 100_000

# Chamada sem header Origin (curl, monitoração, script copiado para um .html
# local). Entra com rótulo próprio em vez de ser descartada: esse tráfego conta
# para o teto igual, e sumir com ele falsearia a margem.
SEM_ORIGIN = '(sem origin)'


def _db_ok():
    """Verifica se o pool de banco está disponível."""
    try:
        from modules.database import _pool
        return _pool is not None
    except Exception:
        return False


def _dominio(origin):
    """Extrai o hostname de um Origin, em minúsculo e sem porta.

    'https://www.Cliente.com.br:443' -> 'www.cliente.com.br'

    Guarda o hostname e não o Origin inteiro porque o mesmo site chega como
    http e https ao longo de uma migração de certificado, e contar os dois
    separados esconderia o volume real do cliente.
    """
    if not origin:
        return SEM_ORIGIN
    origin = origin.strip()
    try:
        host = urlparse(origin).hostname if '//' in origin else origin.split('/')[0]
    except Exception:
        host = None
    if not host:
        return SEM_ORIGIN
    return host.lower()[:253]  # limite de um hostname válido


# ─────────────────────────────────────────────────────────────
# Ingestão (chamada pelo Worker)
# ─────────────────────────────────────────────────────────────

@visitor_api_bp.route('/api/visitor/hit', methods=['POST'])
def registrar_hit():
    """Soma 1 ao contador de (domínio, dia). Chamado pelo Worker.

    Protegido por segredo compartilhado: o endpoint é público e, sem ele,
    qualquer um inflaria a métrica de um cliente com um laço de curl.

    Responde 204 mesmo quando não grava. Quem chama é um `waitUntil` do Worker,
    que não lê o corpo nem trata erro — devolver detalhe de falha aqui só
    entregaria informação para quem estivesse sondando o endpoint.
    """
    segredo = os.environ.get('VISITOR_API_SECRET')
    if not segredo or request.headers.get('X-Visitor-Secret') != segredo:
        return '', 204

    if not _db_ok():
        return '', 204

    payload = request.get_json(silent=True) or {}
    dominio = _dominio(payload.get('origin') or request.headers.get('Origin'))

    try:
        # O dia vem do NOW() do Postgres convertido para Brasília, nunca do
        # relógio do container: ele corre ~100s atrasado e, perto da virada,
        # jogaria hits do começo do dia para a véspera.
        execute(
            """
            INSERT INTO visitor_api_hits (dominio, dia, hits)
            VALUES (%s, (NOW() AT TIME ZONE 'America/Sao_Paulo')::date, 1)
            ON CONFLICT (dominio, dia) DO UPDATE
               SET hits = visitor_api_hits.hits + 1,
                   visto_em = NOW()
            """,
            (dominio,)
        )
    except Exception as e:
        logger.warning(f"visitor_api: falha ao registrar hit de {dominio}: {e}")

    return '', 204


# ─────────────────────────────────────────────────────────────
# Painel
# ─────────────────────────────────────────────────────────────

@visitor_api_bp.route('/visitor')
@login_required
def visitor_page():
    return render_template('visitor_api.html', teto_diario=TETO_DIARIO)


@visitor_api_bp.route('/api/visitor/stats')
@login_required
def stats():
    """Volume por domínio e por dia nos últimos N dias."""
    if not _db_ok():
        return jsonify({'erro': 'banco indisponível'}), 503

    try:
        dias = max(1, min(int(request.args.get('dias', 30)), 365))
    except (TypeError, ValueError):
        dias = 30

    # A janela também sai do NOW() do banco — comparar com datetime.now() do
    # app já produziu métrica zerada aqui antes.
    por_dia = fetch_all(
        """
        SELECT dia, dominio, hits
          FROM visitor_api_hits
         WHERE dia > (NOW() AT TIME ZONE 'America/Sao_Paulo')::date - %s::int
         ORDER BY dia DESC, hits DESC
        """,
        (dias,)
    ) or []

    por_dominio = fetch_all(
        """
        SELECT dominio,
               SUM(hits)      AS total,
               MAX(dia)       AS ultimo_dia,
               COUNT(*)       AS dias_ativos
          FROM visitor_api_hits
         WHERE dia > (NOW() AT TIME ZONE 'America/Sao_Paulo')::date - %s::int
         GROUP BY dominio
         ORDER BY total DESC
        """,
        (dias,)
    ) or []

    hoje = fetch_all(
        """
        SELECT COALESCE(SUM(hits), 0) AS total
          FROM visitor_api_hits
         WHERE dia = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
        """
    ) or [{'total': 0}]

    def _linha(r):
        d = dict(r)
        if d.get('dia'):
            d['dia'] = d['dia'].isoformat()
        if d.get('ultimo_dia'):
            d['ultimo_dia'] = d['ultimo_dia'].isoformat()
        for k in ('hits', 'total', 'dias_ativos'):
            if k in d and d[k] is not None:
                d[k] = int(d[k])
        return d

    total_hoje = int(_linha(hoje[0]).get('total') or 0)

    return jsonify({
        'dias': dias,
        'teto_diario': TETO_DIARIO,
        'total_hoje': total_hoje,
        'pct_teto': round(total_hoje * 100.0 / TETO_DIARIO, 2),
        'por_dominio': [_linha(r) for r in por_dominio],
        'por_dia': [_linha(r) for r in por_dia],
    })
