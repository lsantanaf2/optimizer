"""
Web Push do CRM — avisa de lead novo mesmo com a aba fechada.

Diferença para o alerta da página: quem recebe aqui é o Service Worker, que o
navegador acorda sozinho. Não depende da aba do CRM estar aberta, nem do
Chrome estar na frente. É o único caminho que funciona com tudo fechado.

Chaves (VAPID): a pública identifica o servidor para o navegador e pode ficar
no código — ela aparece no HTML de qualquer jeito. A PRIVADA é segredo e vem
só do ambiente; sem ela o push se desliga e registra no log, sem quebrar nada.

    VAPID_PRIVATE_KEY — segredo, no deploy.sh da VPS
    VAPID_SUBJECT     — mailto: de contato (exigido pelo protocolo)
"""

import json
import logging
import os
import threading

from modules.database import execute, fetch_all

logger = logging.getLogger(__name__)

# Pública: vai para o navegador, não é segredo.
VAPID_PUBLIC_KEY = os.environ.get(
    'VAPID_PUBLIC_KEY',
    'BKGRT5zguPWBFvHA6XzunMZ4pIbgSx4huxWrwCImO1z29s1YX3gpiA92LS3m9ADDxsNdyGXE-8TkWeCJ0Ty9pug')
VAPID_SUBJECT = os.environ.get('VAPID_SUBJECT', 'mailto:ads.lsantanaf@gmail.com')

# Timeout obrigatório: isto roda em thread, e thread pendurada em socket morto
# é vazamento. Regra da casa.
TIMEOUT = 10


def _private_key():
    return (os.environ.get('VAPID_PRIVATE_KEY') or '').strip()


def habilitado():
    return bool(_private_key())


def registrar(endpoint, p256dh, auth, user_id=None, user_agent=None):
    """Guarda (ou atualiza) a inscrição deste navegador."""
    execute(
        """
        INSERT INTO crm_push_subs (endpoint, p256dh, auth, user_id, user_agent)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (endpoint) DO UPDATE SET
            p256dh = EXCLUDED.p256dh,
            auth   = EXCLUDED.auth,
            -- Reassinar zera o contador: a inscrição voltou a valer.
            falhas = 0
        """,
        (endpoint, p256dh, auth, user_id, (user_agent or '')[:300]),
    )


def remover(endpoint):
    return execute("DELETE FROM crm_push_subs WHERE endpoint = %s", (endpoint,))


def _enviar_para(sub, payload):
    """Envia para UMA inscrição. Devolve True se foi aceita."""
    from pywebpush import webpush, WebPushException
    try:
        webpush(
            subscription_info={
                'endpoint': sub['endpoint'],
                'keys': {'p256dh': sub['p256dh'], 'auth': sub['auth']},
            },
            data=json.dumps(payload, ensure_ascii=False),
            vapid_private_key=_private_key(),
            vapid_claims={'sub': VAPID_SUBJECT},
            timeout=TIMEOUT,
        )
        return True
    except WebPushException as e:
        status = getattr(e.response, 'status_code', None)
        # 404/410 = o navegador descartou a inscrição (desinstalou, limpou
        # dados, revogou). Não adianta tentar de novo: some com ela, senão
        # cada lead novo gasta uma requisição à toa para sempre.
        if status in (404, 410):
            remover(sub['endpoint'])
            logger.info("Push: inscrição morta removida (HTTP %s)", status)
        else:
            execute("UPDATE crm_push_subs SET falhas = falhas + 1 WHERE endpoint = %s",
                    (sub['endpoint'],))
            logger.warning("Push: falhou (HTTP %s): %s", status, str(e)[:200])
        return False
    except Exception:
        logger.exception("Push: erro inesperado no envio")
        return False


def avisar_lead(*, nome, whatsapp, origem, lead_id, recadastro=False):
    """Empurra o aviso de lead novo para todos os navegadores inscritos.

    Roda em thread: o lead JÁ está salvo quando isto começa. Se o push falhar,
    perde-se o aviso, nunca o lead.
    """
    if not habilitado():
        logger.info("Push: desabilitado (VAPID_PRIVATE_KEY ausente)")
        return False

    payload = {
        'titulo': '🔁 Lead voltou!' if recadastro else '🎯 LEAD NOVO!',
        'corpo': f"{nome or 'Sem nome'}\n{whatsapp or ''}\n{origem or 'direto'}",
        'lead_id': str(lead_id),
        'url': '/crm',
    }

    def _worker():
        try:
            subs = fetch_all('SELECT endpoint, p256dh, auth FROM crm_push_subs')
        except Exception:
            logger.exception("Push: não consegui ler as inscrições")
            return
        if not subs:
            logger.info("Push: nenhum navegador inscrito")
            return
        ok = sum(1 for s in subs if _enviar_para(s, payload))
        logger.info("Push: lead %s enviado para %s de %s inscrição(ões)",
                    lead_id, ok, len(subs))
        if ok:
            execute('UPDATE crm_push_subs SET ultimo_ok_em = NOW() WHERE falhas = 0')

    threading.Thread(target=_worker, name=f'push-{lead_id}', daemon=True).start()
    return True
