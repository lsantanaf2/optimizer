"""
Conversions API (CAPI) — envia o evento Lead para a Meta pelo servidor.

Por que existir, se o Pixel do browser já manda o mesmo evento: bloqueador de
anúncio, ITP do Safari e aba fechada antes do script rodar derrubam uma fatia
dos eventos do browser. O envio pelo servidor não depende de nada disso.

Os dois caminhos mandam o MESMO `event_id`, então a Meta deduplica e conta uma
conversão só. Sem isso, cada lead viraria dois e o CPL no gerenciador cairia
pela metade — métrica errada é pior que métrica faltando.

Configuração (nenhuma vai para o repositório além do pixel, que é público):
    META_PIXEL_ID    — id do pixel (aparece no HTML da página de qualquer jeito)
    META_CAPI_TOKEN  — token de acesso. SEGREDO: só no deploy.sh da VPS.
    META_TEST_EVENT_CODE — opcional, para ver o evento em "Eventos de teste".
"""

import hashlib
import logging
import os
import threading
import time

import requests

from modules.meta_client import GRAPH_BASE

logger = logging.getLogger(__name__)

PIXEL_ID = os.environ.get('META_PIXEL_ID', '6693749304086298')
TEST_EVENT_CODE = os.environ.get('META_TEST_EVENT_CODE', '').strip()

# Timeout curto: isso roda numa thread à parte, mas uma thread pendurada num
# socket morto é vazamento. Regra da casa: nenhum requests sem timeout.
TIMEOUT = (5, 10)


def _token():
    return (os.environ.get('META_CAPI_TOKEN') or '').strip()


def habilitado():
    return bool(_token() and PIXEL_ID)


def _hash(valor):
    """SHA-256 do valor normalizado. A Meta exige os dados pessoais hasheados."""
    if not valor:
        return None
    return hashlib.sha256(str(valor).strip().lower().encode('utf-8')).hexdigest()


def _hash_telefone(e164):
    """Telefone hasheado: só dígitos, com DDI, sem '+' — formato que a Meta espera."""
    if not e164:
        return None
    digitos = ''.join(c for c in str(e164) if c.isdigit())
    if not digitos:
        return None
    return hashlib.sha256(digitos.encode('utf-8')).hexdigest()


def _primeiro_e_ultimo_nome(nome):
    partes = [p for p in (nome or '').strip().split() if p]
    if not partes:
        return None, None
    return _hash(partes[0]), (_hash(partes[-1]) if len(partes) > 1 else None)


def enviar_lead(*, event_id, email=None, telefone_e164=None, nome=None,
                fbp=None, fbc=None, ip=None, user_agent=None,
                event_source_url=None, event_time=None, custom=None):
    """Dispara o Lead em uma thread. Nunca levanta e nunca bloqueia a captura.

    O lead JÁ ESTÁ salvo no banco quando isto roda — se a Meta estiver fora do
    ar, perde-se a conversão no gerenciador, nunca o lead.
    """
    if not habilitado():
        logger.info("CAPI: desabilitada (META_CAPI_TOKEN ausente) — evento %s não enviado", event_id)
        return False

    nome_h, sobrenome_h = _primeiro_e_ultimo_nome(nome)
    user_data = {
        'em': [_hash(email)] if email else None,
        'ph': [_hash_telefone(telefone_e164)] if telefone_e164 else None,
        'fn': [nome_h] if nome_h else None,
        'ln': [sobrenome_h] if sobrenome_h else None,
        'fbp': fbp or None,
        'fbc': fbc or None,
        'client_ip_address': ip or None,
        'client_user_agent': user_agent or None,
    }
    user_data = {k: v for k, v in user_data.items() if v}

    evento = {
        'event_name': 'Lead',
        'event_time': int(event_time or time.time()),
        'event_id': str(event_id),          # mesma chave do Pixel → deduplicação
        'action_source': 'website',
        'user_data': user_data,
    }
    if event_source_url:
        evento['event_source_url'] = event_source_url[:1000]
    if custom:
        evento['custom_data'] = custom

    def _worker():
        payload = {'data': [evento], 'access_token': _token()}
        if TEST_EVENT_CODE:
            payload['test_event_code'] = TEST_EVENT_CODE
        url = f'{GRAPH_BASE}/{PIXEL_ID}/events'
        try:
            resp = requests.post(url, json=payload, timeout=TIMEOUT)
            if resp.status_code >= 400:
                # Nunca logar o payload inteiro: ele carrega os hashes e, no
                # corpo da requisição, o token.
                logger.error("CAPI: Meta recusou o evento %s — HTTP %s: %s",
                             event_id, resp.status_code, resp.text[:300])
                return
            corpo = resp.json()
            logger.info("CAPI: Lead %s aceito (events_received=%s)",
                        event_id, corpo.get('events_received'))
        except requests.Timeout:
            logger.warning("CAPI: timeout ao enviar o evento %s", event_id)
        except Exception:
            logger.exception("CAPI: falha ao enviar o evento %s", event_id)

    threading.Thread(target=_worker, name=f'capi-{event_id}', daemon=True).start()
    return True
