"""
Instagram — posts impulsionados, orgânico e crescimento do perfil.

Três fontes, com degradação graciosa quando falta permissão:

1. POSTS IMPULSIONADOS (Ads API — funciona com os escopos atuais)
   Gasto, seguidores ganhos, impressões, cliques e custo por seguidor de cada
   post impulsionado. Identificados pelo padrão do nome da campanha.

2. CRESCIMENTO DO PERFIL (IG Insights — exige instagram_manage_insights)
   follower_count por dia. A Meta só devolve os últimos ~30 dias e NÃO separa
   orgânico de pago — o quanto veio de anúncio sai do cruzamento com (1).

3. ORGÂNICO DOS POSTS (IG Media Insights — exige instagram_manage_insights)
   Alcance, curtidas, comentários, salvamentos e compartilhamentos do post.
   Diferente do `post_engagement` da Ads API, que mede só o que o ANÚNCIO gerou.

Sem o escopo, (2) e (3) retornam vazio com um aviso — a aba continua útil
mostrando (1), em vez de quebrar.
"""

import logging
from datetime import datetime, date, timedelta

from modules.meta_client import GRAPH_BASE, META_TAX_RATE, meta_get, meta_get_paginated

logger = logging.getLogger(__name__)

# Campanhas de impulsionamento nascem com este prefixo no nome
BOOST_PATTERNS = ('post do instagram:', 'publicação do instagram:')

PERM_ERROR_HINTS = ('permission', 'insufficient', 'oauth', '#10', '#200', '#100')


def _is_boost(nome):
    n = (nome or '').lower()
    # "DIST: Post do Instagram: ..." também conta
    return any(p in n for p in BOOST_PATTERNS)


def _num(v):
    """'R$1.234,56 BRL' | '1427' | None → float."""
    if v is None or v == '':
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v)
    for t in ('R$', 'BRL', '\xa0', ' ', '%'):
        s = s.replace(t, '')
    s = s.strip()
    if ',' in s and '.' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0


def _act(actions, *tipos_por_prioridade):
    """Extrai action_type por prioridade (nunca soma tipos que se sobrepõem)."""
    mapa = {}
    for a in (actions or []):
        t = a.get('action_type')
        if t:
            mapa[t] = mapa.get(t, 0) + int(float(a.get('value', 0) or 0))
    for t in tipos_por_prioridade:
        if mapa.get(t):
            return mapa[t]
    return 0


# ── 1. Posts impulsionados (Ads API) ─────────────────────────────────────────

def fetch_boosted_posts(account_id, token, since, until):
    """Insights por campanha, filtrando só os impulsionamentos de post.

    Retorna lista ordenada por seguidores ganhos (desc), cada item com
    gasto bruto e real, seguidores, impressões, cliques e custo por seguidor.
    """
    import json
    acct = account_id if str(account_id).startswith('act_') else f'act_{account_id}'
    params = {
        'access_token': token,
        'level': 'campaign',
        'fields': ('campaign_id,campaign_name,spend,impressions,reach,'
                   'inline_link_clicks,clicks,actions'),
        'limit': 500,
        'time_range': json.dumps({'since': since, 'until': until}, separators=(',', ':')),
    }
    rows = meta_get_paginated(f'{GRAPH_BASE}/{acct}/insights', params, timeout=60)

    posts = []
    for r in rows:
        nome = r.get('campaign_name', '')
        if not _is_boost(nome):
            continue
        actions = r.get('actions') or []
        gasto = _num(r.get('spend'))
        # Seguidores ganhos: a Meta usa vários rótulos conforme o objetivo
        seguidores = _act(actions,
                          'instagram_profile_follow_v2',
                          'onsite_conversion.follow',
                          'follow')
        imp = int(_num(r.get('impressions')))
        cliques = int(_num(r.get('inline_link_clicks'))) or int(_num(r.get('clicks')))
        custo_real = round(gasto * (1 + META_TAX_RATE), 2)
        posts.append({
            'campaign_id':   r.get('campaign_id', ''),
            'nome':          nome,
            # Título limpo, sem o prefixo "Post do Instagram:"
            'titulo':        _titulo_limpo(nome),
            'gasto':         round(gasto, 2),
            'custo_real':    custo_real,
            'seguidores':    seguidores,
            'impressoes':    imp,
            'alcance':       int(_num(r.get('reach'))),
            'cliques':       cliques,
            'engajamento':   _act(actions, 'post_engagement', 'page_engagement'),
            # Custo por seguidor sobre o CUSTO REAL (padrão da dash)
            'custo_seguidor': round(custo_real / seguidores, 2) if seguidores else None,
            'cpm':           round(custo_real / imp * 1000, 2) if imp else None,
            'ctr':           round(cliques / imp * 100, 2) if imp else None,
        })
    posts.sort(key=lambda p: (p['seguidores'] or 0), reverse=True)
    return posts


def _titulo_limpo(nome):
    n = nome or ''
    for sep in ('Post do Instagram:', 'Publicação do Instagram:'):
        if sep.lower() in n.lower():
            i = n.lower().find(sep.lower())
            n = n[i + len(sep):]
            break
    return n.strip().strip('.').strip() or nome


def totais_boosted(posts):
    """Consolidado dos impulsionamentos, com custo/seguidor sobre os totais."""
    t = {'gasto': 0.0, 'custo_real': 0.0, 'seguidores': 0,
         'impressoes': 0, 'cliques': 0, 'engajamento': 0, 'posts': len(posts)}
    for p in posts:
        for k in ('gasto', 'custo_real', 'seguidores', 'impressoes', 'cliques', 'engajamento'):
            t[k] += p.get(k, 0) or 0
    t['gasto'] = round(t['gasto'], 2)
    t['custo_real'] = round(t['custo_real'], 2)
    t['custo_seguidor'] = round(t['custo_real'] / t['seguidores'], 2) if t['seguidores'] else None
    t['cpm'] = round(t['custo_real'] / t['impressoes'] * 1000, 2) if t['impressoes'] else None
    t['ctr'] = round(t['cliques'] / t['impressoes'] * 100, 2) if t['impressoes'] else None
    return t


# ── 2. Conta do Instagram ligada à conta de anúncios ─────────────────────────

def fetch_ig_account(account_id, token):
    """Descobre o IG vinculado à conta de anúncios. None se não houver."""
    acct = account_id if str(account_id).startswith('act_') else f'act_{account_id}'
    for edge in ('instagram_accounts', 'connected_instagram_accounts'):
        try:
            resp = meta_get(f'{GRAPH_BASE}/{acct}/{edge}',
                            {'fields': 'id,username', 'access_token': token, 'limit': 25},
                            timeout=30)
            data = resp.get('data') or []
            if data:
                return {'id': data[0].get('id'), 'username': data[0].get('username', '')}
        except Exception as e:
            logger.warning(f'[instagram] {edge} falhou: {e}')
    return None


# ── 3. Crescimento do perfil (IG Insights) ───────────────────────────────────

def fetch_follower_growth(ig_id, token, since, until):
    """follower_count por dia. Retorna (serie, erro).

    A API só devolve os últimos ~30 dias e no máximo 30 dias por chamada.
    Sem o escopo instagram_manage_insights, retorna ([], mensagem).
    """
    if not ig_id:
        return [], 'Conta do Instagram não encontrada na conta de anúncios.'

    # A janela é limitada: no máximo 30 dias e nada anterior a isso
    hoje = date.today()
    d_since = max(datetime.strptime(since, '%Y-%m-%d').date(), hoje - timedelta(days=29))
    d_until = min(datetime.strptime(until, '%Y-%m-%d').date(), hoje)
    if d_since > d_until:
        return [], None

    try:
        resp = meta_get(f'{GRAPH_BASE}/{ig_id}/insights', {
            'metric': 'follower_count',
            'period': 'day',
            'since': d_since.isoformat(),
            'until': d_until.isoformat(),
            'access_token': token,
        }, timeout=30)
    except Exception as e:
        msg = str(e)
        if any(h in msg.lower() for h in PERM_ERROR_HINTS):
            return [], ('Sem permissão para ler métricas do Instagram. '
                        'Reconecte o Facebook autorizando "instagram_manage_insights".')
        return [], f'Instagram Insights indisponível: {msg[:160]}'

    serie = []
    for m in (resp.get('data') or []):
        if m.get('name') != 'follower_count':
            continue
        for v in (m.get('values') or []):
            dt = (v.get('end_time') or '')[:10]
            if dt:
                serie.append({'date': dt, 'novos_seguidores': int(v.get('value') or 0)})
    serie.sort(key=lambda x: x['date'])
    return serie, None


# ── 4. Orgânico dos posts impulsionados (IG Media Insights) ──────────────────

def fetch_organic_posts(ig_id, token, limit=12):
    """Métricas orgânicas das últimas publicações do perfil.

    Retorna (posts, erro). Cada post traz alcance, curtidas, comentários,
    salvamentos e compartilhamentos — o que o post fez ALÉM do impulsionamento.
    """
    if not ig_id:
        return [], 'Conta do Instagram não encontrada.'

    try:
        media = meta_get(f'{GRAPH_BASE}/{ig_id}/media', {
            'fields': 'id,caption,media_type,permalink,timestamp,like_count,comments_count',
            'limit': limit,
            'access_token': token,
        }, timeout=30)
    except Exception as e:
        msg = str(e)
        if any(h in msg.lower() for h in PERM_ERROR_HINTS):
            return [], ('Sem permissão para ler publicações do Instagram. '
                        'Reconecte o Facebook autorizando "instagram_manage_insights".')
        return [], f'Publicações indisponíveis: {msg[:160]}'

    posts = []
    for m in (media.get('data') or [])[:limit]:
        mid = m.get('id')
        item = {
            'id': mid,
            'legenda': (m.get('caption') or '')[:90],
            'tipo': m.get('media_type', ''),
            'link': m.get('permalink', ''),
            'data': (m.get('timestamp') or '')[:10],
            'curtidas': m.get('like_count'),
            'comentarios': m.get('comments_count'),
            'alcance': None, 'salvamentos': None, 'compartilhamentos': None,
        }
        # Insights por mídia (falha silenciosa: o post ainda aparece com o básico)
        try:
            ins = meta_get(f'{GRAPH_BASE}/{mid}/insights', {
                'metric': 'reach,saved,shares',
                'access_token': token,
            }, timeout=20)
            for met in (ins.get('data') or []):
                nome = met.get('name')
                vals = met.get('values') or [{}]
                val = vals[0].get('value')
                if nome == 'reach':
                    item['alcance'] = val
                elif nome == 'saved':
                    item['salvamentos'] = val
                elif nome == 'shares':
                    item['compartilhamentos'] = val
        except Exception as e:
            logger.debug(f'[instagram] insights da midia {mid}: {e}')
        posts.append(item)
    return posts, None
