"""Teste direto da API Meta para debugar criacao de creative."""
import json, requests, os
from dotenv import load_dotenv

load_dotenv('notepad.env')

with open('token.json') as f:
    ACCESS_TOKEN = json.load(f).get('access_token')

ACCOUNT_ID = 'act_446772417972343'
PAGE_ID = '169510206254300'

resp = requests.get(
    f'https://graph.facebook.com/v18.0/{ACCOUNT_ID}/adimages',
    params={'access_token': ACCESS_TOKEN, 'fields': 'hash,name', 'limit': 1}
).json()

if 'error' in resp:
    print(f"ERRO buscando imagens: {json.dumps(resp, indent=2, ensure_ascii=True)}")
    exit()

IMAGE_HASH = resp['data'][0]['hash']
print(f"Hash: {IMAGE_HASH}")

payload = {
    'object_story_spec': json.dumps({
        'page_id': PAGE_ID,
        'link_data': {
            'link': 'https://google.com',
            'message': 'Teste',
            'image_hash': IMAGE_HASH,
            'call_to_action': {
                'type': 'LEARN_MORE',
                'value': {'link': 'https://google.com'}
            }
        }
    }),
    'access_token': ACCESS_TOKEN,
}

resp = requests.post(
    f'https://graph.facebook.com/v18.0/{ACCOUNT_ID}/adcreatives',
    data=payload
)

with open('test_output.txt', 'w', encoding='utf-8') as f:
    f.write(f"Status: {resp.status_code}\n")
    f.write(json.dumps(resp.json(), indent=2, ensure_ascii=True))

print("Resultado salvo em test_output.txt")
