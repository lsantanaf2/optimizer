import sys
import copy
sys.path.insert(0, r"d:\Clientes\SK MKT\OPTIMIZER")
from modules.cruzamento import processar_cruzamento

fb_ads = [
    {'ad_name': 'teste_ad', 'campaign_name': 'camp1', 'adset_name': 'set1', 'spend': 100.0, 'impressions': 1000, 'clicks': 10, 'date_start': '2023-01-01', 'ad_status': 'ACTIVE'}
]
mqls_rows = [
    {'Deal ID': '1', 'Produto indicado': 'Produto A', 'utm_term': 'teste_ad', 'Data do preenchimento': '2023-01-01'},
    {'Deal ID': '2', 'Produto indicado': 'Produto B', 'utm_term': 'null', 'Data do preenchimento': '2023-01-01'}
]
wons_rows = []

try:
    res = processar_cruzamento(fb_ads, mqls_rows, wons_rows)
    print("SUCCESS")
except Exception as e:
    import traceback
    traceback.print_exc()
