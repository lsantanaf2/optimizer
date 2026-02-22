import requests
import time
import sys
import subprocess
import json

def test_requests():
    print("--- Teste via Requests ---")
    url = "https://graph.facebook.com/v22.0/act_446772417972343/adimages"
    start = time.time()
    try:
        # Teste sem arquivo primeiro (GET)
        resp = requests.get(url, timeout=30)
        print(f"GET Status: {resp.status_code}")
    except Exception as e:
        print(f"GET Erro: {e}")
    
    print(f"Tempo decorrido: {time.time() - start:.2f}s")

def test_curl():
    print("\n--- Teste via CURL ---")
    start = time.time()
    try:
        output = subprocess.check_output([
            "curl", "-v", "-I", "https://graph.facebook.com/v22.0/act_446772417972343/adimages"
        ], stderr=subprocess.STDOUT, text=True)
        print("CURL Conectado com sucesso!")
        for line in output.split('\n'):
            if "HTTP/" in line or "SSL" in line:
                print(line)
    except Exception as e:
        print(f"CURL Erro: {e}")
    print(f"Tempo decorrido: {time.time() - start:.2f}s")

if __name__ == "__main__":
    print("Iniciando Diagnóstico Profundo Squad AIOS...")
    test_curl()
    test_requests()
