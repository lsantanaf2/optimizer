# Usar imagem leve do Python
FROM python:3.11-slim

# Definir diretório de trabalho
WORKDIR /app

# Instalar dependências de sistema (ffmpeg para extração de thumbnails de vídeo)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements primeiro para cache de camadas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o resto do código
COPY . .

# Expor a porta 5000
EXPOSE 5000

# v2.11.2: PYTHONUNBUFFERED=1 faz os print() do app aparecerem no `docker logs`
# em tempo real (antes o stdout ficava bufferizado e a gente ficava cego no
# servidor quando o upload travava).
ENV PYTHONUNBUFFERED=1

# Comando para rodar com Gunicorn (Produção)
# v2.11.2: worker 'gthread' (era 'sync'). Com sync, cada upload/SSE monopolizava
# um worker inteiro por minutos — 4 uploads grandes esgotavam os 4 workers e o
# próximo request (staging do item seguinte) ficava na fila sem timeout, travando
# a fila no item 2-3. gthread com threads atende vários requests por worker, então
# um upload longo NÃO bloqueia os demais. timeout 600s cobre VPS→Meta de vídeos
# grandes + processamento + criação do creative.
CMD ["gunicorn", "--bind", "0.0.0.0:5000", \
     "--workers", "3", "--threads", "4", "--worker-class", "gthread", \
     "--timeout", "600", "--graceful-timeout", "30", \
     "--access-logfile", "-", "app:app"]
