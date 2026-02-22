# Usar imagem leve do Python
FROM python:3.11-slim

# Definir diretório de trabalho
WORKDIR /app

# Instalar dependências de sistema (ffmpeg para extração de thumbnails de vídeo)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements primeiro para cache de camadas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o resto do código
COPY . .

# Expor a porta 5000
EXPOSE 5000

# Comando para rodar com Gunicorn (Produção)
# Usamos gthread para lidar melhor com conexões de streaming (pode demorar)
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--worker-class", "gthread", "--threads", "4", "--timeout", "600", "app:app"]


