FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["sh", "-c", "python -m gunicorn bot:app --bind 0.0.0.0:${PORT:-8080} --worker-class gthread --workers 1 --threads 8 --timeout 20 --keep-alive 5 --access-logfile -"]
