FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    traceroute iputils-ping dnsutils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["gunicorn", "--worker-class", "geventwebsocket.gunicorn.workers.GeventWebSocketWorker", \
     "--bind", "0.0.0.0:8080", "--workers", "1", "--timeout", "120", "app:app"]
