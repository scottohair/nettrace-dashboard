FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    traceroute iputils-ping dnsutils gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Compile C engines for nanosecond-level performance
RUN cc -O3 -march=native -shared -fPIC -o agents/hft_core.so agents/hft_core.c -lm && \
    cc -O3 -march=native -shared -fPIC -o agents/fast_engine.so agents/fast_engine.c -lm && \
    echo "C engines compiled successfully"

EXPOSE 8080

CMD ["gunicorn", "--worker-class", "geventwebsocket.gunicorn.workers.GeventWebSocketWorker", \
     "--bind", "0.0.0.0:8080", "--workers", "1", "--timeout", "120", "app:app"]
