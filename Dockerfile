FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# системные зависимости для сборки колёс и SSL
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates tzdata \
    libffi-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# сначала зависимости, чтобы кэшировалось
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# затем код бота
COPY . .

# если файл называется 5bot.py — оставь так
CMD ["python", "5bot.py"]

