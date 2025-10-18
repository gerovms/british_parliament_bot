FROM python:3.12-slim

WORKDIR /app

# Установка зависимостей для работы с PostgreSQL
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/results

CMD ["python", "run.py"]