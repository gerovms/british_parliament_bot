FROM python:3.12-slim

WORKDIR /app

# Установка зависимостей для PostgreSQL и netcat
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости и ставим их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект
COPY . .

# Создаём папку для результатов
RUN mkdir -p /app/results

# Делаем скрипт wait-for-it исполняемым
RUN chmod +x /app/wait-for-it.sh

# CMD запускает wait-for-it, чтобы дождаться PostgreSQL
CMD ["./wait-for-it.sh", "db:5432", "--timeout=30", "--", "python", "run.py"]
