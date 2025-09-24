# Dockerfile for Render deploy (aiogram 2.x, webhook mode)
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy bot source
COPY bot.py /app/bot.py

# Create data dir for sqlite & backups
RUN mkdir -p /data

# Environment defaults (can be overridden in Render)
ENV DB_PATH=/data/database.sqlite3
ENV JOB_DB_PATH=/data/jobs.sqlite
ENV PORT=10000

CMD ["python", "bot.py"]
