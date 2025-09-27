# Dockerfile for Render deploy (aiogram 2.x, web service mode)
FROM python:3.11-slim

WORKDIR /app

# Upgrade pip first to avoid dependency resolver issues
RUN pip install --no-cache-dir --upgrade pip

# Install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy all source files
COPY . /app

# Create data dir for sqlite & backups
RUN mkdir -p /app/data

# Environment defaults (overridden by Render dashboard)
ENV DB_PATH=/app/data/database.sqlite3
ENV JOB_DB_PATH=/app/data/jobs.sqlite
ENV PORT=8080

# Expose the port for Render (Render injects $PORT)
EXPOSE 8080

# Start bot (with aiohttp web server inside bot.py)
CMD ["python", "bot.py"]