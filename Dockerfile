# Use slim python image
FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY . .

# Expose port (for webhook / healthcheck)
EXPOSE 8000

# Start bot
CMD ["python", "bot.py"]