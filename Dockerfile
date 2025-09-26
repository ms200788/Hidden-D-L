# Use Python base image
FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (better caching)
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy rest of the code
COPY . /app/

# Expose port for webhook
EXPOSE 10000

# Entrypoint and command
ENTRYPOINT ["python"]
CMD ["bot.py"]