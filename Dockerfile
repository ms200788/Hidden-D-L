# Use an official Python image
FROM python:3.11-slim

# Prevents Python from writing .pyc files
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Expose port (needed for aiohttp webhook)
EXPOSE 8080

# Start the bot
CMD ["python", "bot.py"]