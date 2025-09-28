# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# system deps needed for some packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential gcc libssl-dev libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements & install
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy app code
COPY bot.py /app/bot.py

# runtime
ENV PYTHONUNBUFFERED=1
EXPOSE 8080

# Use Gunicorn to serve the Flask app; Render will set $PORT
CMD ["gunicorn", "-w", "1", "-k", "gthread", "--threads", "2", "--bind", "0.0.0.0:$PORT", "bot:app"]
