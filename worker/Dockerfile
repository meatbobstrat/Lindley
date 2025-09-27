FROM python:3.11-slim

# Install system deps
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    imagemagick \
    ghostscript \
    qpdf \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install latest ocrmypdf
RUN pip install --no-cache-dir ocrmypdf

# Copy worker script
COPY worker/worker.py .

# 🚨 Force unbuffered logging
ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "worker.py"]
