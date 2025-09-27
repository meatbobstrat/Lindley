FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY watcher/watcher.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "watcher.py"]
