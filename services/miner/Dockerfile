FROM python:3.11-slim
WORKDIR /app

# سیستم پکیج‌های لازم اختیاری (اگه لازم شد)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY miner.py feature_dict.yml ./
CMD ["python", "/app/miner.py"]
