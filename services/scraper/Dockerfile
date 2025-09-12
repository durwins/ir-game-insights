# ./services/scraper/Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY crawler.py ./crawler.py
# فایل URLها را با volume میگیریم
CMD ["python","/app/crawler.py"]
