# ./services/analyzer/Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -U pip && pip install -r requirements.txt
COPY analyzer.py .
ENV ES_HOST=http://es:9200 ES_INDEX=games SUCCESS_RATING_THRESH=4.5 MIN_SAMPLES=100 BATCH_SIZE=500
CMD ["python","/app/analyzer.py"]
