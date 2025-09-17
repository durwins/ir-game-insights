FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# کد
COPY . .
# پایتون‌پث: هم ریشه‌ی اپ، هم پوشهٔ سرویس
ENV PYTHONPATH="/app:/app/services/scraper"
CMD ["python", "/app/crawler.py"]
