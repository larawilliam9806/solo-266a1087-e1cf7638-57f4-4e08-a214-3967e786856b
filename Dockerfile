FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app_service ./app_service
COPY source_service ./source_service

# 采集端默认数据库位置（compose 中挂载持久卷）
RUN mkdir -p /data
ENV MUSEUM_DB_PATH=/data/museum.db

EXPOSE 8000
CMD ["uvicorn", "app_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
