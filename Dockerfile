FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts
COPY samples ./samples

# 容器启动前确保内置样例就绪（仓库已带样例，此步为幂等保险）
RUN python -I scripts/generate_samples.py

EXPOSE 8000

# 超时放宽到 10 分钟，给大检查点解析留足时间
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--timeout", "600", "app.web:app"]
