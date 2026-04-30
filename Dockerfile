FROM python:3.11-slim

LABEL maintainer="qijian-music-system"
LABEL description="七剑音乐生成系统 - Python 后处理 Worker（qijian-worker）"

# 安装 ffmpeg（moviepy 依赖）
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY audio_processor.py .

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python3 -c "import redis, os; r=redis.Redis(host=os.getenv('REDIS_HOST','localhost'), password=os.getenv('REDIS_PASSWORD','') or None); r.ping()" || exit 1

CMD ["python3", "-u", "audio_processor.py"]
