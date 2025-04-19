FROM python:3.12-slim

WORKDIR /app

# 設置環境變量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    PATH="/root/.local/bin:$PATH"

# 安裝系統依賴
RUN apt-get update && \
    apt-get install -y curl libssl-dev pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 安裝 uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# 複製應用代碼
COPY . /app/

# 使用 uv 安裝 Python 依賴
RUN uv sync --all-extras

# 運行應用
CMD ["uv", "run", "run_live.py"]