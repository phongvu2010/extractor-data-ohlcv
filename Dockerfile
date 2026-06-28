# ==========================================
# STAGE 1: BUILDER
# ==========================================
FROM python:3.12-slim AS builder

# Ngăn Python tạo file cache và ép xuất log ra console ngay lập tức
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Tạo môi trường ảo (Virtual Environment)
ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv $VIRTUAL_ENV

ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Cài đặt Python Dependencies vào venv
COPY requirements.txt .

# Cập nhật pip và cài đặt các thư viện từ requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install -v --no-cache-dir -r requirements.txt

# ==========================================
# STAGE 2: RUNTIME (IMAGE CUỐI CÙNG)
# ==========================================
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Tạo tài khoản người dùng phi đặc quyền (non-root)
RUN useradd -m appuser
USER appuser
WORKDIR /home/appuser/app

# Copy toàn bộ môi trường ảo (đã có sẵn các pip packages) từ builder
COPY --from=builder --chown=appuser:appuser /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy toàn bộ mã nguồn vào container
COPY --chown=appuser:appuser . .

# Khai báo lệnh thực thi
ENTRYPOINT ["python", "main.py"]
