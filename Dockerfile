FROM python:3.12-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim
RUN useradd -r -s /sbin/nologin -d /app marketuser
WORKDIR /app
COPY --from=builder /install /usr/local
COPY --chown=marketuser:marketuser collector/ ./collector/
COPY --chown=marketuser:marketuser data/ ./data/
COPY --chown=marketuser:marketuser main.py .
EXPOSE 8001
USER marketuser
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-u", "main.py"]
