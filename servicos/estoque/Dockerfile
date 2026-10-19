FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml ./
COPY estoque ./estoque
RUN pip install --no-cache-dir . && useradd --create-home app
USER app
EXPOSE 8000
CMD ["python", "-m", "estoque"]
