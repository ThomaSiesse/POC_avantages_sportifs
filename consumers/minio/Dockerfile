FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Extensions DuckDB utilisées par dbt, installées une fois pour toutes dans l'image
RUN python -c "import duckdb; [duckdb.install_extension(e) for e in ('httpfs', 'delta', 'excel', 'postgres')]"

COPY consumer_minio.py .

CMD ["python", "consumer_minio.py"]