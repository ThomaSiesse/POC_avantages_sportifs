FROM python:3.12-slim
RUN pip install --no-cache-dir boto3
COPY init.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "init.py"]