FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    bentoml==1.1.11 \
    mlflow==2.10.0 \
    pydantic==2.5.0 \
    numpy==1.26.2 \
    requests==2.31.0 \
    boto3==1.34.0 \
    prometheus-client==0.19.0

COPY services/ /app/services/

ENV MLFLOW_TRACKING_URI=http://mlflow:5000
ENV BENTOML_PORT=3001
ENV AWS_ACCESS_KEY_ID=minio_admin
ENV AWS_SECRET_ACCESS_KEY=minio_password
ENV MLFLOW_S3_ENDPOINT_URL=http://minio:9000

EXPOSE 3001

CMD ["bentoml", "serve", "services.recommendation_service:svc", "--host", "0.0.0.0", "--port", "3001"]
