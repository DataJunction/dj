FROM python:3.10
WORKDIR /code
COPY . /code
RUN pip install --no-cache-dir --upgrade -r /code/requirements/docker.txt
RUN pip install -e .
RUN pip install opentelemetry-distro==0.38b0 opentelemetry-exporter-otlp-proto-grpc==1.17.0

CMD ["opentelemetry-instrument", "uvicorn", "dj.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
EXPOSE 8000
