FROM python:3.10

ARG RELOAD="--reload"
ENV RELOAD ${RELOAD}

WORKDIR /code
COPY . /code
RUN pip install --no-cache-dir --upgrade -r /code/requirements/docker.txt
RUN pip install -e .

CMD ["sh", "-c", "opentelemetry-instrument uvicorn datajunction_server.api.main:app --host 0.0.0.0 --port 8000 $RELOAD"]
EXPOSE 8000
