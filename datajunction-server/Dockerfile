FROM python:3.10

ARG RELOAD="--reload"
ENV RELOAD ${RELOAD}

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /code
COPY . /code

# Install dependencies using uv (faster than pip)
RUN uv sync --extra uvicorn --extra transpilation --no-dev

CMD ["sh", "-c", "uv run opentelemetry-instrument uvicorn datajunction_server.api.main:app --host 0.0.0.0 --port 8000 $RELOAD"]
EXPOSE 8000
