FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /code
COPY . /code

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN uv sync --extra uvicorn --no-dev

CMD ["uv", "run", "uvicorn", "djqs.api.main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
EXPOSE 8001
