FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN addgroup --system celery && \
    adduser --system --ingroup celery --home /code celery

WORKDIR /code

COPY . .

RUN uv sync --no-dev

RUN chown -R celery:celery /code
USER celery

CMD ["uv", "run", "celery", "--app", "datajunction_reflection.celery_app", "beat", "--loglevel=info"]
