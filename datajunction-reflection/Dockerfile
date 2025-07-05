FROM python:3.11-slim

RUN addgroup --system celery && \
    adduser --system --ingroup celery --home /code celery

WORKDIR /code

COPY . .

RUN pip install --no-cache-dir -e .

RUN chown -R celery:celery /code
USER celery

CMD ["celery", "--app", "datajunction_reflection.celery_app", "beat", "--loglevel=info"]
