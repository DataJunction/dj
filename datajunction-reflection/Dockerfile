FROM python:3.11

# Create a non-root user
RUN addgroup --system celery && adduser --system --ingroup celery celery

# Set user
USER celery

WORKDIR /code
COPY . /code

RUN pip install -e .
