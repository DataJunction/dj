FROM jupyter/pyspark-notebook
USER root
WORKDIR /code
COPY . /code
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN pip install -e .[uvicorn]
CMD ["uvicorn", "djqs.api.main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
EXPOSE 8001
