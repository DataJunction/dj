FROM jupyter/pyspark-notebook
USER root
WORKDIR /code
COPY . /code
RUN pip install --no-cache-dir --upgrade -r /code/requirements/docker.txt
RUN pip install -e .
CMD ["uvicorn", "djqs.api.main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
EXPOSE 8001
