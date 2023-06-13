FROM jupyter/pyspark-notebook
USER root
WORKDIR /code
COPY . /code
RUN pip install -e .[uvicorn]
CMD ["uvicorn", "djqs.api.main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
EXPOSE 8001
