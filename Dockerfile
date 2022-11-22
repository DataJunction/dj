FROM python:3.10
WORKDIR /code
COPY . /code
RUN pip install --no-cache-dir --upgrade -r /code/requirements/docker.txt
RUN pip install -e .
RUN pip install https://github.com/rogerbinns/apsw/releases/download/3.38.1-r1/apsw-3.38.1-r1.zip \
  --global-option=fetch --global-option=--version --global-option=3.38.1 --global-option=--all \
  --global-option=build --global-option=--enable-all-extensions
CMD ["uvicorn", "djqs.api.main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
EXPOSE 8001
