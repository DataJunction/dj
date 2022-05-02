FROM python:3.10
WORKDIR /code
RUN pip install https://github.com/rogerbinns/apsw/releases/download/3.36.0-r1/apsw-3.36.0-r1.zip \
  --global-option=fetch --global-option=--version --global-option=3.36.0 --global-option=--all \
  --global-option=build --global-option=--enable-all-extensions
RUN pip install uvicorn pydruid 'shillelagh[gsheetsapi]' psycopg2-binary
COPY . /code
RUN pip install --no-cache-dir --upgrade -r /code/requirements/base.txt
RUN pip install -e .
CMD ["alembic", "upgrade", "head"]
CMD ["dj", "compile"]
CMD ["uvicorn", "datajunction.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
EXPOSE 8000
