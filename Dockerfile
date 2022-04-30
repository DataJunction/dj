FROM python:3.10
WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
RUN pip install uvicorn pydruid 'shillelagh[gsheetsapi]' psycopg2-binary
COPY . /code
RUN pip install -e .
CMD ["alembic", "upgrade", "head"]
CMD ["dj", "compile"]
CMD ["uvicorn", "datajunction.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
EXPOSE 8000
