FROM python:3.9-slim

COPY ./requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt

EXPOSE 8501

WORKDIR /app

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
