FROM python:3.9-slim

RUN python -m pip install --upgrade pip setuptools
RUN python -m pip install pandas sentence-transformers openai fastapi uvicorn

EXPOSE 8500
WORKDIR /workspace
