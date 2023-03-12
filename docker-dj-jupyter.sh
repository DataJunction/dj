jupyter notebook --no-browser --allow-root --port=8001 --ip=0.0.0.0 --NotebookApp.token='' --NotebookApp.password='' &
uvicorn dj.api.main:app --host 0.0.0.0 --port 8000 --reload
