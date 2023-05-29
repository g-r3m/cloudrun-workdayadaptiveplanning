FROM python:3.10

WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY main.py /app
CMD exec gunicorn --bind :$PORT main:app --timeout 200
