FROM python:3.7.11-slim-buster

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

ENTRYPOINT ["python"]

CMD ["StreamToPubsub.py"]
