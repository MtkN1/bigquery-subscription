FROM python:3.12-slim-bookworm

COPY . .

RUN pip install .

CMD [ "python", "-Bum", "bigquery_subscription" ]
