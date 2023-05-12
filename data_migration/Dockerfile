FROM python:3.7

COPY requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /app
COPY . .

ENV PYTHONPATH /app

ENTRYPOINT [ "alembic", "upgrade", "head" ]
