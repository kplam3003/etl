FROM python:3.7

RUN pip install gunicorn

COPY requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /app
COPY . .

ENV PORT=5000

# Run the web service on container startup.
CMD exec gunicorn --bind :$PORT --workers 2 --threads 8 --timeout 0 app:app
