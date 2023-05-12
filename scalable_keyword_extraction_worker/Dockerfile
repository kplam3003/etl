FROM python:3.8

COPY requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /app
COPY . .

ENV SPACY_MODEL=en_core_web_md

RUN python -m spacy download ${SPACY_MODEL}

CMD ["python", "app.py"]
