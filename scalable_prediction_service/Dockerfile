FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

COPY requirements.txt .

RUN pip install -r requirements.txt
RUN apt-get update && apt-get install -y locales
RUN locale-gen en_US.UTF-8 && update-locale
RUN apt-get update && apt-get install -y locales && rm -rf /var/lib/apt/lists/* \
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')"

ENV LANG en_US.UTF-8 

WORKDIR /app

COPY . .

# Run the web service on container startup.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8083"]