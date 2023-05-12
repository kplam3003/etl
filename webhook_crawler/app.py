import os
from flask import Flask, request, jsonify
import config
import json
import handlers
import utils
import base64
import hashlib
import sys
import pandas as pd
import requests

app = Flask(__name__)

logger = utils.load_logger()


def source_to_collector_id(source_type):
    return {
        "g2_reviews": config.LUMINATI_DCA_ID_G2_REVIEWS,
        "linkedin_company_profile": config.LUMINATI_DCA_ID_LINKEDIN_COMPANY_PROFILE,
    }.get(source_type, config.LUMINATI_DCA_ID_G2_REVIEWS)


@app.route("/", methods=["GET"])
def hello():
    return jsonify({"message": "Hello World"}), 200


@app.route("/luminati", methods=["POST"])
def create_luminati_job():
    headers = request.headers
    secret = headers.get("Authorization")
    if secret != config.SECRET:
        return jsonify({"success": False}), 403

    body = request.json
    url = body.get("url")
    source_type = body.get("source_type")
    collector_id = source_to_collector_id(source_type)

    res = requests.post(
        config.LUMINATI_DCA_URL,
        params={"collector": collector_id, "queue_next": 1},
        headers={"Authorization": f"Bearer {config.LUMINATI_DCA_TOKEN}"},
        json=[{"url": url}],
    )

    try:
        res_data = res.json()
        return jsonify(res_data), 200

    except json.decoder.JSONDecodeError:
        logger.exception(
            f"Error while decoding JSON from response content: {res_data.content}"
        )
        return (
            jsonify({"message": "Error while decoding JSON from response content"}),
            500,
        )


@app.route("/luminati/<collection_id>", methods=["GET"])
def retrieve_luminati_data(collection_id=None):
    headers = request.headers
    secret = headers.get("Authorization")
    if secret != config.SECRET:
        return jsonify({"success": False}), 403

    page = int(request.args.get("page", "1"))

    gcs_file = f"gs://{config.GCP_STORAGE_BUCKET}/luminati/{collection_id}.csv"
    try:
        chunks = pd.read_csv(gcs_file, chunksize=config.ROWS_PER_STEP)
        data = []
        total = 0
        for ix, chunk in enumerate(chunks, 1):
            if ix == page:
                data = chunk.to_json(orient="records")
                data = json.loads(data)
            total += chunk.shape[0]

        return jsonify({"total": total, "data": data}), 200

    except:
        logger.info(f"Not found")
        return jsonify({"error": "file not found"}), 404


@app.route("/webhook/luminati", methods=["POST", "PUT", "GET"])
def webhook_luminati():
    headers = request.headers
    logger.info(f"Receive luminati webhook {headers}")

    dca_id = headers.get("dca-collection-id")
    body = request.json
    logger.info(f"Receive total of {len(body)} records. Processing")

    df = pd.DataFrame(body)
    gcs_file = f"gs://{config.GCP_STORAGE_BUCKET}/luminati/{dca_id}.csv"
    df.to_csv(gcs_file, index=False)

    logger.info(f"Uploaded {gcs_file} file to gcs")

    return "", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
