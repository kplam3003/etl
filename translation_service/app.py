import os
from flask import Flask, request, jsonify
from flask_caching import Cache
import config
import handlers
import utils
import sys
import traceback


cache = Cache(
    config={
        "CACHE_TYPE": "memcached",
        "CACHE_MEMCACHED_SERVERS": [config.MEMCACHED_SERVER],
        "CACHE_KEY_PREFIX": "translatesrv",
        "CACHE_DEFAULT_TIMEOUT": 0,  # Never expire
    }
)

app = Flask(__name__)
cache.init_app(app)

logger = utils.load_logger()


@app.route("/", methods=["GET"])
def hello():
    return jsonify({"message": "Hello World"}), 200


@app.route("/translate", methods=["POST"])
def translate():
    headers = request.headers
    x_ref_id = headers.get("x-ref-id")

    body = request.json
    text = body.get("text", [])
    service = body.get("service", "google")
    secret = headers.get("Authorization")
    assert text is not None, "Need some text to translate"
    assert secret == config.SECRET, "Unknow error occured"

    logger.info(
        f"translate - summary: x_ref_id={x_ref_id} - {len(text)} items - text: {text}"
    )

    response = None

    if config.CACHE_ENABLE:
        cache_key = f"{service}_{hash(frozenset(text))}"

        response = cache.get(cache_key)

    if response is None or not config.CACHE_ENABLE:
        try:
            response = handlers.translate(text, service)
        except:
            clazz, msg, _ = sys.exc_info()
            error = traceback.format_exc()
            logger.info(f"[ERROR] - x_ref_id={x_ref_id}: unable to translate due to {msg} - {error}")
            logger.exception(msg)
            return f"{msg}", 500
    else:
        logger.info(f"Using cached data: {cache_key}")

    if config.CACHE_ENABLE:
        cache.set(cache_key, response)

    logger.info(f"translated - x_ref_id={x_ref_id}: {response}")
    response = jsonify(response)
    response.headers.set("x-ref-id", x_ref_id)

    return response, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
