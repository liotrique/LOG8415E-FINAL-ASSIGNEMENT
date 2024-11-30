import requests
import json
from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

# read "public_ips.json" file to get the public IPs of the workers
with open("public_ips.json", "r") as f:
    public_ips = json.load(f)

proxy_ip = public_ips["proxy"]


@app.route("/", methods=["GET"])
def home():
    return "Trusted host instance"


@app.route("/query", methods=["POST"])
def query():
    try:
        data = request.json
        query = data.get("query")

        if not query:
            return jsonify({"error": "No query provided"}), 400

        url = f"http://{proxy_ip}:5000/query"
        response = requests.post(url, json={"query": query})
        return jsonify(response.json()), response.status_code

    except Exception as e:
        app.logger.error(f"Error executing query: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/mode", methods=["GET"])
def get_mode():
    # call proxy to get the mode
    url = f"http://{proxy_ip}:5000/mode"
    response = requests.get(url)
    return jsonify(response.json()), response.status_code


@app.route("/mode", methods=["POST"])
def set_mode():
    # call proxy to set the mode
    data = request.json
    mode = data.get("mode")
    url = f"http://{proxy_ip}:5000/mode"
    response = requests.post(url, json={"mode": mode})
    return jsonify(response.json()), response.status_code


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
