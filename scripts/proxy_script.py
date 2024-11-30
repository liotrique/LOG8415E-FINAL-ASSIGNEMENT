import time
import requests
import json
from flask import Flask, request, jsonify
import logging
import random

# "DIRECT_HIT", "RANDOM" or "CUSTOMIZED"
mode = "DIRECT_HIT"

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

# read "public_ips.json" file to get the public IPs of the workers
with open("public_ips.json", "r") as f:
    public_ips = json.load(f)

# filter out the ones that are not workers or manager
public_ips = {
    key: value
    for key, value in public_ips.items()
    if key.startswith(("worker", "manager"))
}


@app.route("/", methods=["GET"])
def home():
    return "Proxy instance"


@app.route("/query", methods=["POST"])
def query():
    try:
        data = request.json
        query = data.get("query")

        if not query:
            return jsonify({"error": "No query provided"}), 400

        is_write_query = (
            query.strip().lower().startswith(("insert", "update", "delete"))
        )

        if is_write_query:
            url = f"http://{public_ips['manager']}:5000/query"
            response = requests.post(url, json={"query": query})
            response_data = {}
            response_data["handled_by"] = "manager"
            response_data["result"] = response.json()
            return jsonify(response_data), response.status_code

        else:
            global mode

            if mode == "DIRECT_HIT":
                url = f"http://{public_ips['manager']}:5000/query"
                response = requests.post(url, json={"query": query})
                response_data = {}
                response_data["handled_by"] = "manager"
                response_data["result"] = response.json()
                return jsonify(response_data), response.status_code

            elif mode == "RANDOM":
                # keep only workers in public ips
                worker_ips = {
                    key: value
                    for key, value in public_ips.items()
                    if key.startswith("worker")
                }
                target = random.choice(list(worker_ips.keys()))
                ip = public_ips[target]
                url = f"http://{ip}:5000/query"
                response = requests.post(url, json={"query": query})
                response_data = {}
                response_data["handled_by"] = target
                response_data["result"] = response.json()
                return jsonify(response_data), response.status_code

            elif mode == "CUSTOMIZED":
                ping = {}
                # keep only workers in public ips
                worker_ips = {
                    key: value
                    for key, value in public_ips.items()
                    if key.startswith("worker")
                }
                for key, ip in worker_ips.items():
                    try:
                        start_time = time.time()
                        requests.get(f"http://{ip}:5000/", timeout=2)
                        ping[key] = time.time() - start_time
                    except requests.exceptions.RequestException:
                        ping[key] = float("inf")

                worker_name = min(ping, key=ping.get)
                ip = public_ips[worker_name]
                url = f"http://{ip}:5000/query"
                response = requests.post(url, json={"query": query})
                response_data = {}
                response_data["handled_by"] = worker_name
                response_data["result"] = response.json()
                response_data["pings"] = ping
                return jsonify(response_data), response.status_code

    except Exception as e:
        app.logger.error(f"Error executing query: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/mode", methods=["GET"])
def get_mode():
    global mode
    return jsonify({"mode": mode}), 200


@app.route("/mode", methods=["POST"])
def set_mode():
    global mode
    try:
        data = request.json
        new_mode = data.get("mode")
        if new_mode not in ["DIRECT_HIT", "RANDOM", "CUSTOMIZED"]:
            return jsonify({"error": "Invalid mode"}), 400
        mode = new_mode
        return jsonify({"mode": mode}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
