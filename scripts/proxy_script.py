import os
import requests
import json
from flask import Flask, request, jsonify
import logging
import random

# "DIRECT_HIT", "RANDOM", "CUSTOMIZED"
mode = "DIRECT_HIT"

app = Flask(__name__)

# MySQL configurations (using environment variables for security)
app.config["MYSQL_DATABASE_USER"] = os.getenv("MYSQL_USER", "root")
app.config["MYSQL_DATABASE_PASSWORD"] = os.getenv("MYSQL_PASSWORD", "root_password")
app.config["MYSQL_DATABASE_DB"] = os.getenv(
    "MYSQL_DB", "sakila"
)  # Change to your database
app.config["MYSQL_DATABASE_HOST"] = os.getenv("MYSQL_HOST", "localhost")

# Set up logging
logging.basicConfig(level=logging.INFO)

# read "public_ips.json" file to get the public IPs of the workers
with open("public_ips.json", "r") as f:
    public_ips = json.load(f)


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
            # Send it to the manager instance
            url = f"http://{public_ips['manager']}:5000/query"
            response = requests.post(url, json={"query": query})
            return response.json(), response.status_code

        else:
            # Use app.config to get the mode
            mode = app.config.get("MODE", "DIRECT_HIT")

            if mode == "DIRECT_HIT":
                url = f"http://{public_ips['manager']}:5000/query"
                response = requests.post(url, json={"query": query})
                return response.json(), response.status_code

            elif mode == "RANDOM":
                ip = random.choice(list(public_ips.values()))
                url = f"http://{ip}:5000/query"
                response = requests.post(url, json={"query": query})
                return response.json(), response.status_code

            else:
                # TODO: implement a customized mode
                pass

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
