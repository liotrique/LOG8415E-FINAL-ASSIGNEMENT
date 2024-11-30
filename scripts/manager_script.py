import os
import mysql.connector
import requests
import json
from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

# MySQL configurations (using environment variables for security)
app.config["MYSQL_DATABASE_USER"] = os.getenv("MYSQL_USER", "root")
app.config["MYSQL_DATABASE_PASSWORD"] = os.getenv("MYSQL_PASSWORD", "root_password")
app.config["MYSQL_DATABASE_DB"] = os.getenv("MYSQL_DB", "sakila")
app.config["MYSQL_DATABASE_HOST"] = os.getenv("MYSQL_HOST", "localhost")

logging.basicConfig(level=logging.INFO)

# read "public_ips.json" file to get the public IPs of the workers
with open("public_ips.json", "r") as f:
    public_ips = json.load(f)


@app.route("/", methods=["GET"])
def home():
    return "Manager instance"


@app.route("/query", methods=["POST"])
def query():
    try:
        data = request.json
        query = data.get("query")

        if not query:
            return jsonify({"error": "No query provided"}), 400

        # Check if the query is a read or write query
        is_write_query = (
            query.strip().lower().startswith(("insert", "update", "delete"))
        )

        # Open a new database connection using mysql-connector-python
        conn = mysql.connector.connect(
            user=app.config["MYSQL_DATABASE_USER"],
            password=app.config["MYSQL_DATABASE_PASSWORD"],
            host=app.config["MYSQL_DATABASE_HOST"],
            database=app.config["MYSQL_DATABASE_DB"],
        )
        cursor = conn.cursor()

        if is_write_query:
            # For write queries, execute the query and commit the changes
            cursor.execute(query)
            conn.commit()

            app.logger.info(
                "Write query executed successfully by manager (replicated on workers)"
            )

            # Contact the workers with the write query to replicate the changes
            for (
                name,
                ip,
            ) in public_ips.items():
                if not name.startswith("worker"):
                    continue
                response = requests.post(
                    f"http://{ip}:5000/query",
                    json={"query": query},
                )
                app.logger.info(
                    f"Response from worker {name} ({ip}): {response.json()}"
                )

            return (
                jsonify(
                    {
                        "message": "Write query executed successfully by manager (replicated on workers)",
                    }
                ),
                200,
            )
        else:
            # For read queries, execute and fetch the result
            cursor.execute(query)
            result = cursor.fetchall()

            app.logger.info("Read query executed successfully by manager")

            return jsonify(result), 200

    except Exception as e:
        app.logger.error(f"Error executing query: {e}")
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
