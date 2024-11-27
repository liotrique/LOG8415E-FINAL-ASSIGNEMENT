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
            # For write queries, execute and commit the transaction
            cursor.execute(query)
            conn.commit()

            app.logger.info("Write query executed successfully")

            # Contact the workers with the write query
            for ip in public_ips:
                response = requests.post(
                    f"http://{ip}:5000/query",
                    json={"query": query},
                )
                app.logger.info(f"Response from worker {ip}: {response.json()}")

            return jsonify({"message": "Write query executed successfully"}), 200
        else:
            # For read queries, execute and fetch the result
            cursor.execute(query)
            result = cursor.fetchall()
            app.logger.info("Read query executed successfully")
            return jsonify(result), 200

    except Exception as e:
        app.logger.error(f"Error executing query: {e}")
        return jsonify({"error": str(e)}), 500

    finally:
        if conn:
            cursor.close()
            conn.close()  # Ensure the connection is closed


if __name__ == "__main__":
    app.run(debug=True)
