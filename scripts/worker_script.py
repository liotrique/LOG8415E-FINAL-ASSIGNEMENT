import os
import mysql.connector
from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

# MySQL configurations (using environment variables for security)
app.config["MYSQL_DATABASE_USER"] = os.getenv("MYSQL_USER", "root")
app.config["MYSQL_DATABASE_PASSWORD"] = os.getenv("MYSQL_PASSWORD", "root_password")
app.config["MYSQL_DATABASE_DB"] = os.getenv("MYSQL_DB", "sakila")
app.config["MYSQL_DATABASE_HOST"] = os.getenv("MYSQL_HOST", "localhost")

logging.basicConfig(level=logging.INFO)


@app.route("/", methods=["GET"])
def home():
    return "Worker instance"


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
            conn.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
