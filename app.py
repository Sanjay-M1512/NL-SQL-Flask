from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
import bcrypt
from pymongo import MongoClient
import google.generativeai as genai
import os
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
mongo_uri = os.getenv("MONGODB_URI")

# Configure Gemini
genai.configure(api_key=api_key)
model = genai.GenerativeModel('gemini-2.0-flash')

# Initialize Flask
app = Flask(__name__)
CORS(app)

# MongoDB connection
client = MongoClient(mongo_uri)
db = client['SQLConvertor']
users_collection = db['users']

# Create MySQL database function
def create_mysql_database(root_user, root_password, database_name):
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user=root_user,
            password=root_password
        )
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name};")
        connection.commit()
        cursor.close()
        connection.close()
        return True, "Database created successfully!"
    except mysql.connector.Error as err:
        return False, str(err)

# Register route
@app.route('/register', methods=['POST'])
def register():
    data = request.json
    username = data.get('username')
    email = data.get('email')
    password = data.get('password')
    root_user = data.get('mysql_root_user')
    root_password = data.get('mysql_root_password')
    database_name = data.get('database_name')

    if not all([username, email, password, root_user, root_password, database_name]):
        return jsonify({"error": "All fields are required!"}), 400

    success, message = create_mysql_database(root_user, root_password, database_name)
    if not success:
        return jsonify({"error": message}), 500

    password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    user_data = {
        "username": username,
        "email": email,
        "password_hash": password_hash,
        "mysql_root_user": root_user,
        "mysql_root_password": root_password,
        "mysql_databases": [database_name]
    }

    try:
        users_collection.insert_one(user_data)
        return jsonify({"message": "User registered successfully!"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Login route
@app.route('/login', methods=['POST'])
def login():
    data = request.json
    email = data.get('email')
    password = data.get('password')

    if not all([email, password]):
        return jsonify({"error": "Email and password are required!"}), 400

    user = users_collection.find_one({"email": email})
    if not user:
        return jsonify({"error": "User not found!"}), 404

    if not bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
        return jsonify({"error": "Invalid password!"}), 401

    return jsonify({
        "message": "Login successful!",
        "username": user["username"],
        "databases": user["mysql_databases"]
    }), 200

# Execute query route
@app.route('/execute_query', methods=['POST'])
def execute_query():
    data = request.json
    email = data.get('email')
    database_name = data.get('database_name')
    query_or_nl = data.get('query')

    if not all([email, database_name, query_or_nl]):
        return jsonify({"error": "Email, database name, and query are required!"}), 400

    user_data = users_collection.find_one({"email": email})
    if not user_data:
        return jsonify({"error": "User not found!"}), 404

    root_user = user_data.get("mysql_root_user")
    root_password = user_data.get("mysql_root_password")

    try:
        prompt = (
            f"Convert the following natural language query to a valid SQL query "
            f"for a MySQL database named '{database_name}':\n\n\"{query_or_nl}\"\n\nSQL Query:"
        )
        response = model.generate_content(prompt)
        sql_query = response.text.strip()

        sql_query = re.sub(r"```(?:sql)?", "", sql_query, flags=re.IGNORECASE).replace("```", "").strip()
        sql_query = re.sub(r'\s+', ' ', sql_query).strip()

        connection = mysql.connector.connect(
            host="localhost",
            user=root_user,
            password=root_password,
            database=database_name
        )
        cursor = connection.cursor()
        cursor.execute(sql_query)

        if sql_query.strip().upper().startswith("SELECT"):
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            result_data = [dict(zip(column_names, row)) for row in result]
        else:
            connection.commit()
            result_data = {"message": "Query executed successfully"}

        cursor.close()
        connection.close()

        return jsonify({
            "success": True,
            "nl_query": query_or_nl,
            "sql_query": sql_query,
            "result": result_data
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

# Get user databases
@app.route('/get_databases', methods=['GET'])
def get_databases():
    email = request.args.get('email')
    if not email:
        return jsonify({"error": "Email is required!"}), 400

    user_data = users_collection.find_one({"email": email})
    if not user_data:
        return jsonify({"error": "User not found!"}), 404

    databases = user_data.get("mysql_databases", [])
    return jsonify({"success": True, "databases": databases}), 200

# Get user info
@app.route("/user", methods=["GET"])
def get_user():
    email = request.args.get("email")
    if not email:
        return jsonify({"error": "Email is required"}), 400

    user = users_collection.find_one(
        {"email": email},
        {"_id": 0, "username": 1, "email": 1, "mysql_root_user": 1}
    )
    if user:
        return jsonify({"user": user}), 200
    else:
        return jsonify({"error": "User not found"}), 404

if __name__ == '__main__':
    app.run(debug=True)
