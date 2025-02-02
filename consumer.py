import redis
import base64
import mysql.connector
import csv

# This script is for testing if all data was published successfully to cloud
# The script will pull data from MySQL and write back into a CSV, and
# pull image data from Redis and write back into image files

# Redis config
REDIS_HOST = "34.60.231.110"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = "sofe4630u"

# MySQL config
MYSQL_HOST = "35.192.83.12"
MYSQL_USER = "usr"
MYSQL_PASSWORD = "sofe4630u"
MYSQL_DATABASE = "Readings"

# Retrieves all images from Redis and saves them as files
def fetch_images_from_redis():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)

        # Get all keys (image names) stored in Redis
        image_keys = r.keys("*")  # List all stored keys
        if not image_keys:
            print("No images found in Redis.")
            return

        for key in image_keys:
            image_data = r.get(key)  # Retrieve Base64 encoded image
            if image_data:
                decoded_image = base64.b64decode(image_data)

                # Save image to file
                filename = key.decode("utf-8")  # Convert bytes key to string
                with open(f"retrieved_{filename}", "wb") as f:
                    f.write(decoded_image)

                print(f"Saved image: retrieved_{filename}")

        print("All images retrieved successfully.")

    except Exception as e:
        print(f"Error retrieving images from Redis: {e}")

# Retrieves all records from MySQL and saves them into a CSV file
def fetch_csv_from_mysql():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cursor = conn.cursor()

        # Fetch all records from the Labels table
        query = "SELECT * FROM CSVlabels;"
        cursor.execute(query)
        rows = cursor.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Save data to CSV
        with open("retrieved_data.csv", mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(column_names)  # Write header
            writer.writerows(rows)  # Write data

        print("CSV data retrieved and saved as retrieved_data.csv")

        # Close database connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error retrieving data from MySQL: {e}")

# Run both functions to get image data and CSV data from cloud
if __name__ == "__main__":
    fetch_images_from_redis()
    fetch_csv_from_mysql()
