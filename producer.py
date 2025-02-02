from google.cloud import pubsub_v1
import csv
import glob
import json
import os
import base64

# This script will publish the CSV data to a MySQL database
# It will also publish images from the provided dataset to a Redis database

# Search for the JSON service account key
files = glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set project ID and topic names
project_id = "sofe4630"
csv_topic_name = "csvRecords"  # MySQL integration topic
image_topic_name = "Image2Redis"  # Redis integration topic

# Create publishers and define topic paths
publisher_csv = pubsub_v1.PublisherClient()
csv_topic_path = publisher_csv.topic_path(project_id, csv_topic_name)

# Correct way to enable ordering for images
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher_images = pubsub_v1.PublisherClient(publisher_options=publisher_options)
image_topic_path = publisher_images.topic_path(project_id, image_topic_name)

print(f"Publishing CSV records to {csv_topic_path}.")
print(f"Publishing images to {image_topic_path}.")

# Reads CSV file and publishes each row to the csvRecords topic
# Using MySQL connector and application integration this data is written to MySQL database
def publish_csv_records(csv_file="Labels.csv"):
    try:
        with open(csv_file, mode="r") as file:
            csv_reader = csv.DictReader(file)

            for row in csv_reader:
                # Convert CSV values to correct data types
                formatted_row = {
                    "Timestamp": int(row["Timestamp"]),
                    "Car1_Location_X": float(row["Car1_Location_X"]),
                    "Car1_Location_Y": int(row["Car1_Location_Y"]),
                    "Car1_Location_Z": float(row["Car1_Location_Z"]),
                    "Car2_Location_X": float(row["Car2_Location_X"]),
                    "Car2_Location_Y": int(row["Car2_Location_Y"]),
                    "Car2_Location_Z": float(row["Car2_Location_Z"]),
                    "Occluded_Image_view": row["Occluded_Image_view"],
                    "Occluding_Car_view": row["Occluding_Car_view"],
                    "Ground_Truth_View": row["Ground_Truth_View"],
                    "pedestrianLocationX_TopLeft": int(row["pedestrianLocationX_TopLeft"]),
                    "pedestrianLocationY_TopLeft": int(row["pedestrianLocationY_TopLeft"]),
                    "pedestrianLocationX_BottomRight": int(row["pedestrianLocationX_BottomRight"]),
                    "pedestrianLocationY_BottomRight": int(row["pedestrianLocationY_BottomRight"])
                }

                # Convert row to JSON format
                message_json = json.dumps(formatted_row).encode("utf-8")

                # Publish to Pub/Sub
                future = publisher_csv.publish(csv_topic_path, message_json)
                future.result()  # Ensure successful publishing

                print(f"Published CSV record: {formatted_row}")

        print("All CSV records published successfully.")

    except Exception as e:
        print(f"Error publishing CSV records: {e}")

# Reads images from folder, encodes in Base64, and publishes to Image2Redis topic
# Using Redis connector and application integration this data is written to Redis database
def publish_images(image_folder="Dataset_Occluded_Pedestrian"):
    image_files = glob.glob(os.path.join(image_folder, "*.*"))

    if not image_files:
        print("No images found.")
        return

    for image_path in image_files:
        try:
            image_name = os.path.basename(image_path)  # Use filename as key

            # Read and serialize image to Base64
            with open(image_path, "rb") as img_file:
                image_data = base64.b64encode(img_file.read()).decode("utf-8")

            # Publish to Pub/Sub
            future = publisher_images.publish(
                image_topic_path,
                image_data.encode("utf-8"),  # Message value (Base64 image)
                ordering_key=image_name  # Message key (image filename)
            )
            future.result()  # Ensure successful publishing

            print(f"Published image: {image_name}")

        except Exception as e:
            print(f"Failed to publish {image_name}: {e}")

    print("All images published successfully.")

# Run both functions, publish CSV first then publish images
if __name__ == "__main__":
    publish_csv_records()
    publish_images()
