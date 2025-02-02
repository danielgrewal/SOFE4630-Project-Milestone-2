# SOFE4630-Project-Milestone-2 Design Part: Data Storage and Integration Connectors

## Overview
This project implements a cloud-based data processing pipeline using **Google Cloud Pub/Sub, MySQL, and Redis**. The system consists of a **producer script** that publishes **CSV records and images** to the cloud, and a **consumer script** that retrieves and verifies the stored data.

## Features
✅ Publishes **CSV data** to MySQL via Google Cloud Pub/Sub  
✅ Publishes **images** to Redis via Google Cloud Pub/Sub  
✅ Retrieves stored **CSV records** and saves them to a new CSV file  
✅ Retrieves stored **images** and reconstructs them from Base64 encoding  
✅ Uses **Google Cloud integrations** for efficient data storage and retrieval  

## Components
- **`producer.py`** → Reads CSV and image data, then publishes to Google Cloud Pub/Sub.
- **`consumer.py`** → Retrieves and verifies data from MySQL and Redis.
- **Google Cloud Services Used**:
  - **Pub/Sub** (message-based communication)
  - **Cloud SQL (MySQL)** (structured CSV storage)
  - **Cloud Memorystore (Redis)** (unstructured image storage)
