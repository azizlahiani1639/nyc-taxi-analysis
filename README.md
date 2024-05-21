# NYC Taxi Data Analysis

## Introduction
This project involves the analysis of the NYC taxi dataset using Apache Spark on Google Cloud Platform (GCP). The goal is to demonstrate various data processing and analysis techniques using Spark, including trip analysis, tip analysis, fare analysis, traffic analysis, and demand prediction.

## Setup Instructions
### Prerequisites
- Python 
- Apache Spark
- Google Cloud Platform account
- Access to the NYC taxi dataset in Parquet format stored in a Google Cloud Storage (GCS) bucket

### Step-by-Step Setup
1. **Clone the repository**:
    ```sh
    git clone https://github.com/azizlahiani1639/nyc-taxi-analysis.git
    cd nyc-taxi-analysis
    ```

2. **Install necessary dependencies**:
    ```sh
    pip install -r requirements.txt
    ```

3. **Tip Analysis**
    Run the tip analysis job to analyze tip percentages by location, time, and payment type:
    ```sh
    spark-submit src/jobs/tip_analysis.py data/
    ```
4. **Fare Analysis**
    Run the fare analysis job to calculate average fare by location, passenger count, and to analyze the correlation between fare amount and trip distance:
    ```sh
    spark-submit src/jobs/fare_analysis.py data/
    ```
5. **Traffic Analysis**
    Run the traffic analysis job to calculate average trip speed and infer traffic conditions:
    ```sh
    spark-submit src/jobs/traffic_analysis.py data/
    ```
6. **Demand Prediction**
    Run the demand prediction job to create features for demand prediction and use a regression model to predict the number of pickups:
    ```sh
    spark-submit src/jobs/demand_prediction.py data/
    ```

