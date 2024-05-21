# NYC Taxi Data Analysis

## Introduction
This project involves the analysis of the NYC taxi dataset using Apache Spark on Google Cloud Platform (GCP). The goal is to demonstrate various data processing and analysis techniques using Spark, including trip analysis, tip analysis, fare analysis, traffic analysis, and demand prediction.

## Setup Instructions
### Prerequisites
- Python 3.x
- Apache Spark
- Google Cloud Platform account

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


## Running the Analysis
Each analysis part is organized into separate jobs. To run a specific job, use the following commands. 

### Trip Analysis
Run the trip analysis job to calculate average duration and distance of rides by time of day, day of week, and month of year:
```sh
spark-submit src/jobs/trip_analysis.py data/
```

### Tip Analysis
Run the tip analysis job to analyze tip percentages by location, time, and payment type:
```sh
spark-submit src/jobs/tip_analysis.py data/
```

### Fare Analysis
Run the fare analysis job to calculate average fare by location, passenger count, and to analyze the correlation between fare amount and trip distance:
```sh
spark-submit src/jobs/fare_analysis.py data/
```

### Traffic Analysis
Run the traffic analysis job to calculate average trip speed and infer traffic conditions:
```sh
spark-submit src/jobs/traffic_analysis.py data/
```

### Demand Prediction
Run the demand prediction job to create features for demand prediction and use a regression model to predict the number of pickups:
```sh
spark-submit src/jobs/demand_prediction.py data/
```

## Results and Discussion
### Trip Analysis
- **Average Duration and Distance**:
  - The analysis reveals patterns such as longer trips during rush hours, on weekends, and during holiday seasons.
- **Popular Locations**:
  - Identified the top 10 pickup and dropoff locations, which can be visually mapped for better understanding.

### Tip Analysis
- **Tip Percentage by Location**:
  - Certain locations show higher tip percentages. There might be a correlation between trip distance and tip amount.
- **Tips by Time**:
  - Tips tend to vary by time of day, week, and year, potentially influenced by holidays or events.
- **Payment Type**:
  - The payment type affects tipping behavior, with some payment methods leading to higher tips.

### Fare Analysis
- **Average Fare by Location**:
  - Average fares vary significantly by pickup and dropoff locations.
- **Passenger Count**:
  - Analyzed the correlation between passenger count and fare amount.
- **Correlation with Distance**:
  - Found a significant correlation between fare amount and trip distance.

### Traffic Analysis
- **Average Trip Speed**:
  - Calculated the average speed of trips to infer traffic conditions.
- **Traffic Inference**:
  - Grouped trips by similar routes and analyzed speed variations to deduce traffic patterns.

### Demand Prediction
- **Feature Engineering**:
  - Created features from pickup datetime for the model.
- **Regression Model**:
  - Used a regression model to predict the number of pickups, which helps in understanding demand patterns.

## References
- [NYC Taxi Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Spark Documentation](https://spark.apache.org/docs/latest/)
- [Google Cloud Platform Documentation](https://cloud.google.com/docs)



