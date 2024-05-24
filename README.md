# Analysis of Google Playstore Apps Project

This project leverages a dataset sourced from the Google Play Store to derive valuable insights into the Android app market. The dataset is chosen for its potential to provide meaningful information to various stakeholders including developers, businesses, and general users interested in the Android app ecosystem.

## Objective

Data Analysis and update information for Apps on the Google Play Store.

## Technologies

- Python
- Jupyter Notebook
- MySQL (Database)
- API
- Airflow
- Kafka
- PowerBI

## About the Data

The dataset contains 23 attributes, making it ideal for comprehensive data analysis and visualization. These efforts aim to inform development strategies within the Android application market. The dataset includes the following columns:

- App Name
- Category
- Score
- Installs
- Minimum Installs
- Maximum Installs
- Free
- Released
- Last Updated
- Content Rating

## Installation

### Prerequisites:

- Python 3.x
- Python Libraries: `pandas`, `pymysql`, `configparser`
- MySQL Server
- Airflow

### Setup:

### Step 1: Clone this repository 
### Step 2: Database Configuration Ensure MySQL Server is installed and running. Create a user and assign the necessary permissions.
 **Configuration File**:
   - Within the `db_conexion` folder, create a `config.ini` file with the following structure, adjusting the values according to your MySQL setup:

```ini
[mysql]
host = localhost
user = your_user
password = your_password
```
### Step 3: Virtual Environment
Open your linux terminal and install and activate your virtual environment with the following code:
```
python3 -m venv nombre_del_entorno
```
### Step 4: Activate Virtual Environment
```
source nombre_del_entorno/bin/activate
```
### Step 5: Install Dependencies
Run the following command in your terminal to install the dependencies needed to run this project:
```
pip install -r requirements.txt
```
### Step 6: Install Apache Airflow
Install Airflow in your repository folder
```
pip install apache-airflow
```

After installing Airflow, you must run the following command while in the repository root to set the AIRFLOW_HOME environment variable:
```
export AIRFLOW_HOME=$(pwd)
```

You need to adjust the settings in the `airflow.cfg` file. In the `dags_folder` section, make sure to indicate the location of the DAGs. Change `dags` to `Airflow` to look like this:
```
dags_folder = /root/proyect_google_play/Airflow
```
### Step 7: Running the DAG in Airflow:
Once you have set up Airflow and your DAGs are in the correct location, start Airflow using the following command from the repository root:
```
airflow standalone
```
Then, log in to the Airflow dashboard, find the DAG named `proyect_google_play`," and execute it to begin the ETL process.
Note: Ensure that all preceding steps have been successfully completed before running the DAG in Airflow.

Additional Notes:
Ensure the scripts are executed in the given order to avoid database-related errors.
