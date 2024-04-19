# Analysis of Google Playstore Apps Project

This project leverages a dataset sourced from the Google Play Store to derive valuable insights into the Android app market. The dataset is chosen for its potential to provide meaningful information to various stakeholders including developers, businesses, and general users interested in the Android app ecosystem.

## Objective

Data Analysis and Ratings Prediction for Apps on the Google Play Store.

## Technologies

- Python
- Jupyter Notebook
- MySQL (Database)
- API
- PowerBI

## About the Data

The dataset contains 23 attributes, making it ideal for comprehensive data analysis and visualization. These efforts aim to inform development strategies within the Android application market. The dataset includes the following columns:

- App Name
- Category
- Rating
- Installs
- Minimum Installs
- Maximum Installs
- Free
- Size
- Minimum Android
- Released
- Last Updated
- Content Rating
- Ad Supported
- In App Purchases
- Editors Choice

## API

The `google-play-scraper` is an API for easily scraping the Google Play Store using Python, with no external dependencies. This API allows for retrieving app details, searching for apps by specific terms, and more.

## Installation

### Prerequisites:

- Python 3.x
- Python Libraries: `pandas`, `pymysql`, `configparser`
- MySQL Server

### Setup:

1. **Clone this repository**
2. **Database Configuration**: Ensure MySQL Server is installed and running. Create a user and assign the necessary permissions.
3. **Configuration File**:
   - Within the `db_conexion` folder, create a `config.ini` file with the following structure, adjusting the values according to your MySQL setup:

```ini
[mysql]
host = localhost
user = your_user
password = your_password
```
### Data:
Place the Google-Playstore-Dataset-Clean.csv file in a data subfolder within the main project directory.

### Running the Scripts:
1. ETL_csv.py: This script is responsible for creating the database and the main table, as well as loading the data from the CSV file.
   Run the script with the command: ```python ETL_csv.py```

3. ModelDimensional.py: This script creates the dimensional tables and a fact table, performing data transformations as necessary.
   Run the script with the command: ```python ModelDimensional.py```

Additional Notes:
Ensure the scripts are executed in the given order to avoid database-related errors.
