
# Text-to-SQL with Dynamic Data Ingestion

This repository contains a Flask-based web service that allows users to query CSV data using natural language. The system dynamically ingests a CSV file from an AWS S3 bucket, loads it into a temporary PostgreSQL table, and uses Google's Gemini AI to convert natural language questions into executable SQL queries.

## Key Features

-   **Natural Language to SQL:** Converts user questions (e.g., "show me the total sales by region") into SQL queries using the Google Gemini Pro model.
-   **Dynamic Data Ingestion:** Initiates a session by downloading a user-specified CSV from S3.
-   **Automated Schema Inference:** Automatically detects column data types (integer, float, date, string) from the CSV.
-   **Session-Based Architecture:** Creates a unique, temporary database table for each user session, ensuring data isolation.
-   **Data Visualization:** Automatically generates [Highcharts](https://www.highcharts.com/) chart configurations based on the query results for easy visualization.
-   **Safety & Cleanup:** Includes checks to prevent destructive SQL commands (`UPDATE`, `DELETE`, `DROP`) and automatically cleans up database resources when a session ends or the application shuts down.

## How It Works

1.  **Start Session (`/start_session`):** A user provides a UUID and the S3 key for a CSV file. The application downloads the file, infers its schema, creates a temporary table in PostgreSQL, and loads the data into it.
2.  **Query Data (`/query`):** The user sends a natural language question. The backend forwards the question, along with the table schema, to the Gemini AI.
3.  **Get SQL & Execute:** The AI returns a validated, safe-to-execute `SELECT` query in JSON format. The application runs this query against the session's temporary table.
4.  **Return Results:** The query results are returned to the user as JSON. The system also attempts to generate a chart configuration for the data.
5.  **View Chart (`/view_chart`):** Renders a simple HTML page displaying the generated chart.
6.  **End Session (`/end_session`):** The temporary PostgreSQL table and all associated session data are deleted.

## API Endpoints

-   `POST /start_session`: Initializes a new session with data from an S3 CSV file.
-   `POST /query`: Submits a natural language query to the active session.
-   `POST /read_collection_data`: Paginates through the raw data of the session's table.
-   `POST /view_chart`: Renders an HTML page with a chart of the last query result.
-   `POST /end_session`: Terminates the session and cleans up resources.

## Setup and Installation

### Prerequisites

-   Python 3.8+
-   PostgreSQL Database
-   AWS S3 Bucket with credentials
-   Google Gemini API Key

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/firstcryorg/TEXT-TO-SQL.git
    cd TEXT-TO-SQL
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```




### Running the Application

To start the Flask server, run:
```bash
python app/app.py
```
