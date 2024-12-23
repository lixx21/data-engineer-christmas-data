from airflow.utils.dates import days_ago
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta 
from src.extract_transform import extract_csv_data, extract_spotify_data, extract_weather_data
from src.load import upload_to_s3

default_dag = {
    "owner": "Felix Pratamasan",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(hours=1)
}

with DAG (
    dag_id= "christmas_data_pipeline",
    schedule_interval= "0 1 * * *",
    default_args= default_dag,
    description="Create data pipeline for Christmas data",
    max_active_runs = 1
) as dag:

    def extract_csv(**kwargs):  
        christmas_list, christmas_movies = extract_csv_data()
        
        return "success extract csv data"

    def extract_spotify(**kwargs):

        data = extract_spotify_data()
        
        print(data[0])

        return "success extract spotify data"

    def extract_weather(**kwargs):

        data = extract_weather_data()

        print(data)

        return "success extract weather data"

    def upload_data_to_s3(**kwargs):
        
        bucket_name = "christmas-project-data"

        print(upload_to_s3("dags/data/Christmas Sales and Trends.csv", bucket_name, "Christmas_Sales_and_Trends.csv"))
        print(upload_to_s3("dags/data/christmas_movies.csv", bucket_name, "Christmas_Movies.csv"))
        print(upload_to_s3("dags/data/christmas_playlist.csv", bucket_name, "Christmas_Playlist.csv"))
        print(upload_to_s3("dags/data/weather_data.csv", bucket_name, "Weather_Data.csv"))

        return "success upload all data to S3 Bucket"

    # task_extract_csv_data = PythonOperator(
    #     task_id = "extract_csv_data",
    #     python_callable=extract_csv,
    #     provide_context = True
    # )   
    
    task_extract_spotify_data = PythonOperator(
        task_id = "extract_spotify_data",
        python_callable=extract_spotify,
        provide_context = True
    )

    task_extract_weather_data = PythonOperator(
        task_id = "extract_weather_data",
        python_callable=extract_weather,
        provide_context = True
    )

    task_upload_data_to_s3 = PythonOperator(
        task_id= "upload_data_to_s3",
        python_callable=upload_data_to_s3,
        provide_context=True
    )

    

    # task_extract_csv_data
    [task_extract_spotify_data, task_extract_weather_data] >> task_upload_data_to_s3
