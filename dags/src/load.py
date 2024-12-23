import boto3
from botocore.exceptions import NoCredentialsError
import os

def upload_to_s3(filename, bucket_name, object_name):

    # print(os.getenv("AWS_ACCESS_KEY_ID"))
    # print(os.getenv("AWS_SECRET_ACCESS_KEY"))
    # print(os.getenv("AWS_REGION_NAME"))

    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION_NAME") # e.g., 'us-west-2'
    )

    s3 = session.resource('s3')

    try:
        s3.meta.client.upload_file(filename, bucket_name, object_name)
        return f"File '{filename}' uploaded to S3 bucket '{bucket_name}' successfully."
    except FileNotFoundError:
        return f"The file '{filename}' was not found."
    except NoCredentialsError:
        return "Credentials not available."

