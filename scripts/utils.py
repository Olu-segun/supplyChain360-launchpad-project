import boto3
import os
import logging
from sqlalchemy import create_engine
from google.oauth2.service_account import Credentials
import json

# ----------------------------
# LOGGER SETUP
# ----------------------------

def get_logger(name: str):
    logger = logging.getLogger(name)
    if not logger.handlers:  # prevent duplicate handlers
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger



REGION = "eu-west-2"
logger = get_logger(__name__)





# ----------------------------
# BASE SESSION (KEY FIX)
# ----------------------------
def get_boto3_session(region=REGION):
    
    try:
        profile = os.getenv("AWS_PROFILE")

        if profile:
            logger.info(f"Using AWS profile: {profile}")
            return boto3.Session(profile_name=profile, region_name=region)

        logger.info("Using default AWS credential.")
        return boto3.Session(region_name=region)

    except Exception as e:
        logger.error(f"Failed to create boto3 session: {e}")
        raise


# ----------------------------
# SSM CLIENT (USES SESSION)
# ----------------------------
def get_ssm_client(region=REGION):
    session = get_boto3_session(region)
    return session.client("ssm")


# ----------------------------
# S3 CLIENTS (FETCH CREDS FROM SSM)
# ----------------------------
def get_source_s3_client(region=REGION):
    ssm = get_ssm_client(region)

    access_key = ssm.get_parameter(Name="/source/aws/access_key")["Parameter"]["Value"]
    secret_key = ssm.get_parameter(Name="/source/aws/secret_key")["Parameter"]["Value"]

    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )


def get_destination_s3_client(region=REGION):
    ssm = get_ssm_client(region)

    access_key = ssm.get_parameter(Name="/destination/aws/access_key")["Parameter"]["Value"]
    secret_key = ssm.get_parameter(Name="/destination/aws/secret_key")["Parameter"]["Value"]
	
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )


# ----------------------------
# COPY OBJECT
# ----------------------------
def copy_object(source_bucket, source_key, dest_bucket, dest_key, region=REGION):
    source_s3 = get_source_s3_client(region)
    dest_s3 = get_destination_s3_client(region)

    obj = source_s3.get_object(Bucket=source_bucket, Key=source_key)
    dest_s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=obj["Body"].read())


# ----------------------------
# POSTGRES ENGINE (FROM SSM)
# ----------------------------
def get_db_engine(region=REGION, connect_args=None):
    ssm = get_ssm_client(region)

    host = ssm.get_parameter(Name="/supplychain360/db/host")["Parameter"]["Value"].strip()
    port = ssm.get_parameter(Name="/supplychain360/db/port")["Parameter"]["Value"].strip()
    user = ssm.get_parameter(Name="/supplychain360/db/user")["Parameter"]["Value"].strip()
    password = ssm.get_parameter(Name="/supplychain360/db/password")["Parameter"]["Value"].strip()
    database = ssm.get_parameter(Name="/supplychain360/db/dbname")["Parameter"]["Value"].strip()

    default_connect_args = {
        "connect_timeout": 30,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "options": "-c statement_timeout=0",
    }
    if connect_args:
        default_connect_args.update(connect_args)

    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
        connect_args=default_connect_args,
        pool_pre_ping=True,
        pool_recycle=1800,
    )
    
    
# ----------------------------
# GOOGLE SERVICE ACCOUNT CREDS (FROM SSM)
# ----------------------------
def get_google_service_account_credentials(param_name="google_sheet_api", scopes=None, region=REGION):
    """
    Fetch Google service account JSON from SSM Parameter Store
    and return a Credentials object.
    """
    ssm = get_ssm_client(region)
    logger.info(f"Fetching Google service account JSON file from SSM: {param_name}")
    response = ssm.get_parameter(Name=param_name)
    service_account_json = response["Parameter"]["Value"]

    info = json.loads(service_account_json)
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return creds