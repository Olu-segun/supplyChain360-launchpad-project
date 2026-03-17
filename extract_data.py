import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import boto3
import io
import hashlib

# -------------------------------
# 1. Google Sheets Authentication
# -------------------------------
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    "credentials.json", scope
)

client = gspread.authorize(creds)

sheet = client.open("MySheetName").sheet1
data = sheet.get_all_values()

# -------------------------------
# 2. Convert to DataFrame
# -------------------------------
df = pd.DataFrame(data[1:], columns=data[0])

# -------------------------------
# 3. Convert to Parquet in memory
# -------------------------------
buffer = io.BytesIO()
df.to_parquet(buffer, engine="pyarrow", index=False)
buffer.seek(0)

# -------------------------------
# 4. Compute Hash for Idempotency
# -------------------------------
file_hash = hashlib.md5(buffer.getvalue()).hexdigest()

# -------------------------------
# 5. Upload to S3 if changed
# -------------------------------
s3 = boto3.client("s3")

bucket_name = "supplychain360-data-lake-bucket"
object_key = "store-locations/store_locations.parquet"

try:
    response = s3.head_object(Bucket=bucket_name, Key=object_key)
    existing_hash = response["Metadata"].get("filehash")

    if existing_hash == file_hash:
        print("No changes detected. Skipping upload.")
    else:
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=buffer.getvalue(),
            Metadata={"filehash": file_hash}
        )
        print("Data changed. File updated in S3.")

except s3.exceptions.ClientError:
    # Object does not exist yet
    s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=buffer.getvalue(),
        Metadata={"filehash": file_hash}
    )
    print("File uploaded to S3 for the first time.")
