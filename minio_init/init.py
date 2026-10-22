import os
import time
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

s3 = boto3.client(
    "s3", endpoint_url=os.environ["AWS_ENDPOINT_URL"], region_name="eu-west-3"
)

for bucket in ["raw", "clean"]:
    for _ in range(20):
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"Bucket créé : {bucket}", flush=True)
            break
        except s3.exceptions.BucketAlreadyOwnedByYou:
            print(f"Bucket déjà présent : {bucket}", flush=True)
            break
        except EndpointConnectionError:
            time.sleep(2)
    else:
        raise SystemExit(f"MinIO injoignable, bucket non créé : {bucket}")
