import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='password'
)

try:
    s3.create_bucket(Bucket='lakehouse')
except Exception:
    pass

metadata = '''{
    "format-version": 2,
    "table-uuid": "a1b2c3d4-e5f6-4a8b-9c0d-1e2f3a4b5c6d",
    "current-snapshot-id": 1001,
    "current-schema-id": 0,
    "schemas": [
        {"schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "long"}]}
    ],
    "snapshots": [
        {
            "snapshot-id": 1001,
            "timestamp-ms": 1700000000000,
            "summary": {"operation": "append"},
            "data-files": []
        }
    ]
}'''

s3.put_object(Bucket='lakehouse', Key='bronze/fmcg_sales/metadata/v1.json', Body=metadata.encode('utf-8'))
print('Successfully uploaded v1.json to MinIO lakehouse bucket!')
