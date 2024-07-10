import boto3

def get_bucket_region(bucket_name):
    """Retrieve the region of the specified bucket."""
    s3 = boto3.client('s3')
    response = s3.get_bucket_location(Bucket=bucket_name)
    region = response['LocationConstraint']
    # AWS returns None for the US East (N. Virginia) region. Standardize it to 'us-east-1'.
    if region is None:
        region = 'us-east-1'
    return region

def delete_all_objects(bucket_name, region):
    """Delete all objects and all versions in the specified S3 bucket."""
    s3 = boto3.client('s3', region_name=region)
    versioned = s3.get_bucket_versioning(Bucket=bucket_name)
    if versioned.get('Status') == 'Enabled':
        version_paginator = s3.get_paginator('list_object_versions')
        version_pages = version_paginator.paginate(Bucket=bucket_name)

        for version_page in version_pages:
            objects_to_delete = {'Objects': []}
            versions = version_page.get('Versions', [])
            delete_markers = version_page.get('DeleteMarkers', [])
            for version in versions:
                objects_to_delete['Objects'].append({'Key': version['Key'], 'VersionId': version['VersionId']})
            for delete_marker in delete_markers:
                objects_to_delete['Objects'].append({'Key': delete_marker['Key'], 'VersionId': delete_marker['VersionId']})

            if objects_to_delete['Objects']:
                s3.delete_objects(Bucket=bucket_name, Delete=objects_to_delete)
                for obj in objects_to_delete['Objects']:
                    print(f"Deleted {obj['Key']} version {obj['VersionId']} from bucket {bucket_name}")
    else:
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name)
        for page in page_iterator:
            if 'Contents' in page:
                objects_to_delete = {'Objects': [{'Key': obj['Key']} for obj in page['Contents']]}
                s3.delete_objects(Bucket=bucket_name, Delete=objects_to_delete)
                for obj in objects_to_delete['Objects']:
                    print(f"Deleted object {obj['Key']} from bucket {bucket_name}")

def delete_bucket(bucket_name, region):
    """Delete the specified S3 bucket."""
    s3 = boto3.client('s3', region_name=region)
    try:
        s3.delete_bucket(Bucket=bucket_name)
        print(f"Deleted bucket {bucket_name}")
    except Exception as e:
        print(f"Failed to delete bucket {bucket_name}: {str(e)}")

def empty_and_delete_all_buckets():
    """Empty and delete all S3 buckets."""
    s3 = boto3.client('s3', region_name='us-east-1')
    buckets = s3.list_buckets()

    for bucket in buckets['Buckets']:
        bucket_name = bucket['Name']
        bucket_region = get_bucket_region(bucket_name)  # Now this function is defined
        print(f"Deleting all objects in bucket: {bucket_name}")
        delete_all_objects(bucket_name, bucket_region)

        print(f"Deleting bucket: {bucket_name}")
        delete_bucket(bucket_name, bucket_region)

if __name__ == "__main__":
    empty_and_delete_all_buckets()
