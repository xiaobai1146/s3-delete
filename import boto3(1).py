import boto3

def get_bucket_region(bucket_name):
    """
    获取指定存储桶的区域。

    参数:
    bucket_name (str): 存储桶的名称。

    返回:
    str: 存储桶所在的AWS区域。
    """
    s3 = boto3.client('s3')  # 初始化S3客户端
    response = s3.get_bucket_location(Bucket=bucket_name)  # 获取存储桶位置
    region = response['LocationConstraint']  # 从响应中提取区域
    if region is None:  # AWS对于美国东部（弗吉尼亚北部）区域返回None
        region = 'us-east-1'  # 将其标准化为'us-east-1'
    return region

def delete_all_objects(bucket_name, region):
    """
    删除指定S3存储桶中的所有对象及其所有版本。

    参数:
    bucket_name (str): 存储桶的名称。
    region (str): 存储桶的区域。
    """
    s3 = boto3.client('s3', region_name=region)  # 用正确的区域初始化S3客户端
    versioned = s3.get_bucket_versioning(Bucket=bucket_name)  # 检查版本控制是否启用
    if versioned.get('Status') == 'Enabled':  # 如果启用了版本控制
        version_paginator = s3.get_paginator('list_object_versions')
        version_pages = version_paginator.paginate(Bucket=bucket_name)

        for version_page in version_pages:
            objects_to_delete = {'Objects': []}  # 准备删除的对象列表
            versions = version_page.get('Versions', [])  # 获取所有版本
            delete_markers = version_page.get('DeleteMarkers', [])  # 获取所有删除标记
            for version in versions:
                objects_to_delete['Objects'].append({'Key': version['Key'], 'VersionId': version['VersionId']})
            for delete_marker in delete_markers:
                objects_to_delete['Objects'].append({'Key': delete_marker['Key'], 'VersionId': delete_marker['VersionId']})

            if objects_to_delete['Objects']:
                s3.delete_objects(Bucket=bucket_name, Delete=objects_to_delete)  # 删除所有列出的版本和标记
                for obj in objects_to_delete['Objects']:
                    print(f"从存储桶 {bucket_name} 删除了 {obj['Key']} 版本 {obj['VersionId']}")
    else:  # 如果没有启用版本控制
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name)
        for page in page_iterator:
            if 'Contents' in page:
                objects_to_delete = {'Objects': [{'Key': obj['Key']} for obj in page['Contents']]}
                s3.delete_objects(Bucket=bucket_name, Delete=objects_to_delete)
                for obj in objects_to_delete['Objects']:
                    print(f"从存储桶 {bucket_name} 删除了对象 {obj['Key']}")

def delete_bucket(bucket_name, region):
    """
    删除指定的S3存储桶。

    参数:
    bucket_name (str): 存储桶的名称。
    region (str): 存储桶的区域。
    """
    s3 = boto3.client('s3', region_name=region)  # 用正确的区域初始化S3客户端
    try:
        s3.delete_bucket(Bucket=bucket_name)  # 尝试删除存储桶
        print(f"已删除存储桶 {bucket_name}")
    except Exception as e:  # 处理异常，例如存储桶不为空时
        print(f"删除存储桶 {bucket_name} 失败: {str(e)}")

def empty_and_delete_all_buckets():
    """
    清空并删除AWS账户中列出的所有S3存储桶。
    """
    s3 = boto3.client('s3', region_name='us-east-1')  # 用默认区域初始化S3客户端
    buckets = s3.list_buckets()  # 列出所有存储桶

    for bucket in buckets['Buckets']:
        bucket_name = bucket['Name']
        bucket_region = get_bucket_region(bucket_name)  # 获取每个存储桶的区域
        print(f"正在删除存储桶 {bucket_name} 中的所有对象")
        delete_all_objects(bucket_name, bucket_region)  # 删除存储桶中的所有对象

        print(f"正在删除存储桶: {bucket_name}")
        delete_bucket(bucket_name, bucket_region)  # 尝试删除存储桶

if __name__ == "__main__":
    empty_and_delete_all_buckets()  # 运行脚本以清空并删除所有存储桶
