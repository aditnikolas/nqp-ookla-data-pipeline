import boto3
from botocore import UNSIGNED
from botocore.config import Config

s3 = boto3.client('s3', region_name='us-west-2', config=Config(signature_version=UNSIGNED))
paginator = s3.get_paginator("list_objects_v2")
quarter_list = []
for page in paginator.paginate(Bucket="ookla-open-data", Prefix="parquet/performance/type=mobile/"): 
    for obj in page.get("Contents", []): 
        if obj['Key'].endswith('.parquet'): 
            parts = obj['Key'].split('/') 
            term = {}
            term['Key'] = obj['Key']
            term['Size'] = obj['Size']
            for part in parts: 
                if '=' in part:
                    key, value = part.split("=", 1)
                    if value.isdigit():
                        value = int(value)
                    term[key] = value
            quarter_list.append(term)

quarter = {(d['year'], d['quarter']) for d in quarter_list}
latest_quarter = sorted(quarter)[-4:]
# print(f"quarter list: {quarter_list}")
files_to_download = []
for d in quarter_list:
    if (d["year"], d["quarter"]) in latest_quarter:
        files_to_download.append(d)

files_to_download.sort(key=lambda d: (d["year"], d["quarter"]))
for f in files_to_download:
    print(f"Key: {f['Key']}, Year: {f['year']}, Quarter: {f['quarter']}")


            # year = parts[3].split('=')
            # quarter = parts[4].split('=')
            # quarter_list.append((int(year[1]), int(quarter[1])))
            # i += 1
            # print(f"{i} Key : {obj['Key']}, Size: {obj['Size']/1e6:8.1f} MB")

# for x in sorted(quarter_list[-4:]):
#     print(x)

