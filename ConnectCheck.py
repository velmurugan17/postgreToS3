from PostgresLib import PostgreDB
from S3Lib import AwsS3
import json

postgre_obj = PostgreDB()
s3_obj = AwsS3()

postgre_obj.create_table()
postgre_obj.add_data_to_table(row=None)
postgre_data = postgre_obj.execute_query(query=None)
postgre_obj.close_connection()
# print(rows)
all_buckets = s3_obj.get_all_buckets()
# print(all_buckets)
all_files = s3_obj.get_all_files_in_bucket(all_buckets[-1])
# print(all_files)
contents = s3_obj.get_file_content(all_buckets[-1],all_files[0])
s3_data = json.loads(contents)

def prepare_data_for_comparison(data):
    prep_data = []
    for k in data.keys():
        d=(k,data[k]['orders counts'],data[k]['gross'],data[k]['net'])
        prep_data.append(d)
    return prep_data

aws_s3_data = prepare_data_for_comparison(s3_data)
aws_s3_data.sort()
postgre_data.sort()
print(aws_s3_data)
print(postgre_data)
assert aws_s3_data==postgre_data
for i in range(len(aws_s3_data)):
    if aws_s3_data[i]==postgre_data[i]:
        print("SUCCESS : AWS S3 and Postgres data are same")
    else:
        print("FAIL : Data mismatch...AWS data {} != Postgres data {}".format(aws_s3_data[i],postgre_data[i]))
