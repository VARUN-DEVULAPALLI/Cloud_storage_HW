import boto3
import csv

s3 = boto3.resource('s3', aws_access_key_id='',
                    aws_secret_access_key='')
'''
try:
    s3.create_bucket(Bucket='dvsmaster', CreateBucketConfiguration={
                     'LocationConstraint': 'us-west-2'})
except:
    print("this may already exist")
'''
bucket = s3.Bucket("dvsmaster")
bucket.Acl().put(ACL='public-read')
body = open(r'C:\Users\Varun_Devulapalli\Desktop\backgrounds\unnamed.jpg', 'rb')
o = s3.Object('dvsmaster', 'test2').put(Body=body)
s3.Object('dvsmaster', 'test2').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb', region_name='us-west-2',
                       aws_access_key_id='', aws_secret_access_key='')
try:
    table = dyndb.create_table(TableName='DataTable', KeySchema=[
        {'AttributeName': 'PartitionKey', 'KeyType': 'HASH'}, {'AttributeName': 'RowKey', 'KeyType': 'RANGE'}], AttributeDefinitions=[{'AttributeName': 'PartitionKey', 'AttributeType': 'S'}, {'AttributeName': 'RowKey', 'AttributeType': 'S'}, ], ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5})
except:
    table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
print(table.item_count)
with open(r'C:\Users\Varun_Devulapalli\Desktop\1660\hw2\data.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        body = open(
            r'C:\Users\Varun_Devulapalli\Desktop\1660\hw2\\'+item[3], 'rb')
        s3.Object('dvsmaster', item[3]).put(Body=body)
        md = s3.Object('dvsmaster', item[3]).Acl().put(ACL='public-read')
        url = item[0]
        metadata_item = {'PartitionKey': item[3], 'RowKey': item[4], 'cuteness': item[1],
                         'animal': item[2], 'filename': item[3], 'url': item[0]}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item is already there or another failure")
response = table.get_item(Key={'PartitionKey': 'trash.png', 'RowKey': '1'})
item = response['Item']
print(item)
