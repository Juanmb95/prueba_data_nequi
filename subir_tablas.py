# -*- coding: utf-8 -*-
"""
Created on Thu May 18 23:07:52 2023

@author: juan-
"""

import boto3

def upload_csv_to_s3(bucket_name, file_path, s3_key):
    # Crear una instancia del cliente S3
    s3 = boto3.client('s3')
    # Subir el archivo al cubo de S3
    s3.upload_file(file_path, bucket_name, s3_key)



s3 = boto3.resource('s3')
for bucket in s3.buckets.all():
    print(bucket.name)
bucket_name = 'airbnb-nequi'
#print(bucket_name)

file_path = 'reviews.csv'


lista = ["listings_edit","reviews","calendar"]

for i in lista:
    s3_key = 's3://airbnb-nequi/{}/{}.csv'.format(i,i)
    upload_csv_to_s3(bucket_name, file_path, s3_key)
    print("se cargo archivo {}".format(i))