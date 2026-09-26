#!/bin/sh
# Create the Iceberg warehouse bucket once LocalStack is ready.
awslocal s3api create-bucket --bucket lakehouse
