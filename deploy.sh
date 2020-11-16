zip dist.zip main.py requirements.txt CA.pem
aws --endpoint-url=https://storage.yandexcloud.net  s3 cp  dist.zip s3://yourdevbucketname/dist.zip
yc serverless function version create       \
    --function-name billfunc                    \
    --runtime python37-preview                      \
    --entrypoint main.handler               \
    --memory 512M                           \
    --execution-timeout 600s                  \
    --package-bucket-name vsgrab-dev         \
    --package-object-name dist.zip          \
    --environment STORAGE_BUCKET=my-bucket  \
    --environment AWS_ACCESS_KEY_ID=XXX     \
    --environment AWS_SECRET_ACCESS_KEY=XXX \
     --environment CH_PASSWORD=password \
     --environment CH_HOST=hostfqdn \
     --environment CH_DB=db1 \
     --environment CH_USER=chuser \
     --environment CH_TABLE=billing \
     --environment STORAGE_FOLDER=yc-billing-export