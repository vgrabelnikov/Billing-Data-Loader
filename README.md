# Billing-Data-Loader
Serverless Function for loading Yandex.Cloud billing data to ClickHouse

# Prerequisites
Ask Yandex.Cloud support to export your billing data to S3 bucket of your choice

https://cloud.yandex.ru/docs/billing-folder-report/get-folder-report

Also you need to have Service Account and access keys to access Object Storage (previously mentioned bucket). It could be created using web console or with yc (don't forget to write down your access key and secret key):

    $ yc iam service-account create --name function-sa
    $ yc iam access-key create --service-account-name function-sa
    $ yc resource-manager folder add-access-binding <Folder-Name> \
    --subject serviceAccount:<ServiceAccount-ID> --role serverless.functions.invoker

## Create ClickHouse database

Before we start deploying, we need to create ClickHouse database
and obtain it's connection parameters. Easiest way is to use web-console.

    $ yc managed-clickhouse cluster create  --name billingserver --environment=production  \
    --network-name vsgrabnet --clickhouse-resource-preset s2.micro \
    --host type=clickhouse,zone-id=ru-central1-c,assign-public-ip=true,subnet-id=YYYYYY \
    --clickhouse-disk-size 20 --clickhouse-disk-type network-ssd \
    --user name=user1,password=XXXX --database name=db1

**Don't forget**:
* make your database available using public IP address if you are using web-console (assign-public-ip=true flag in CLI)
* use strong passwords.


## Deploy function

To deploy your function:

    $ yc serverless function create --name billfunc
    $ sh deploy.sh

Used environment variables (inside deploy.sh):

* `CH_HOST` – IP Address;
* `CH_DB` – Database Name;
* `CH_USER` – Database Username;
* `CH_PASSWORD` – Database Password
* `CH_TABLE` – Table Name (function will create it if table not exists);
* `STORAGE_BUCKET` – Storage Bucket;
* `STORAGE_FOLDER` – Folder name inside bucket (usually it is yc-billing-export) ;
* `AWS_ACCESS_KEY_ID` – AWS Access Key
* `AWS_SECRET_ACCESS_KEY` – Aws Access Secret Key

## Initial load

    $ yc serverless function invoke billfunc -d '{"queryStringParameters": {"method": "reload"}}'

## Behaviour
During first run with method = "reload" parameter function creates table `CH_TABLE` in ClickHouse and loads all data from CSV files.

## Create S3 Trigger

After deploying your function you should create trigger for object storage where you store your billing data:
https://cloud.yandex.com/docs/functions/quickstart/os-trigger-quickstart
  
    $ yc serverless trigger create object-storage billfuncs3trigger \
        --description 'Billing-Data-Loader s3 trigger' \
        --invoke-function-name billfunc \
        --invoke-function-service-account-name function-sa  \
        --bucket-id string \
        --prefix yc-billing-export \
        --suffix csv \
        --events 'create-object','update-object'    