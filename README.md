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

    $ yc managed-clickhouse cluster create --name billingserver --environment=production  \
        --network-name VPC_NAME --clickhouse-resource-preset s2.small \
        --host type=clickhouse,zone-id=ru-central1-c,assign-public-ip=true,subnet-id=SUBNET_C_ID \
        --host type=clickhouse,zone-id=ru-central1-a,assign-public-ip=true,subnet-id=SUBNET_A_ID \
        --host type=zookeeper,zone-id=ru-central1-c,subnet-id=SUBNET_C_ID \
        --host type=zookeeper,zone-id=ru-central1-b,subnet-id=SUBNET_B_ID \
        --host type=zookeeper,zone-id=ru-central1-a,subnet-id=SUBNET_A_ID \
        --clickhouse-disk-size 100 --clickhouse-disk-type network-ssd \
        --user name=user1,password=pass@word12 --database name=db1 --serverless-access
        
    $ yc clickhouse user grant-permission user1 --cluster-name billingserver --database db1

**Don't forget**:
* use strong passwords


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

## Several Billing Accounts
If you have several clouds inside several billing accounts - you can combine billing data between them inside same ClickHouse cluster. 
For each billing account use separate function instance and separate target table. You can combine them with such view:

    create view db1.bill_all on cluster '{cluster}'
    as
    SELECT billing_account_id, billing_account_name, cloud_id, cloud_name, folder_id, folder_name, service_id, service_name, sku_id, sku_name, `date`, currency, pricing_quantity, pricing_unit, cost, credit, monetary_grant_credit, volume_incentive_credit, cud_credit, misc_credit, locale, updated_at, exported_at
    from db1.billing ba 
    union all
    SELECT billing_account_id, billing_account_name, cloud_id, cloud_name, folder_id, folder_name, service_id, service_name, sku_id, sku_name, `date`, currency, pricing_quantity, pricing_unit, cost, credit, monetary_grant_credit, volume_incentive_credit, cud_credit, misc_credit, locale, updated_at, exported_at
    from db1.billing1 ba1
            
