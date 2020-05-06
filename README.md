# Billing-Data-Loader
Serverless Function to load Yandex.Cloud billing data to ClickHouse

# Prerequisites
Ask Yandex.Cloud support to export your billing data to S3 bucket of your choice

Also you need to have Service Account and access keys to access Object Storage (previously mentioned bucket). It could be created using web console or with yc (don't forget to write down your access key and secret key):


## Create ClickHouse database

Before we start deploying, we need to create ClickHouse database
and obtain it's connection parameters. Easiest way is to use web-console.

**Don't forget**:
* make your database available using public IP address;
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

## Test it

    $ yc serverless function invoke --name billfunc
    {"statusCode": 200, "body": "2 objects loaded", "isBase64Encoded": false}

## Behaviour
During first run functions create table `CH_TABLE` in ClickHouse and loads all data from CSV files.
After first run function calculates maximum date in `CH_TABLE`,
 subtracts 2 days and loads all csv files which are greater or equal than that date.
    
## Create Timer

After deploying your function you can create trigger with timer:
https://cloud.yandex.ru/docs/functions/quickstart/timer-quickstart

For cron expression you can use this template (Trigger runs every 1 hour): 0 * ? * * *    