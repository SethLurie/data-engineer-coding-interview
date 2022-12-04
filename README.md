# senior-data-engineer-coding-interview

## Data model
![](DataModel_ERD.png)

## Assumptions
* The entire project is run inside the same vpc. As such, no considerations we made to speed up the flow of data over the network or to add encryptio in transit to services that support it
* The timezone used for all aspects of the system is gmt. This means that a month is from 00:00:00 +0 (GMT) on the first day of the month to 23:59:59 +0 (GMT) in the last day of the month
* Secrets for the db username and password already exist in the aws secrets manager service and the rotation of these secrets is handled by the system already in place

## Project architecture discussion
* A glue crawler was connected to the datastore to put metadata into the catalog db. The crawler was scheduled to run every month on the first day of the month at 1 minute past midnight
* A glue catalog db was created to store the metadata created by the crawler
* A glue job was created to collect the average loan amount over the last 3 months for each branch. The file was output for each bank to show the average loan amount per month. This file was partitioned by year, month, and bank name. The job was sheduled to be run after the crawler succeeded. The limitation of this is that if the crawler is ever run on demand, a new glue job will also be started when this succeeds. Idempotenecy is ensured by only running the job against months that have completed. It is assumed that there is no bad data entering the system that has a bad `loan_date` in the past. 
* If any glue job fails, an email is sent that describes the failure. This was done using sns topics and cloudwatch events

## Additional features
* The Glue folder containing the etl script is automatically uploaded to s3
* There are 2 s3 buckets: one containing artifacts, and one containging customer data
* All iam policies have been created as part of the terraform infrastucture
* All s3 buckets have been encrypted at rest using kms. In practice I have found setting up these policies to encrypt and decrypt to be very specific, so I suspect that some tweaking might be needed here.
* Local dev for glue etl scripts can be done in one of 2 ways:
  * Connect editer inside of the container used for the `docker-compose` file (just change the entrypoint to `tail -F null` so that the container stays up). This will give you access to all libraries within the container
  * Use `local-requirements.txt` to create a venv. This will contain all you need for intellicode creation

## Testing
* A test suite has been added to the glue job that ensures the business logic is correct without the having the access to prod tables. This can be run using `docker-compose up`
* Further etl file test runs can be done locally also within the docker container. An importovemnt could be made here to change the entrypoint dynamically.
* The terraform part of this project was tested uing `terraform plan`. This method has significant shortcomings, as the output is not guarenteed to do what I think it will do and should be tested in a real environment. The secrets also do not yet exist in my own personal aws environment, and these parts of the test need to be commented out when running this locally.

## Improvements
* The testing and deployment of all resources outlined in this package shoul be done via some automated pipeline. e.g. github actions
* The terraform code could be broken up into modules, and it could have functions to generate random usernames and passwords
* The email template remains untested. I do not think the code I have written will output a readable email; it should just dump the message forwarded to it by the failed task as json
* The code to rename the csv files feels clunky. I am not entirely sure if there is a better way to do this and am open to ideas.
* Integration tests would be quite easy to run if there were access s3. I think this would be quite a useful addition and would be able to test the logic in `job.py`
* S3 has a number of unexplored options, including replication, object versioning and object lock. These can be enabled if needed, but is not needed with the current bussiness logic
* General processing optimisations can be made when a real prod datasource is connected. This may include changing partitioning, setting timeouts and maximum resources used by the glue job.
* Unfortunately, I did not get to complete the sftp file transfer section of this project, but I think provisioning an ec2 instance to do this is overkill. This can be done using fargat or even lambda. The ideal solution here would actually be to use the aws transfer service. I did not get  far with this, and was using this project as a refernce: https://github.com/MrC0ns1st3nt/AWS-Transfer-Service/blob/master 

# Problem Statement

You are consulting on a Banking group's Data Engineering project and need to create a monthly extract of data from their Aurora cluster into a CSV file on S3, to be consumed by an external system.

There are three different banks in the group.

An Amazon Aurora cluster (postgres flavour) houses their Operational Data Store.

Connection details are:
mycluster.cluster-123456789012.us-east-1.rds.amazonaws.com
(Please use the default postgres port to connect)
username: `postgres`
password: `5Y67bg#r#`

Given the following data model:

![](DataModel_ERD.png)

# Perform the following tasks

1. Write code to deploy the following resources via Terraform:

(Put this code in the file `Terraform/main.tf`)

* A Glue Crawler to crawl this datasource
* A Glue Catalog Database to persist the metadata
* A Glue Job which will read data from this datasource and write it to S3
* An S3 bucket to house the transformed dataset
* Any other terraform resources you might require

2. Write a Glue ETL script (use the file `Glue/etl.py`), containing scaleable business logic to calculate the moving average of loan amounts taken out over the last three months, per branch.
   1. Create a separate monthly output file for each bank in the group.
   2. Files need to be partitioned by Bank Name, Year and Month and the filename needs to be of the format BankName_YYYYMMDD.csv
   
3. Remember, bonus points can be earned, if you really want to impress your client, by adding some of the following:
   1. Create an SFTP server on EC2 (using Terraform), which makes the contents of the S3 bucket containing the extracts available to the external system
   2. Build in some form of scheduling and an orchestration layer which includes notifications upon job failure to a support email address (data-support@mybigbank.co.za)
   3. Ensure idempotency of the ETL system
   4. Keep passwords and connection details secure. In fact, passwords should probably be rotated and S3 data should probably be encrypted...
   5. Add comments and logging to your code
   6. Documentation is always nice, especially if you're good at drawing architectual diagrams. Please add any documentation you create to a separate README file.
   7. Anything else we did not specify in the task outline, but which you think will add value to your solution (keep in mind templatisability for reuse of your code by other team members)
