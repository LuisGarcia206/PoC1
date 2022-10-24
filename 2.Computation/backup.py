from google.cloud import bigquery
import datetime as dt
mytime = dt.datetime.now().strftime("%H:%M:%S")
mydate = dt.date.today()

# Construct a BigQuery client object.
client = bigquery.Client()
bucket_name = 'bucket_poc1_797510974348'
project = "exemplary-fiber-366315"
dataset_id = "db1_poc1"

###################
#DEPARTMENTS TABLE#
###################
table_id1 = "departments"

destination_uri1 = "gs://{}/Backups/{}_{}_{}.avro".format(bucket_name,table_id1,mydate,mytime)
dataset_ref1 = bigquery.DatasetReference(project, dataset_id)
table_ref1 = dataset_ref1.table(table_id1)

job_config1 = bigquery.job.ExtractJobConfig()
job_config1.destination_format = bigquery.DestinationFormat.AVRO

extract_job1 = client.extract_table(
    table_ref1,
    destination_uri1,
    # Location must match that of the source table.
	job_config=job_config1,
    location="US",
)  # API request
extract_job1.result()  # Waits for job to complete.

print(
    "Exported {}:{}.{} to {}".format(project, dataset_id, table_id1, destination_uri1)
)

############
#JOBS TABLE#
############
table_id2 = "jobs"

destination_uri2 = "gs://{}/Backups/{}_{}_{}.avro".format(bucket_name,table_id2,mydate,mytime)
dataset_ref2 = bigquery.DatasetReference(project, dataset_id)
table_ref2 = dataset_ref2.table(table_id2)

job_config2 = bigquery.job.ExtractJobConfig()
job_config2.destination_format = bigquery.DestinationFormat.AVRO

extract_job2 = client.extract_table(
    table_ref2,
    destination_uri2,
    # Location must match that of the source table.
	job_config=job_config2,
    location="US",
)  # API request
extract_job2.result()  # Waits for job to complete.

print(
    "Exported {}:{}.{} to {}".format(project, dataset_id, table_id2, destination_uri2)
)

#######################
#HIRED EMPLOYEES TABLE#
#######################
table_id3 = "hired_employees"

destination_uri3 = "gs://{}/Backups/{}_{}_{}.avro".format(bucket_name,table_id3,mydate,mytime)
dataset_ref3 = bigquery.DatasetReference(project, dataset_id)
table_ref3 = dataset_ref3.table(table_id3)

job_config3 = bigquery.job.ExtractJobConfig()
job_config3.destination_format = bigquery.DestinationFormat.AVRO

extract_job3 = client.extract_table(
    table_ref3,
    destination_uri3,
    # Location must match that of the source table.
	job_config=job_config3,
    location="US",
)  # API request
extract_job3.result()  # Waits for job to complete.

print(
    "Exported {}:{}.{} to {}".format(project, dataset_id, table_id3, destination_uri3)
)