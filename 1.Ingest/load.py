from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

###################
#DEPARTMENTS TABLE#
###################
# TODO(developer): Set table_id to the ID of the table to create.
table_id1 = "exemplary-fiber-366315.db1_poc1.departments"

job_config1 = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("department", "STRING"),
    ],
    skip_leading_rows=0,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri1 = "gs://bucket_poc1_797510974348/departments.csv"

load_job1 = client.load_table_from_uri(
    uri1, table_id1, job_config=job_config1
)  # Make an API request.

load_job1.result()  # Waits for the job to complete.

destination_table1 = client.get_table(table_id1)  # Make an API request.
print("Loaded {} rows on departments table".format(destination_table1.num_rows))

############
#JOBS TABLE#
############
# TODO(developer): Set table_id to the ID of the table to create.
table_id2 = "exemplary-fiber-366315.db1_poc1.jobs"

job_config2 = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("job", "STRING"),
    ],
    skip_leading_rows=0,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri2 = "gs://bucket_poc1_797510974348/jobs.csv"

load_job2 = client.load_table_from_uri(
    uri2, table_id2, job_config=job_config2
)  # Make an API request.

load_job2.result()  # Waits for the job to complete.

destination_table2 = client.get_table(table_id2)  # Make an API request.
print("Loaded {} rows on jobs table".format(destination_table2.num_rows))

#######################
#HIRED EMPLOYEES TABLE#
#######################
# TODO(developer): Set table_id to the ID of the table to create.
table_id3 = "exemplary-fiber-366315.db1_poc1.hired_employees"

job_config3 = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
		bigquery.SchemaField("datetime", "STRING"),
		bigquery.SchemaField("department_id", "INTEGER"),
		bigquery.SchemaField("job_id", "INTEGER"),
    ],
    skip_leading_rows=0,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri3 = "gs://bucket_poc1_797510974348/hired_employees.csv"

load_job3 = client.load_table_from_uri(
    uri3, table_id3, job_config=job_config3
)  # Make an API request.

load_job3.result()  # Waits for the job to complete.

destination_table3 = client.get_table(table_id3)  # Make an API request.
print("Loaded {} rows on hired_employees table".format(destination_table3.num_rows))