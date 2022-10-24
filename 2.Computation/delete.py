from google.cloud import bigquery
import sys

# Construct a BigQuery client object.
client = bigquery.Client()

param = ''
if len(sys.argv) > 1:
	param = sys.argv[1]

def truncate(table):
	dml_statement = ("TRUNCATE TABLE db1_poc1."+table)
	query_job = client.query(dml_statement)  # API request
	query_job.result()  # Waits for statement to finish
	print("Truncated table db1_poc1."+table)

if param == '':
	for x in ["departments","jobs","hired_employees"]:
		truncate(x)
else:
	truncate(param)
