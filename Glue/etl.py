from awsglue.context import GlueContext
from pyspark.context import SparkContext
from Glue.utils.jobs import write_avg_loans_per_branch_per_bank_to_s3
from awsglue.job import Job


def main():
    context = GlueContext(SparkContext.getOrCreate())
    job = Job(context)
    job.init("WriteAvgLoansPerBranch")
    write_avg_loans_per_branch_per_bank_to_s3(
        context, "banking-group-mydb-reports"
    )
    job.commit()
