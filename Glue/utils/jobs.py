from datetime import datetime, timedelta

from awsglue.context import GlueContext
from boto3 import client
from pyspark.sql.functions import col

from Glue.utils.dynamic_source_frames import (get_account_df, get_bank_df,
                                              get_branch_df, get_client_df,
                                              get_loan_df_in_range)
from Glue.utils.fields import FK, PK
from Glue.utils.transforms import (get_avg_loans_per_branch, with_bank,
                                   with_year_month)


def write_avg_loans_per_branch_per_bank_to_s3(
    context: GlueContext, bucket_name: str
):
    """
    Writes the everage loan amount per branch per bank to an s3 file named:
    BankName_YYYYMMDD.csv
    """
    now = datetime.now()
    # for the sake of idempotency, this function runs using:
    # * last month end day as the end of the period considered
    # * 4 months ago start day as the start of the window
    start_date = now.replace(
        month=now.month - 4,
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0
        )
    # go to first of the current month
    end_date = now.replace(
        day=1
    )
    # minus one day is the end of the previous month
    end_date = end_date - timedelta(days=1)
    end_date = end_date.replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=99999
    )
    # Get all the source dynamic frames
    loans_df = get_loan_df_in_range(
        context, start_date, end_date
    )

    account_df = get_account_df(context).select_fields(
        [PK.ACCOUNT.value, FK.CLIENT.value]
    )

    client_df = get_client_df(context).select_fields(
        [PK.CLIENT.value, FK.BRANCH.value]
    )

    branch_df = get_branch_df(context).select_fields(
        [PK.BRANCH.value, FK.BANK.value]
    )

    bank_df = get_bank_df(context)

    # transforms
    loans_per_branch = get_avg_loans_per_branch(
        loans_df=loans_df,
        account_df=account_df,
        client_df=client_df,
        branch_df=branch_df
    )

    loans_per_branch = with_bank(
        loans_per_branch,
        bank_df.toDF()
    )

    loans_per_branch = with_year_month(
        loans_per_branch,
        end_date.year,
        end_date.month
    )

    partitioned_loans = loans_per_branch.repartition(1)  # single file

    end_date_year = end_date.year
    end_date_month = end_date.month
    source = f"{bucket_name}/year={end_date_year}/month={end_date_month}"
    partitioned_loans.write.option(
        "header", True
    ).partitionBy(  # 1 file per bank
        col(PK.BANK)
    ).csv(f"s3://{source}")

    # file renaming in s3
    s3 = client("s3")
    resp = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=f"year={end_date_year}/month={end_date_month}"
    ).get("Contents", [])

    for obj in resp:
        file_key: str = obj.get("Key")
        bank_name = file_key.split("/")[-2].replace("Bank=", "")
        file_date = end_date.strftime("%Y%m%d")
        copy_to_file_name = f"{bank_name}_{file_date}.csv"
        s3.copy_object(
            Bucket=bucket_name,
            CopySource=f"{bucket_name}/{file_key}",
            Key=f"year={end_date_year}/month={end_date_month}/Bank={bank_name}/{copy_to_file_name}"  # noqa
        )
        s3.delete_object(
            Bucket=bucket_name,
            Key=file_key
        )
