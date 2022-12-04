from datetime import datetime
from typing import Optional

from awsglue import DynamicFrame
from awsglue.context import GlueContext

DATABASE_NAME = "mydb"
LOAN_TABLE_NAME = "Loans"
ACCOUNT_TABLE_NAME = "Account"
CLIENT_TABLE_NAME = "Client"
BRANCH_TABLE_NAME = "Branch"
BANK_TABLE_NAME = "Bank"


def get_table(
    context: GlueContext,
    table: str,
    push_down_predicate: Optional[str] = None,
    database: Optional[str] = None
) -> DynamicFrame:
    if not database:
        database = DATABASE_NAME
    return context.create_dynamic_frame_from_catalog(
        database=database,
        table_name=table,
        transformation_ctx=f"{database}-{table}-transform-ctx",
        push_down_predicate=push_down_predicate
    )


def get_loan_df_in_range(
    context: GlueContext,
    start_date: datetime,
    end_date: datetime
) -> DynamicFrame:
    return get_table(
        context,
        LOAN_TABLE_NAME,
        f"""
        loan_date BETWEEN
        {start_date.strftime("%Y-%m-%d")} AND {end_date.strftime("%Y-%m-%d")}
        """
    )


def get_account_df(context: GlueContext):
    return get_table(
        context,
        ACCOUNT_TABLE_NAME
    )


def get_client_df(context: GlueContext):
    return get_table(
        context,
        CLIENT_TABLE_NAME
    )


def get_branch_df(context: GlueContext):
    return get_table(
        context,
        BRANCH_TABLE_NAME
    )


def get_bank_df(context: GlueContext):
    return get_table(
        context,
        BANK_TABLE_NAME
    )
