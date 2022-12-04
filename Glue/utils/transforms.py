from awsglue import DynamicFrame
from awsglue.transforms import Join
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, first, lit

from Glue.utils.fields import FK, PK, Field


def get_avg_loans_per_branch(
    loans_df: DynamicFrame,
    account_df: DynamicFrame,
    client_df: DynamicFrame,
    branch_df: DynamicFrame
) -> DataFrame:

    # Join all the source dfs together to prepare for aggregation by branch
    loans_df = Join.apply(
        account_df, loans_df, PK.ACCOUNT.value, FK.ACCOUNT.value
    ).drop_fields([FK.ACCOUNT.value])

    loans_df = Join.apply(
        client_df, loans_df, PK.CLIENT.value, FK.CLIENT.value
    ).drop_fields([FK.CLIENT.value])

    loans_df: DynamicFrame = Join.apply(
        branch_df, loans_df, PK.BRANCH.value, FK.BRANCH.value
    ).drop_fields([FK.BRANCH.value])

    loan_dataframe = avg_loan_amount_by_branch(loans_df)

    return loan_dataframe


def avg_loan_amount_by_branch(
    df_with_branch: DynamicFrame
) -> DataFrame:
    dataframe = df_with_branch.toDF()
    # Perform the aggregation per branch
    dataframe = dataframe.groupBy(
        col(PK.BRANCH.value)
    ).agg(
        avg(Field.AMOUNT.value).alias("avg_loan_amount"),
        first(FK.BANK.value).alias(FK.BANK.value)
    )
    return dataframe


def with_bank(
    df_with_bank_fk: DataFrame,
    bank_df: DataFrame
) -> DataFrame:
    return df_with_bank_fk.join(
        bank_df, col(FK.BANK.value) == col(PK.BANK.value)
    ).drop(Field.CAPITALIZATION.value, FK.BANK.value)


def with_year_month(
    df: DataFrame,
    end_year: int,
    end_month: int
) -> DataFrame:
    return df.withColumn(
        "year", lit(end_year)
    ).withColumn(
        "month", lit(end_month)
    )
