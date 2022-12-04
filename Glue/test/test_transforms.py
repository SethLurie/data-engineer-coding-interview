from datetime import datetime

import pytest
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, IntegerType, StringType, StructField,
                               StructType, FloatType)

from Glue.utils.fields import FK, PK, Field
from Glue.utils.transforms import get_avg_loans_per_branch, with_bank


@pytest.fixture
def spark_session(request):
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    # to teardown the session after test
    request.addfinalizer(lambda: spark.stop())
    return spark


@pytest.fixture
def glue_context():
    return GlueContext(SparkContext.getOrCreate())


@pytest.fixture
def loan_schema():
    return StructType(
        [
            StructField(PK.LOAN.value, IntegerType(), False),
            StructField(Field.AMOUNT.value, IntegerType(), False),
            StructField(FK.ACCOUNT.value, IntegerType(), False),
            StructField(Field.LOAN_DATE.value, DateType(), False)
        ]
    )


@pytest.fixture
def loan_df(spark_session, loan_schema, glue_context):
    date = datetime.strptime("2022-04-01", "%Y-%m-%d")
    return DynamicFrame.fromDF(spark_session.createDataFrame(
        [
            (1, 60, 1, date),
            (2, 200, 1, date),
            (3, 20, 2, date),
            (4, 100, 3, date)
        ],
        loan_schema
    ), glue_context, "loan")


@pytest.fixture
def account_schema():
    return StructType(
        [
            StructField(PK.ACCOUNT.value, IntegerType(), False),
            StructField(Field.BALANCE.value, IntegerType(), False),
            StructField(FK.CLIENT.value, IntegerType(), False),
            StructField(Field.OPEN_DATE.value, DateType(), False)
        ]
    )


@pytest.fixture
def account_df(spark_session, account_schema, glue_context):
    date = datetime.strptime("2021-04-01", "%Y-%m-%d")
    return DynamicFrame.fromDF(spark_session.createDataFrame(
        [
            (1, 1000, 1, date),
            (2, 300, 2, date),
            (3, 10, 3, date),
        ],
        account_schema
    ), glue_context, "account")


@pytest.fixture
def client_schema():
    return StructType(
        [
            StructField(PK.CLIENT.value, IntegerType(), False),
            StructField(Field.NAME.value, StringType(), False),
            StructField(Field.SURNAME.value, StringType(), False),
            StructField(FK.BRANCH.value, IntegerType(), False)
        ]
    )


@pytest.fixture
def client_df(spark_session, client_schema, glue_context):
    return DynamicFrame.fromDF(spark_session.createDataFrame(
        [
            (1, "Seth", "Lurie", 1),
            (2, "MJ", "London", 2),
            (3, "Homer", "Simposon", 2),
        ],
        client_schema
    ), glue_context, "client")


@pytest.fixture
def branch_schema():
    return StructType(
        [
            StructField(PK.BRANCH.value, IntegerType(), False),
            StructField(FK.BANK.value, IntegerType(), False),
            StructField(Field.ADDRESS.value, StringType(), False),
        ]
    )


@pytest.fixture
def branch_df(spark_session, branch_schema, glue_context):
    return DynamicFrame.fromDF(spark_session.createDataFrame(
        [
            (1, 1, "Middleton"),
            (2, 1, "Upton")
        ],
        branch_schema
    ), glue_context, "branch")


@pytest.fixture
def result_loan_schema():
    return StructType(
        [
            StructField(PK.BRANCH.value, IntegerType(), False),
            StructField("avg_loan_amount", FloatType(), False),
            StructField(FK.BANK.value, IntegerType(), False),
        ]
    )


@pytest.fixture
def bank_schema():
    return StructType(
        [
            StructField(PK.BANK.value, IntegerType(), False),
            StructField(Field.NAME.value, StringType(), False),
            StructField(Field.CAPITALIZATION.value, IntegerType(), False),
        ]
    )


@pytest.fixture
def bank_df(spark_session, bank_schema, glue_context):
    return spark_session.createDataFrame(
        [
            (1, "Middleton", 0),
        ],
        bank_schema
    )


class TestAvgLoansPerBranchPerBank:
    def test_get_avg_loans_per_branch(
        self,
        loan_df,
        account_df,
        client_df,
        branch_df,
        bank_df,
        result_loan_schema,
        spark_session
    ):
        loan_per_branch = get_avg_loans_per_branch(
            loan_df, account_df, client_df, branch_df
        )
        # First 2 loans are for branch 1 with average 130
        # Last 2 loans are for branch 2 with average 60
        expected_df = spark_session.createDataFrame(
            [
                (1, 130.0, 1),
                (2, 60.0, 1)
            ],
            result_loan_schema
        )
        assert (
            sorted(expected_df.collect()) == sorted(loan_per_branch.collect())
        )

        loan_per_branch_with_bank = with_bank(loan_per_branch, bank_df)

        expected_df_with_bank = spark_session.createDataFrame(
            [
                (1, 130.0, 1, "Middleton"),
                (2, 60.0, 1, "Middleton")
            ],
            result_loan_schema.add(
                StructField(Field.NAME.value, StringType(), False)
            )
        )

        assert (
            sorted(expected_df_with_bank.collect()) ==
            sorted(loan_per_branch_with_bank.collect())
        )
