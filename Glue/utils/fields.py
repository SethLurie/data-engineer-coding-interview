from enum import Enum


def pk(inp: str) -> str:
    return f"id{inp}"


def fk(inp: str) -> str:
    return f"{inp}_id{inp}"


class Field(Enum):
    AMOUNT = "Amount"
    BALANCE = "Balance"
    LOAN_DATE = "loan_date"
    OPEN_DATE = "open_date"
    CAPITALIZATION = "Capitalization"
    NAME = "Name"
    SURNAME = "Surname"
    ADDRESS = "Address"


class PK(Enum):
    LOAN = pk("Loan")
    ACCOUNT = pk("Account")
    CLIENT = pk("Client")
    BRANCH = pk("Branch")
    BANK = pk("Bank")


class FK(Enum):
    ACCOUNT = fk("Account")
    CLIENT = fk("Client")
    BRANCH = fk("Branch")
    BANK = fk("Bank")
