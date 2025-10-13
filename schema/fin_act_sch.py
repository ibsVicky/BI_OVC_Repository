# schemas.py
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType

# Diccionario con los schemas
fin_act_schs = {
    "fin-act-raw": StructType([
        StructField("SOURCE_SYSTEM_CODE", StringType(), True),
        StructField("COMPANY_CODE", StringType(), True),
        StructField("POSTING_DATE", StringType(), True),
        StructField("POSTING_DOC_TYPE", StringType(), True),
        StructField("POSTING_DOC_NUMBER", StringType(), True),
        StructField("POSTING_DOC_LINE_NUMBER", StringType(), True),
        StructField("POSTING_TEXT", StringType(), True),
        StructField("POSTING_TEXT_ADDITIONAL", StringType(), True),
        StructField("ORIG_DOC_ENTRY_DATE", StringType(), True),
        StructField("ORIG_DOC_ISSUE_DATE", StringType(), True),
        StructField("REVERSE_POSTING_DOC_NUMBER", StringType(), True),
        StructField("COST_CENTER_CODE", StringType(), True),
        StructField("SAP_ACCOUNT_CODE", StringType(), True),
        StructField("SAP_ACCOUNT_NAME", StringType(), True),
        StructField("OFF_SAP_ACCOUNT_CODE", StringType(), True),
        StructField("OFF_SAP_ACCOUNT_NAME", StringType(), True),
        StructField("TRANS_CURRENCY_CODE", StringType(), True),
        StructField("LOCAL_CURRENCY_CODE", StringType(), True),
        StructField("USER_NAME", StringType(), True),
        StructField("EXPENSE_AMOUNT_LC", StringType(), True),
        StructField("EXPENSE_AMOUNT_TC", StringType(), True),
        StructField("TRANS_UM_CODE", StringType(), True),
        StructField("EXPENSE_QTY_TRANS_UM", StringType(), True),
        StructField("VENDOR_CODE", StringType(), True),
        StructField("VENDOR_NAME", StringType(), True),
        StructField("COST_CENTER_NAME", StringType(), True)
    ]),
    "fin-act-bronze": StructType([
        StructField("SOURCE_SYSTEM_CODE", StringType(), True),
        StructField("COMPANY_CODE", StringType(), True),
        StructField("POSTING_DATE", StringType(), True),
        StructField("POSTING_DOC_TYPE", StringType(), True),
        StructField("POSTING_DOC_NUMBER", StringType(), True),
        StructField("POSTING_DOC_LINE_NUMBER", StringType(), True),
        StructField("POSTING_TEXT", StringType(), True),
        StructField("POSTING_TEXT_ADDITIONAL", StringType(), True),
        StructField("ORIG_DOC_ENTRY_DATE", StringType(), True),
        StructField("ORIG_DOC_ISSUE_DATE", StringType(), True),
        StructField("REVERSE_POSTING_DOC_NUMBER", StringType(), True),
        StructField("COST_CENTER_CODE", StringType(), True),
        StructField("SAP_ACCOUNT_CODE", StringType(), True),
        StructField("SAP_ACCOUNT_NAME", StringType(), True),
        StructField("OFF_SAP_ACCOUNT_CODE", StringType(), True),
        StructField("OFF_SAP_ACCOUNT_NAME", StringType(), True),
        StructField("TRANS_CURRENCY_CODE", StringType(), True),
        StructField("LOCAL_CURRENCY_CODE", StringType(), True),
        StructField("USER_NAME", StringType(), True),
        StructField("EXPENSE_AMOUNT_LC", StringType(), True),
        StructField("EXPENSE_AMOUNT_TC", StringType(), True),
        StructField("TRANS_UM_CODE", StringType(), True),
        StructField("EXPENSE_QTY_TRANS_UM", StringType(), True),
        StructField("VENDOR_CODE", StringType(), True),
        StructField("VENDOR_NAME", StringType(), True),
        StructField("COST_CENTER_NAME", StringType(), True),
        StructField("DW_FILE_NAME", StringType(), False)
    ]),
    "fin-act-silver": StructType([
        StructField("SOURCE_SYSTEM_CODE", StringType(), False),
        StructField("COMPANY_CODE", StringType(), False),
        StructField("POSTING_DATE", DateType(), False),
        StructField("POSTING_DOC_TYPE", StringType(), True),
        StructField("POSTING_DOC_NUMBER", StringType(), True),
        StructField("POSTING_DOC_LINE_NUMBER", StringType(), True),
        StructField("POSTING_TEXT", StringType(), True),
        StructField("POSTING_TEXT_ADDITIONAL", StringType(), True),
        StructField("ORIG_DOC_ENTRY_DATE", DateType(), True),
        StructField("ORIG_DOC_ISSUE_DATE", DateType(), True),
        StructField("REVERSE_POSTING_DOC_NUMBER", StringType(), True),
        StructField("COST_CENTER_CODE", StringType(), True),
        StructField("SAP_ACCOUNT_CODE", StringType(), True),
        StructField("SAP_ACCOUNT_NAME", StringType(), True),
        StructField("OFF_SAP_ACCOUNT_CODE", StringType(), True),
        StructField("OFF_SAP_ACCOUNT_NAME", StringType(), True),
        StructField("TRANS_CURRENCY_CODE", StringType(), False),
        StructField("LOCAL_CURRENCY_CODE", StringType(), False),
        StructField("USER_NAME", StringType(), True),
        StructField("EXPENSE_AMOUNT_LC", DecimalType(25,5), True),
        StructField("EXPENSE_AMOUNT_TC", DecimalType(25,5), True),
        StructField("TRANS_UM_CODE", StringType(), True),
        StructField("EXPENSE_QTY_TRANS_UM", DecimalType(15,5), True),
        StructField("VENDOR_CODE", StringType(), True),
        StructField("VENDOR_NAME", StringType(), True),
        StructField("COST_CENTER_NAME", StringType(), True),
        StructField("DW_FILE_NAME", StringType(), False)
    ])
}

def get_schema(schema_name: str):
    """
    Devuelve el StructType correspondiente al nombre del schema.
    """
    schema = fin_act_schs.get(schema_name)
    if schema is None:
        raise ValueError(f"Schema '{schema_name}' no está definido.")
    return schema