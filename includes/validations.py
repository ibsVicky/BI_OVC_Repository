import pyspark.sql.functions as F
from datetime import datetime

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

"""********************************************************************"""
def get_object_validation(process_run_id, object_validation_type):
    try:
        query = ""
        df = []
        
        query += "SELECT object_validation_id, object_name, validation_rule, validation_rule_detail, reject_all"
        query += " FROM `latam-md-finance`.control.log_process_run run"
        query += " INNER JOIN `latam-md-finance`.control.object_validation val ON val.fk_process_setup_id = run.fk_process_setup_id"
        query += " WHERE run.process_run_id = " + str(process_run_id)
        query += " AND val.object_validation_type = '" + object_validation_type + "'"
        query += " AND val.sys_status_code = 'A'"

        df = spark.sql(query)
        return df

    except Exception as e:
        print(f"❌ ERROR RETRIEVING VALIDATION LIST, PROCESS RUN ID {process_run_id} : {e}")
    return False

"""********************************************************************"""
def build_validation_condition(rule_type,col_name,rule_detail):

    if rule_type == 'date_format':
        condition = F.expr(f"try_to_date({col_name}, '{rule_detail}') is not null") | F.col(col_name).isNull()
    elif rule_type == 'numeric_format':
        condition = F.expr(f"try_cast({col_name} as {rule_detail}) is not null") | F.col(col_name).isNull()
    elif rule_type == 'out_of_range':
        condition = F.expr(f"{col_name} {rule_detail}") | F.col(col_name).isNull()
    elif rule_type == 'not_null':
        condition = F.col(col_name).isNotNull()
    else:
        return F.lit(True)
    
    return condition


def physical_validation(df_data, df_validations):

    #Validations who reject all the extract
    df_validations_reject_all = df_validations.filter(F.col("reject_all") == 1).collect()

    if df_validations_reject_all:

        all_pass_condition = F.lit(True)

        for validation in df_validations_reject_all:
            col_name = validation['object_name']
            rule_type = validation['validation_rule']
            rule_detail = validation['validation_rule_detail']
            
            #generate the code for each validation
            reject_all_cond = build_validation_condition(rule_type,col_name,rule_detail)
            
            #in reject_all = 1, all conditions must be true, so combine them using logical AND
            all_pass_condition = all_pass_condition & reject_all_cond

        #If any fail, everything is rejected
        if df_data.filter(~all_pass_condition).count() > 0:
            
            df_validated = df_data.limit(0)  # return empty df
            df_rejected = df_data  #return all rejected
            
            return df_validated, df_rejected

    #if not rejected all, then continue with record validation
    df_validations_record = df_validations.filter(F.col("reject_all") != 1).collect()
    reject_record_cond = []

    for validation in df_validations_record:
        col_name = validation['object_name']
        rule_type = validation['validation_rule']
        rule_detail = validation['validation_rule_detail']
        
        reject_record_cond.append(build_validation_condition(rule_type,col_name,rule_detail))

    if reject_record_cond:
        # combine all conditions in one
        all_condition = reject_record_cond[0]

        for cond in reject_record_cond[1:]:
            all_condition = all_condition & cond
        
        #get all records pass the validation
        df_validated = df_data.filter(all_condition)

        #get all records not pass the validation
        df_rejected = df_data.filter(~all_condition)
    else:
        # if nothing to validate
        df_validated = df_data
        df_rejected = df_data.limit(0)

    return df_validated, df_rejected

"""********************************************************************"""
def business_validation(df_data, df_validations, table_to_validate, validation_fields_list):






