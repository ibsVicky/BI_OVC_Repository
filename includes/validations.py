import pyspark.sql.functions as F
from datetime import datetime

import ast

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

"""********************************************************************"""
def get_object_validation(process_run_id, object_validation_type):
    try:
        query = ""
        df = []
        
        query += "SELECT object_validation_id, object_name, validation_rule_code, validation_rule_detail, reject_all,master_table_name"
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

    # Validations who reject all the extract
    df_validations_reject_all = df_validations.filter(F.col("reject_all") == 1).collect()

    if df_validations_reject_all:

        all_pass_condition = F.lit(True)
        failed_conditions = []

        for validation in df_validations_reject_all:
            col_name = validation['object_name']
            rule_type = validation['validation_rule_code']
            rule_detail = validation['validation_rule_detail']
            object_validation_id = str(validation['object_validation_id'])

            # generate the code for each validation
            cond = build_validation_condition(rule_type, col_name, rule_detail)
            #in reject_all = 1, all conditions must be true, so combine them using logical AND
            all_pass_condition = all_pass_condition & cond

            # Save reference of condition to evaluate those who failed
            failed_conditions.append((object_validation_id, cond))

        # If any fail, everything is rejected
        if df_data.filter(~all_pass_condition).count() > 0:
            failed_ids = []
            for validation_id, cond in failed_conditions:
                # If at least one record fails, all records are rejected
                if df_data.filter(~cond).count() > 0:
                    failed_ids.append(validation_id)

            df_validated = df_data.limit(0) # return empty df
            df_rejected = df_data.withColumn(
                "rejection_reason",
                F.lit(",".join(failed_ids))
            ) #return all rejected

            return df_validated, df_rejected

    #if not rejected all, then continue with record validation
    df_validations_record = df_validations.filter(F.col("reject_all") != 1).collect()

    if not df_validations_record:
        return df_data, df_data.limit(0)

    # Create empty column for rejection reason in case of errors
    df_data = df_data.withColumn("rejection_reason", F.lit(""))

    # Aplica cada validación individualmente y concatena los IDs que fallan
    for validation in df_validations_record:
        object_validation_id = str(validation['object_validation_id'])
        col_name = validation['object_name']
        rule_type = validation['validation_rule_code']
        rule_detail = validation['validation_rule_detail']

        cond = build_validation_condition(rule_type, col_name, rule_detail)

        df_data = df_data.withColumn(
            "rejection_reason",
            F.when(~cond,
                   F.when(F.col("rejection_reason") == "",
                          F.lit(object_validation_id))
                   .otherwise(F.concat_ws(",", F.col("rejection_reason"), F.lit(object_validation_id)))
                   ).otherwise(F.col("rejection_reason"))
        )

    # Divide entre validados y rechazados
    df_rejected = df_data.filter(F.col("rejection_reason") != "")
    df_validated = df_data.filter(F.col("rejection_reason") == "")

    return df_validated, df_rejected

"""********************************************************************"""

def business_validation(df_data: DataFrame, df_validations: DataFrame):
    # Replace NULL with "UNSP" to avoid null issues
    src_data = df_data.fillna("UNSP")
    rejected_records = []

    list_validations = df_validations.collect()

    for validation in list_validations:
        object_validation_id = validation["object_validation_id"]
        master_name = validation["master_table_name"]
        rule_code = validation["validation_rule_code"]
        rule_detail = ast.literal_eval(validation["validation_rule_detail"])  # list of tuples: [(src_key1, trgt_key1), (src_key2, trgt_key2),...]

        try:
            #Get records from master table
            query = "SELECT * "
            query += " FROM " + master_name
            #query += " WHERE sys_status_code = 'A'" uncomment later

            df_master = spark.sql(query)
        except Exception as e:
            print(f"❌ ERROR TABLE NOT FOUND {master_name}: {e}")
            df_invalid = src_data.withColumn(
                "rejection_reasons",
                F.lit(object_validation_id)
            )
            rejected_records.append(df_invalid)
            continue

        #Build join expression with values from rule details
        join_expr = None
        for src_col, trgt_col in rule_detail:
            cond = src_data[src_col] == df_master[trgt_col]

            if join_expr is None:
                join_expr = cond
            else:
                join_expr = join_expr & cond

        #Apply Validation & add rejection reason column
        df_invalid = src_data.join(
                            df_master, 
                            on=join_expr, 
                            how="left_anti"
                        ) \
        .withColumn("rejection_reasons", F.lit(object_validation_id)) #This is the error id

        rejected_records.append(df_invalid)

    #In case of multiple errors for same record, return only one with all errors in rejection_reasons concatenated
    if rejected_records:
        df_rejected = rejected_records[0]
        for df in rejected_records[1:]:
            df_rejected = df_rejected.unionByName(df)

        df_rejected = df_rejected.groupBy(*src_data.columns).agg(
            F.concat_ws(",", F.collect_list("rejection_reasons")).alias("rejection_reasons")
        )
    else:
        df_rejected = src_data.limit(0).withColumn("rejection_reasons", F.array())

    #Calculate validations using hash to avoid null values issues as null != null
    cols = src_data.columns #to match only columns that are in src_data, as rejection_reasons is not
    df_data_hashed = src_data.withColumn("row_hash", F.sha2(F.concat_ws("||", *cols), 256)) #256 is the hash algorithm SHA-256
    df_rejected_hashed = df_rejected.withColumn("row_hash", F.sha2(F.concat_ws("||", *cols), 256)) #256 is the hash algorithm SHA-256

    df_validated = df_data_hashed.join(
                        df_rejected_hashed.select("row_hash"), 
                        on="row_hash", 
                        how="left_anti"
                    ).drop("row_hash", "rejection_reason")

    return df_validated, df_rejected


"""********************************************************************"""