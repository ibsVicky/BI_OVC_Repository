import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql import DataFrame
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
        raise

"""********************************************************************"""
def object_validation_error_insert(df_errors, process_run_id):
    try:
        err_rows = df_errors.select(
            "rejection_reasons",
            "error_value",
            "dw_row_id"
        ).distinct().collect()  #to filter duplicates

        query = ""
        df = []

        first = True

        query += "INSERT INTO `latam-md-finance`.control.object_validation_error (fk_process_run_id, fk_object_validation_id, fk_dw_row_id,object_validation_error_value) VALUES"

        for row in err_rows:
            rejection_reason = row['rejection_reasons']
            error_value = row['error_value']
            row_id = row['dw_row_id']
            
            if first:
                query += f"({process_run_id}, {rejection_reason}, '{row_id}', '{error_value}')"
                first = False
            else:
                query += f" ,({process_run_id}, {rejection_reason}, '{row_id}', '{error_value}')"

        spark.sql(query)
        return True

    except Exception as e:
        print(f"❌ ERROR IN object_validation_error_insert INSERTING IN control.object_validation_error: {e}")
        raise


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


"""********************************************************************"""

def physical_validation(df_data, df_validations, process_run_id):
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
            rejected_records = []

            for validation_id, cond in failed_conditions:
                # If at least one record fails, all records are rejected
                df_fail = df_data.filter(~cond)
                if df_fail.count() > 0:
                    failed_ids.append(validation_id)
                
                    # to insert in log error table
                    rejected_records.append(
                        df_fail.select(
                            F.lit(validation_id).alias("rejection_reasons"),
                            F.col(col_name).alias("error_value"),
                            F.col("DW_ROW_ID").alias("dw_row_id")
                        ).distinct()
                    )

            df_validated = df_data.limit(0) # return empty df
            df_rejected = df_data.withColumn(
                "rejection_reason",
                F.lit(",".join(failed_ids))
            ) #return all rejected

            #Log individual rejected values in object_validation_error table
            try:
                for df in rejected_records: #use for, because rejected_records is a list of dataframes
                    if df.count() > 0:
                        object_validation_error_insert(df,process_run_id)
            except Exception as e:
                print(f"❌ ERROR INSERTING IN object_validation_error TABLE: {e}")
                raise

            return df_validated, df_rejected

    #if not rejected all, then continue with record validation
    rejected_records = []
    df_validations_record = df_validations.filter(F.col("reject_all") != 1).collect()

    if not df_validations_record:
        return df_data, df_data.limit(0)

    # Create empty column for rejection reason in case of errors
    df_data = df_data.withColumn("rejection_reason", F.lit(""))

    # Applies each validation inidvidually and concatenate the failing IDs
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

        # to insert in log error table
        df_failed_current = df_data.filter(~cond).select(
            F.lit(object_validation_id).alias("rejection_reasons"),
            F.col(col_name).alias("error_value"),
            F.col("DW_ROW_ID").alias("dw_row_id")
        )

        rejected_records.append(df_failed_current)

    #If there are errors
    if rejected_records:
        #Log individual rejected values in object_validation_error table
        try:  
            for df in rejected_records: #use for, because rejected_records is a list of dataframes
                if df.count() > 0:
                    object_validation_error_insert(df,process_run_id)
        except Exception as e:
            print(f"❌ ERROR INSERTING IN object_validation_error TABLE: {e}")
            raise

    # Divide entre validados y rechazados
    df_rejected = df_data.filter(F.col("rejection_reason") != "")
    df_validated = df_data.filter(F.col("rejection_reason") == "")

    return df_validated, df_rejected

"""******************************************************************************"""

def business_validation(df_data, df_validations, process_run_id):
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
            #query += " WHERE sys_status_code = 'A'" uncomment later ahora está así porque no le puse el campo a las tablas master de prueba

            df_master = spark.sql(query)

            # Rename only columns used in validation to avoid conflicts
            df_master_renamed = df_master
            for _, trgt_col in rule_detail:
                if trgt_col in df_master.columns:
                    df_master_renamed = df_master_renamed.withColumnRenamed(trgt_col, f"{trgt_col}_master")

        except Exception as e:
            print(f"❌ ERROR TABLE NOT FOUND {master_name}: {e}")
            df_invalid = df_data.withColumn(
                "rejection_reasons",
                F.lit(object_validation_id)
            )
            rejected_records.append(df_invalid)
            continue

        #Build join expression with values from rule details
        join_expr = None
        error_cols = [] #List of rows with UNSP, TBD
        error_actual_cols = []  #List with real values of rows with UNSP, TBD
        for src_col, trgt_col in rule_detail:
            # Use only columns that exist in join DataFrames to avoid Spark errors
            cond = src_data[src_col] == df_master_renamed[f"{trgt_col}_master"]

            if join_expr is None:
                join_expr = cond
            else:
                join_expr = join_expr & cond

            #Determined UNSP values or TBD values
            error_col = F.when(src_data[src_col] == "UNSP", "UNSP").otherwise("TBD")
            error_cols.append(error_col)

            # Real values for login error table
            error_actual_cols.append(src_data[src_col])

        #Apply Validation & add rejection reason column
        #Perform join using src_data (fillna) and master renamed; select original df_data columns after join
        df_invalid_temp = src_data.join(
            df_master_renamed,
            on=join_expr,
            how="left_anti"
        )
        df_invalid = df_invalid_temp.select(*df_data.columns)\
            .withColumn("rejection_reasons", F.lit(object_validation_id))\
            .withColumn("error_value", F.concat_ws("||", *error_actual_cols))\
            .withColumn("error_value_final", F.concat_ws("||", *error_cols))  # <-- UNSP/TBD para df_rejected

        rejected_records.append(df_invalid)

    #If there are errors
    if rejected_records:
        #first log individual rejected values in object_validation_error table
        try:
            for df in rejected_records: #use for, because rejected_records is a list of dataframes
                #Insert error_value from extract in object_validation_error_insert
                if df.count() > 0:
                    object_validation_error_insert(
                        df.select(*df_data.columns, "rejection_reasons", "error_value"),
                        process_run_id
                    )

        except Exception as e:
            print(f"❌ ERROR INSERTING IN object_validation_error TABLE: {e}")
            raise
        
        #then in case of multiple errors for same record, return only one with all errors in rejection_reasons concatenated
        df_rejected = rejected_records[0]
        for df in rejected_records[1:]:
            df_rejected = df_rejected.unionByName(df)

        df_rejected = df_rejected.groupBy(*src_data.columns).agg(
            F.concat_ws(",", F.collect_list("rejection_reasons")).alias("rejection_reasons"),
            F.concat_ws("||", F.collect_list("error_value_final")).alias("error_value")  # <-- UNPS/TBD for df final
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


"""******************************************************************************"""
