import pyspark.sql.functions as F
from datetime import datetime

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

"""********************************************************************"""

def log_process_run_start(process_setup_name,process_setup_step_name,sys_modified_by_name):
    try:
        #get process setup id
        query = ""
        df = []

        query += "SELECT process_setup_id" 
        query += " FROM `latam-md-finance`.control.sys_process_setup"
        query += " WHERE process_setup_name = '" + process_setup_name + "'"
        query += " AND process_setup_step_name = '" + process_setup_step_name + "'"
        query += " AND sys_status_code = 'A'"

        df = spark.sql(query)
        row = df.first()

        #if no records returns -1
        if row:
            process_setup_id = row["process_setup_id"]
        else:
            process_setup_id = -1

    except Exception as e:
        print(f"❌ ERROR GETTING PROCESS SETUP ID: {e}")
        return False

    try:    
        #get id of last run processed
        query = ""
        df = []

        query += "SELECT process_run_id" 
        query += " FROM `latam-md-finance`.control.log_process_run"
        query += " WHERE fk_process_setup_id = " + str(process_setup_id)
        query += " AND process_run_last_flag = 1"

        df = spark.sql(query)
        row = df.first()

        #if no records returns -1
        if row:
            process_run_id = row["process_run_id"]
        else:
            process_run_id = -1
    
    except Exception as e:
        print(f"❌ ERROR GETTING LAST PROCESS RUN ID: {e}")
        return False

    try:
        #insert new process run
        query = ""
        df = []

        query = "INSERT INTO `latam-md-finance`.control.log_process_run"
        query += "(fk_process_setup_id,"
        query += "process_run_start_date,"
        query += "process_run_last_flag,"
        query += "sys_created_by_name,"
        query += "sys_modified_by_name)"

        query += "VALUES (" + str(process_setup_id) + ","
        query += "'" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "',"
        query += str(1) + ","
        query += "'" + sys_modified_by_name + "',"
        query += "'" + sys_modified_by_name + "'"
        query += ")"

        df = spark.sql(query)
    
    except Exception as e:
        print(f"❌ ERROR INSERTING RUN PROCESS LOG RECORD: {e}")
        return False

    try:
        #SET PREV RUN PROCESSED OFF
        query = ""
        df = []

        query += "UPDATE `latam-md-finance`.control.log_process_run" 
        query += " SET process_run_last_flag = 0"
        query += " ,sys_modified_by_name = '" + sys_modified_by_name + "'"
        query += " ,sys_modified_on = '" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "'"
        query += " WHERE process_run_id = " + str(process_run_id) + ""

        df = spark.sql(query)

    except Exception as e:
        print(f"❌ ERROR UPDATING RUN LAST FLAG OFF: {e}")
        return False
    
    try:
        #return run process id
        query = ""
        df = []

        query += "SELECT process_run_id" 
        query += " FROM `latam-md-finance`.control.log_process_run"
        query += " WHERE fk_process_setup_id = " + str(process_setup_id)
        query += " AND process_run_last_flag = 1"

        df = spark.sql(query)

        return df.select('process_run_id').collect()[0][0]

    except Exception as e:
        print(f"❌ ERROR GETTING LAST PROCESS RUN ID: {e}")
        return False

"""********************************************************************"""

def log_process_run_end(process_run_id,sys_modified_by_name):
    try:
        #SET PREV RUN PROCESSED OFF
        query = ""
        df = []

        query += "UPDATE `latam-md-finance`.control.log_process_run" 
        query += " SET process_run_end_date = '" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "'"
        query += " ,sys_status_code = 'C'"
        query += " ,sys_modified_by_name = '" + sys_modified_by_name + "'"
        query += " ,sys_modified_on = '" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "'"
        query += " WHERE process_run_id = " + str(process_run_id) + ""

        df = spark.sql(query)
        return True
    
    except Exception as e:
        print(f"❌ ERROR UPDATING LOG PROCESS RUN, ID {process_run_id} : {e}")
        return False

"""********************************************************************"""

def get_process_setup_parameters(process_setup_name,process_setup_step_name):

    try:
        query= ""
        df = []

        query = "SELECT "
        query += " process_setup_source_layer,"
        query += " process_setup_source_bucket_name,"
        query += " process_setup_source_bucket_folder_key,"
        query += " process_setup_source_file_name,"
        query += " process_setup_source_file_extension,"
        query += " process_setup_source_file_delimiter,"
        query += " process_setup_source_file_encoding,"
        query += " process_setup_source_file_name_mask,"
        query += " process_setup_source_file_schema,"
        query += " process_setup_source_table_name,"
        query += " process_setup_source_table_schema,"
        query += " process_setup_source_table_catalog,"
        query += " process_setup_source_data_definition,"
        query += " process_setup_target_layer,"
        query += " process_setup_target_table_name,"
        query += " process_setup_target_table_schema,"
        query += " process_setup_target_table_catalog,"
        query += " process_setup_target_data_definition,"
        query += " process_setup_target_bucket_name,"
        query += " process_setup_target_bucket_folder_key,"
        query += " process_setup_archive_bucket_name,"
        query += " process_setup_archive_bucket_folder_key"
        query += " FROM `latam-md-finance`.control.sys_process_setup"
        query += " WHERE process_setup_name = '" + process_setup_name + "'"
        query += " AND process_setup_step_name = '" + process_setup_step_name + "'"
        query += " AND sys_status_code = 'A'"

        df = spark.sql(query)
        return df
    
    except Exception as e:
        print(f"❌ ERROR GETTING DATA FROM sys_process_setup: {e}")
        return False

"""********************************************************************"""

def log_process_run_update_value(process_run_id,sys_modified_by_name,process_run_column_name, process_run_column_value):
    try:
        #SET PREV RUN PROCESSED OFF
        query = ""
        df = []

        query += "UPDATE `latam-md-finance`.control.log_process_run" 
        query += " SET " + process_run_column_name + " = " + str(process_run_column_value)
        query += " ,sys_modified_by_name = '" + sys_modified_by_name + "'"
        query += " ,sys_modified_on = '" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "'"
        query += " WHERE process_run_id = " + str(process_run_id) + ""

        df = spark.sql(query)
        return True
    
    except Exception as e:
        print(f"❌ ERROR UPDATING LOG PROCESS RUN, ID {process_run_id} : {e}")
        return False

"""********************************************************************"""



