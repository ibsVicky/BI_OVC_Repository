import pyspark.sql.functions as F
from datetime import datetime

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

"""********************************************************************"""

def log_process_run_start(process_source_name,process_step_name,source_system_code,sys_modified_by_name):
    try:
        #get process setup id
        query = ""
        df = []

        query += "SELECT process_source_id" 
        query += " FROM `latam-md-finance`.control.sys_process_source"
        query += " WHERE process_source_name ilike '" + process_source_name + "'"
        query += " AND process_step_name ilike '" + process_step_name + "'"
        query += " AND source_system_code ilike '" + source_system_code + "'"
        query += " AND sys_status_code ilike 'A'"

        df = spark.sql(query.lower())
        row = df.first()

        #if no records returns False
        if row:
            process_source_id = row["process_source_id"]
        else:
            process_source_id = -1
            print(f"❌ NO PROCESS AVAILABLE IN control.sys_process_setup")
            return process_source_id, False

    except Exception as e:
        print(f"❌ ERROR GETTING PROCESS SETUP ID: {e}")
        return process_source_id, False

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

        query += "VALUES (" + str(process_source_id) + ","
        query += "'" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "',"
        query += str(1) + ","
        query += "'" + sys_modified_by_name + "',"
        query += "'" + sys_modified_by_name + "'"
        query += ")"

        df = spark.sql(query)
    
    except Exception as e:
        print(f"❌ ERROR INSERTING RUN PROCESS LOG RECORD: {e}")
        return process_run_id, False

    try:    
        #get current process run
        query = ""
        df = []

        query += "SELECT max(process_run_id) process_run_id" 
        query += " FROM `latam-md-finance`.control.log_process_run"
        query += " WHERE fk_process_setup_id = " + str(process_source_id)
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
        return process_run_id, False


    try:
        #SET PREV RUN PROCESSED OFF
        query = ""
        df = []

        query += "UPDATE `latam-md-finance`.control.log_process_run" 
        query += " SET process_run_last_flag = 0"
        query += " ,sys_modified_by_name = '" + sys_modified_by_name + "'"
        query += " ,sys_modified_on = '" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "'"
        query += " WHERE fk_process_setup_id = " + str(process_source_id) 
        query += " AND process_run_id < " + str(process_run_id) 
        query += " AND process_run_last_flag = 1"

        df = spark.sql(query)

    except Exception as e:
        print(f"❌ ERROR UPDATING RUN LAST FLAG OFF: {e}")
        return process_run_id, False
    
    return process_run_id, True

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

def get_process_source_parameters(process_source_name,process_step_name):

    try:
        query= ""
        df = []

        query = "SELECT "
        query += " source_file_name_prefix,"
        query += " source_sharepoint_host_name,"
        query += " source_sharepoint_site_relative,"
        query += " source_sharepoint_drive,"
        query += " source_sharepoint_file_path,"
        query += " source_bucket_name,"
        query += " source_bucket_folder_key,"
        query += " target_bucket_name,"
        query += " target_bucket_folder_key,"
        query += " archive_bucket_name,"
        query += " archive_bucket_folder_key"
        query += " FROM `latam-md-finance`.control.sys_process_source"
        query += " WHERE process_source_name ilike '" + process_source_name + "'"
        query += " AND process_step_name ilike '" + process_step_name + "'"
        query += " AND sys_status_code = 'A'"

        df = spark.sql(query)
        return df
    
    except Exception as e:
        print(f"❌ ERROR GETTING DATA FROM sys_process_source: {e}")
        return False

"""********************************************************************"""

def log_process_run_update_value(process_run_id,sys_modified_by_name,process_run_column_name, process_run_column_value):
    try:
        #SET PREV RUN PROCESSED OFF
        query = ""
        df = []

        query += "UPDATE `latam-md-finance`.control.log_process_run" 
        query += " SET " + process_run_column_name + " = '" + str(process_run_column_value) + "'"
        query += " ,sys_modified_by_name = '" + sys_modified_by_name + "'"
        query += " ,sys_modified_on = '" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "'"
        query += " WHERE process_run_id = " + str(process_run_id) + ""

        df = spark.sql(query)
        return True
    
    except Exception as e:
        print(f"❌ ERROR UPDATING LOG PROCESS RUN, ID {process_run_id} : {e}")
        return False

"""********************************************************************"""


