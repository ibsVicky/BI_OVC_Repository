# config.py

#Entorno
env = "dev"

#Control
control_table = "`latam-md-finance`.control.sys_process_source"

#Sharepoint connection
hostname = "insightabs.sharepoint.com"
site_relative = "sites/InsightTeam" 
sp_file_folder = "Documents"

#file_name_prefix
fin_var_cost_act_file_name = "tdf_fin_variable_cost_act"

#S3 buckets
log_bucket = "latam-md-finance-log"
raw_bucket = "latam-md-finance-raw"
bronze_bucket = "latam-md-finance-bronze"
silver_bucket = "latam-md-finance-silver"
gold_bucket = "latam-md-finance-gold"
archive_bucket = "latam-md-finance-archive"