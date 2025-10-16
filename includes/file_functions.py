import re
import pandas as pd
from datetime import datetime

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

"""********************************************************************"""

def check_filename(mask,filenamePrefix,filenameExtension,filenameFull):

    file_name_check = True
    place_holders_dict = {}
    file_name_already_validated = ""
    
    place_holders_dict = {
        "fileName": 0,
        "sourceSystem": 0,
        "yyyymmdd": 0,
        "hhmmss": 0,
        "fileExtension": 0
    }


    file_name = filenameFull[0:filenameFull.find(".")] + '.' #I leave the dot at the end to avoid future errors
    file_ext = filenameFull[len(file_name):]

    place_holders_mask = re.findall(r"\{(.*?)\}", mask)

    for placeholder in place_holders_mask:
        if placeholder in place_holders_dict:
            place_holders_dict[placeholder] = 1
    
    for key, value in place_holders_dict.items():
        #FILE_NAME_PREFIX_CHECK
        if value == 1 and key == "fileName":
            if file_name.startswith(filenamePrefix + "_"):
                place_holders_dict[key] = 1
                file_name_already_validated = filenamePrefix
                continue
            else: 
                file_name_check = False 
                return file_name_check
        
        #SOURCE_SYSTEM_CHECK
        if value == 1 and key == "sourceSystem":
            start_index = len(filenamePrefix) + 1
            end_index = file_name.find("_", start_index)

            sourceSystem = file_name[start_index:end_index]
            
            if 1==1:  #PENDING acá habría que ver cómo validar que el source system sea correcto, o si se devuelve o qué se hace
                place_holders_dict[key] = 1
                file_name_already_validated = file_name_already_validated + "_" + sourceSystem
                continue
            else: 
                file_name_check = False 
                return file_name_check

        #DATE_CHECK
        if value == 1 and key == "yyyymmdd":
            start_index = file_name.find("_", len(file_name_already_validated)) + 1
            end_index = file_name.find("_", start_index)

            fecha_str = file_name[start_index:end_index]
            
            try:
                if len(fecha_str) == len("yyyymmdd"):
                    datetime.strptime(fecha_str, "%Y%m%d")
                    place_holders_dict[key] = 1
                    file_name_already_validated = file_name_already_validated + '_' + fecha_str
                    continue
                else:
                    file_name_check = False 
                    return file_name_check 
            except:
                file_name_check = False 
                return file_name_check

        #TIME_CHECK
        if value == 1 and key == "hhmmss":
            start_index = file_name.find("_", len(file_name_already_validated)) + 1
            end_index = file_name.find("_", start_index)

            hora_str = file_name[start_index:end_index]
            
            if len(hora_str) == len("hhmmss") and pd.to_datetime(hora_str, format="%H%M%S", errors="coerce") is not pd.NaT:
                place_holders_dict[key] = 1
                file_name_already_validated = file_name_already_validated + '_' + hora_str
                continue
            else:
                file_name_check = False 
                return file_name_check

        #FILE_EXTENSION_CHECK
        if value == 1 and key == "fileExtension":
            if file_ext == filenameExtension:
                place_holders_dict[key] = 1
                file_name_already_validated = file_name_already_validated + '.' + file_ext
                continue
            else:
                file_name_check = False 
                return file_name_check
    
    return file_name_check

"""********************************************************************"""
def imprimir():
    print("hola mundo")
