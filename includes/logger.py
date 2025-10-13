from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

class S3Logger:
    def __init__(self, spark: SparkSession, s3_path: str):
        """
        Crea un logger funcional que guarda en S3 usando Spark.
        :param spark: sesión Spark activa
        :param s3_path: ruta completa S3 del archivo CSV (ej: 's3://mi-bucket/logs/etl_logs.csv')
        """
        self.spark = spark
        self.s3_path = s3_path
        self.schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("step", StringType(), True),
            StructField("level", StringType(), True),
            StructField("message", StringType(), True)
        ])

    def log_event(self, run_id: str, step: str, message: str, level: str = "INFO"):
        """
        Registra un nuevo evento en el log S3 (crea o appendea según exista).
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Crear un DataFrame con el nuevo registro
        new_row = [(timestamp, run_id, step, level, message)]
        new_df = self.spark.createDataFrame(new_row, schema=self.schema)

        # Intentar leer log existente
        try:
            existing_df = self.spark.read.option("header", "true").schema(self.schema).csv(self.s3_path)
            df_final = existing_df.unionByName(new_df)
        except Exception as e:
            # Si no existe el archivo, se crea desde cero
            print(f"[INFO] No se encontró log previo en {self.s3_path}, creando uno nuevo.")
            df_final = new_df

        # Sobrescribir el CSV con el nuevo contenido
        df_final.write.mode("overwrite").option("header", "true").csv(self.s3_path)

        print(f"{timestamp} [{level}] {step}: {message}")
