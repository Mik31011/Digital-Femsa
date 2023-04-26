import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, when
from pyspark.sql.types import BooleanType, StructType, StructField, IntegerType, StringType, DateType

#Este es un ejemplo de codigo, puedo crear variaciones a necesidad del cliente, etc... solo quise hacer una abstraccion del problema propuesto
#El codigo es funcional pero se deben modificar todas las rutas tanto de los servidores como de los id de GCP
#Ademas se deben agregar las claves de API necesarias

class CustomerDataIntegration:
    def __init__(self, input_schemas_file, output_schemas_file):
        self.spark = SparkSession.builder \
            .appName("Customer Data Integration") \
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.21.1") \
            .getOrCreate()

        self.input_schemas = self.load_schemas(input_schemas_file)
        self.output_schemas = self.load_schemas(output_schemas_file)

    @staticmethod
    def load_schemas(file_path):
        with open(file_path, "r") as f:
            return json.load(f)

    @staticmethod
    def schema_from_json(json_schema):
        fields = [StructField(field['name'], eval(
            field['type'].capitalize() + 'Type')()) for field in json_schema['fields']]
        return StructType(fields)

    @staticmethod
    def validate_schema(df, expected_schema):
        return df.schema == expected_schema

    def read_data(self, source, format, options):
        df = self.spark.read \
            .format(format) \
            .options(**options) \
            .load()

        schema = self.schema_from_json(self.input_schemas[source])
        assert self.validate_schema(
            df, schema), f"{source} DataFrame schema does not match the expected schema"
        return df

    def transform_data(self, oracle_df, sqlserver_df, mysql_df):
        enriched_customers = oracle_df.select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            col("phone_number"),
            col("birth_date")
        ).unionAll(
            sqlserver_df.select(
                col("client_id").alias("customer_id"),
                col("first_name"),
                col("last_name"),
                col("email_address").alias("email"),
                col("phone"),
                col("date_of_birth").alias("birth_date")
            )
        ).unionAll(
            mysql_df.select(
                col("cust_id").alias("customer_id"),
                col("given_name").alias("first_name"),
                col("family_name").alias("last_name"),
                col("mail").alias("email"),
                col("telephone").alias("phone_number"),
                col("dob").alias("birth_date")
            )
        )

        def is_valid_email(email):
            return when(length(email) > 0, True).otherwise(False) & \
                when(col("email").rlike(
                    r'^[\w-]+(\.[\w-]+)*@[\w-]+(\.[\w-]+)*(\.[a-zA-Z]{2,})$'), True).otherwise(False)

        valid_customers = enriched_customers.withColumn(
            "is_valid_email", is_valid_email(col("email")).cast(BooleanType()))
        filtered_customers = valid_customers.filter(col("is_valid_email"))

        output_schema = self.schema_from_json(
            self.output_schemas["Centralized_Customers"])
        assert self.validate_schema(
            filtered_customers, output_schema), "Output DataFrame schema does not match the expected schema"

        return filtered_customers

    def write_data(self, df, table):
        df.write \
            .format("bigquery") \
            .option("temporaryGcsBucket", "your-temporary-gcs-bucket") \
            .option("table", table) \
            .mode("overwrite") \
            .save()


    def main(self):
        # Leer datos de las bases de datos de origen (Oracle, SQL Server, MySQL)
        oracle_df = self.read_data("Oracle", "jdbc", {"url": "jdbc:oracle:thin:@//your_oracle_host:1521/your_sid",
                                    "dbtable": "oracle_customers", "user": "username", "password": "password", "driver": "oracle.jdbc.driver.OracleDriver"})
        sqlserver_df = self.read_data("SQLServer", "jdbc", {"url": "jdbc:sqlserver://your_sqlserver_host:1433;databaseName=your_database",
                                    "dbtable": "sqlserver_customers", "user": "username", "password": "password", "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"})
        mysql_df = self.read_data("MySQL", "jdbc", {"url": "jdbc:mysql://your_mysql_host:3306/your_database",
                                "dbtable": "mysql_customers", "user": "username", "password": "password", "driver": "com.mysql.jdbc.Driver"})

        # Transformar y validar datos
        transformed_data = self.transform_data(oracle_df, sqlserver_df, mysql_df)

        # Escribir datos en BigQuery
        self.write_data(
            transformed_data, "your-gcp-project-id:your-bigquery-dataset.centralized_customers")

        # Detener la sesión de Spark
        self.spark.stop()


if __name__ == "__main__":
    integration = CustomerDataIntegration(
        "input_schemas.json", "output_schema.json")
    integration.main()
