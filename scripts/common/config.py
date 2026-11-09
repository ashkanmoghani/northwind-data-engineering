import os

sql_server_host = os.getenv("sql_server_host", "northwind_sqlserver,1433")
sql_server_user = os.getenv("sql_server_user", "sa")
sql_server_password = os.environ["sql_server_password"]
sql_server_source_db = "Northwind"
sql_server_dw_db = "NorthwindDW"
postgres_host = os.getenv("postgres_host", "northwind_postgres")
postgres_port = int(os.getenv("postgres_port", "5432"))
postgres_db = "Staging_Northwind"
postgres_user = "postgres"
postgres_password = os.environ["postgres_password"]
clickhouse_host = "northwind_clickhouse"
clickhouse_port = 8123
clickhouse_database = "northwind_dw"
clickhouse_user = "clickhouse"
clickhouse_password = os.environ["clickhouse_password"]
kafka_bootstrap_servers = "northwind_kafka:19092"
