from common.connections import (
    get_source_connection,
    get_staging_connection,
    get_clickhouse_connection,
)


def sql_login(conn):
    with conn.cursor() as cur:
        cur.execute("USE [master]")
        cur.execute("""IF SUSER_ID(N'admin') IS NULL
            EXEC(N'CREATE LOGIN [admin] WITH PASSWORD=N''admin'', CHECK_POLICY=OFF, CHECK_EXPIRATION=OFF')
            ELSE BEGIN
              ALTER LOGIN [admin] WITH CHECK_POLICY=OFF, CHECK_EXPIRATION=OFF;
              ALTER LOGIN [admin] WITH PASSWORD=N'admin';
              ALTER LOGIN [admin] ENABLE;
            END""")
        for database in ("Northwind", "NorthwindDW"):
            cur.execute("USE [" + database + "]")
            cur.execute(
                """IF USER_ID(N'admin') IS NULL CREATE USER [admin] FOR LOGIN [admin]
                ELSE ALTER USER [admin] WITH LOGIN=[admin]"""
            )
            cur.execute(
                "IF IS_ROLEMEMBER(N'db_owner',N'admin')=0 ALTER ROLE [db_owner] ADD MEMBER [admin]"
            )


def pg_login(conn):
    with conn, conn.cursor() as cur:
        cur.execute(
            """DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='admin') THEN
            CREATE ROLE admin LOGIN PASSWORD 'admin'; ELSE ALTER ROLE admin LOGIN PASSWORD 'admin'; END IF; END $$"""
        )
        cur.execute('GRANT CONNECT ON DATABASE "Staging_Northwind" TO admin')
        for schema in ("public", "ops"):
            cur.execute(f"GRANT USAGE ON SCHEMA {schema} TO admin")
            cur.execute(
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO admin"
            )
            cur.execute(
                f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO admin"
            )
            cur.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO admin"
            )
            cur.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT USAGE, SELECT ON SEQUENCES TO admin"
            )


def ch_login(client):
    client.command(
        "CREATE USER IF NOT EXISTS admin IDENTIFIED WITH sha256_password BY 'admin'"
    )
    client.command("ALTER USER admin IDENTIFIED WITH sha256_password BY 'admin'")
    client.command("GRANT ALL ON northwind_dw.* TO admin")


def main():
    for open_connection, configure in (
        (get_source_connection, sql_login),
        (get_staging_connection, pg_login),
        (get_clickhouse_connection, ch_login),
    ):
        conn = open_connection()
        try:
            configure(conn)
        finally:
            conn.close()
    print("Classic local database logins ready")


if __name__ == "__main__":
    main()
