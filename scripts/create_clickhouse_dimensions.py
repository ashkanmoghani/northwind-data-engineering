from common.connections import get_clickhouse_connection

client = get_clickhouse_connection()

client.command("""
            create table if not exists dimgeography
            (
            geographykey Int32,
            country Nullable(String),
            region Nullable(String),
            city Nullable(String),
            postalcode Nullable(String),
            address Nullable(String)
            )
            engine = MergeTree
            order by geographykey
            """)

client.command("""
            create table if not exists dimsupplier
            (
            supplierkey Int32,
            supplierid Int32,
            companyname Nullable(String),
            contactname Nullable(String),
            contacttitle Nullable(String),
            address Nullable(String),
            city Nullable(String),
            country Nullable(String),
            loaddate Nullable(DateTime)
            )
            engine = MergeTree
            order by supplierkey
            """)

client.command("""
            create table if not exists dimproducts
            (
            productkey Int32,
            productalternatekey Int32,
            supplierkey Nullable(Int32),
            productname String,
            categoryname Nullable(String),
            quantityperunit Nullable(String),
            unitprice Nullable(Decimal(18, 2)),
            unitsinstock Nullable(Int16),
            unitsonorder Nullable(Int16),
            reorderlevel Nullable(Int16),
            discontinued UInt8,
            startdate Nullable(DateTime),
            enddate Nullable(DateTime)
            )
            engine = MergeTree
            order by productkey
            """)

client.command("""
            create table if not exists dimcustomer
            (
            customerkey Int32,
            customeralternatekey String,
            geographykey Nullable(Int32),
            companyname String,
            contactname Nullable(String),
            contacttitle Nullable(String),
            phone Nullable(String),
            fax Nullable(String),
            startdate DateTime,
            enddate Nullable(DateTime),
            iscurrent UInt8
            )
            engine = MergeTree
            order by customerkey
            """)

client.command("""
            create table if not exists dimemployee
            (
            employeekey Int32,
            employeealternatekey Int32,
            geographykey Nullable(Int32),
            lastname String,
            firstname String,
            title Nullable(String),
            titleofcourtesy Nullable(String),
            birthdate Nullable(Date32),
	    hiredate Nullable(Date32),
            homephone Nullable(String),
            extension Nullable(String),
            reportsto Nullable(Int32),
            loaddate DateTime
            )
            engine = MergeTree
            order by employeekey
            """)

client.command("""
            create table if not exists dimshipper
            (
            shipperkey Int32,
            shipperalternatekey Int32,
            companyname String,
            phone Nullable(String),
            loaddate DateTime
            )
            engine = MergeTree
            order by shipperkey
            """)

client.command("""
            create table if not exists dimdate
            (
            datekey Int32,
            fulldate Date,
            daynumber Int32,
            dayname String,
            monthnumber Int32,
            monthname String,
            quarternumber Int32,
            yearnumber Int32,
            weeknumber Int32
            )
            engine = MergeTree
            order by datekey
            """)

print("clickhouse dimension tables created")
