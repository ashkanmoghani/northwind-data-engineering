IF SCHEMA_ID('dw') IS NULL EXEC('CREATE SCHEMA dw');
GO
IF OBJECT_ID('dw.dimgeography','U') IS NULL CREATE TABLE dw.dimgeography (
    geographykey int IDENTITY(1,1) PRIMARY KEY,
    country nvarchar(256),
    region nvarchar(256),
    city nvarchar(256),
    postalcode nvarchar(256),
    address nvarchar(256)
);
GO
IF OBJECT_ID('dw.dimsupplier','U') IS NULL CREATE TABLE dw.dimsupplier (
    supplierkey int IDENTITY(1,1) PRIMARY KEY,
    supplierid int NOT NULL,
    companyname nvarchar(256),
    contactname nvarchar(256),
    contacttitle nvarchar(256),
    address nvarchar(256),
    city nvarchar(256),
    country nvarchar(256),
    loaddate datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('dw.dimproducts','U') IS NULL CREATE TABLE dw.dimproducts (
    productkey int IDENTITY(1,1) PRIMARY KEY,
    productalternatekey int NOT NULL,
    supplierkey int,
    productname nvarchar(256) NOT NULL,
    categoryname nvarchar(256),
    quantityperunit nvarchar(256),
    unitprice decimal(18,2),
    unitsinstock int,
    unitsonorder int,
    reorderlevel int,
    discontinued bit NOT NULL,
    startdate datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    enddate datetime2(3)
);
GO
IF OBJECT_ID('dw.dimcustomer','U') IS NULL CREATE TABLE dw.dimcustomer (
    customerkey int IDENTITY(1,1) PRIMARY KEY,
    customeralternatekey nvarchar(256) NOT NULL,
    geographykey int,
    companyname nvarchar(256) NOT NULL,
    contactname nvarchar(256),
    contacttitle nvarchar(256),
    phone nvarchar(256),
    fax nvarchar(256),
    startdate datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
    enddate datetime2(3),
    iscurrent bit NOT NULL DEFAULT 1
);
GO
IF OBJECT_ID('dw.dimemployee','U') IS NULL CREATE TABLE dw.dimemployee (
    employeekey int IDENTITY(1,1) PRIMARY KEY,
    employeealternatekey int NOT NULL,
    geographykey int,
    lastname nvarchar(256) NOT NULL,
    firstname nvarchar(256) NOT NULL,
    title nvarchar(256),
    titleofcourtesy nvarchar(256),
    birthdate date,
    hiredate date,
    homephone nvarchar(256),
    extension nvarchar(256),
    reportsto int,
    loaddate datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('dw.dimshipper','U') IS NULL CREATE TABLE dw.dimshipper (
    shipperkey int IDENTITY(1,1) PRIMARY KEY,
    shipperalternatekey int NOT NULL,
    companyname nvarchar(256) NOT NULL,
    phone nvarchar(256),
    loaddate datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('dw.dimdate','U') IS NULL CREATE TABLE dw.dimdate (
    datekey int PRIMARY KEY,
    fulldate date NOT NULL,
    daynumber int NOT NULL,
    dayname nvarchar(256) NOT NULL,
    monthnumber int NOT NULL,
    monthname nvarchar(256) NOT NULL,
    quarternumber int NOT NULL,
    yearnumber int NOT NULL,
    weeknumber int NOT NULL
);
GO
