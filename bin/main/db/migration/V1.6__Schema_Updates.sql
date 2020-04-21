create TABLE SIMA_CENSUS (
    [COUNTRY] INTEGER,
    [CNTYDESC] VARCHAR(255),
    [ldesccen36] VARCHAR(255),
    [grade] VARCHAR(255),
    [VALUE] DECIMAL(30,10),
    UNKNOWN_FIELD_1 DECIMAL(30,10),
    UNKNOWN_FIELD_2 DECIMAL(30,10),
    QTYMT DECIMAL(30,10),
    UNKNOWN_FIELD_3 DECIMAL(30,10),
    [SMONTH] INTEGER,
    [SYEAR] INTEGER
);
go

create TABLE SIMA_LICENSE (
    [Date of Importation] DATE,
    [Country of Origin] VARCHAR(255),
    [Category] VARCHAR(255),
    [HTS Code] VARCHAR(255),
    [Value] DECIMAL(30,10),
    [Volume] DECIMAL(30,10),
    [License_Number] VARCHAR(255)
);
go
