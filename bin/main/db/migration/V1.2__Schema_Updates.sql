CREATE TABLE [OECD_GDP] (
   [LOCATION]              VARCHAR(255)
  ,[Country]               VARCHAR(255)
  ,[Subject]               VARCHAR(255)
  ,[Measure]               VARCHAR(255)
  ,[TIME]                  INTEGER
  ,[Unit_Code]             VARCHAR(255)
  ,[Unit]                  VARCHAR(255)
  ,[PowerCode_Code]        INTEGER
  ,[PowerCode]             VARCHAR(255)
  ,[Reference_Period_Code] VARCHAR(255)
  ,[Reference_Period]      VARCHAR(255)
  ,[Value]                 DECIMAL(30,10)
  ,[Flag_Codes]            VARCHAR(255)
  ,[Flags]                 VARCHAR(255)
);
GO

CREATE TABLE [OECD_STRICTNESS_OF_EMPLOYMENT_PROTECTION](
  [Country]                VARCHAR(255)
  ,[Series]                VARCHAR(255)
  ,[TIME]                  INTEGER
  ,[Unit_Code]             VARCHAR(255)
  ,[Unit]                  VARCHAR(255)
  ,[PowerCode_Code]        INTEGER
  ,[PowerCode]             VARCHAR(255)
  ,[Reference_Period_Code] VARCHAR(255)
  ,[Reference_Period]      VARCHAR(255)
  ,[Value]                 DECIMAL(30,10)
  ,[Flag_Codes]            VARCHAR(255)
  ,[Flags]                 VARCHAR(255)
);
GO

CREATE TABLE [OECD_AVERAGE_ANNUAL_HOURS_WORKED_PER_WORKER](
  [Country]                 VARCHAR(255)
  ,[EMPSTAT]                VARCHAR(255)
  ,[Employment_status]      VARCHAR(255)
  ,[Frequency]              VARCHAR(255)
  ,[TIME]                   INTEGER
  ,[Unit_Code]              VARCHAR(255)
  ,[Unit]                   VARCHAR(255)
  ,[PowerCode_Code]         INTEGER
  ,[PowerCode]              VARCHAR(255)
  ,[Reference_Period_Code]  VARCHAR(255)
  ,[Reference_Period]       VARCHAR(255)
  ,[Value]                  DECIMAL(30,10)
  ,[Flag_Codes]             VARCHAR(255)
  ,[Flags]                  VARCHAR(255)
);
GO
