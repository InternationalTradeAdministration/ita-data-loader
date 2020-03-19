create TABLE SIMA_SMP_BY_HTS (
    [HTS Code] VARCHAR(255),
    [HTS Description] VARCHAR(255),
    [Category] VARCHAR(255),
    [Aggregate Category] VARCHAR(255)
);
go

create view SIMA_SMP_BY_HTS_VW
as
select [HTS Code]
    , UPPER([Category]) as Category
    , CASE WHEN [HTS Description] like '%STAINLESS%' then 'S' ELSE 'C & A' END as grade2
from SIMA_SMP_BY_HTS
go

create view SIMA_LICENSE_VW
as
select lic.[Country of Origin] as [Country]
    , SUM(lic.[Value]) as [VALUE]
    , SUM(lic.[Volume])  / 1000 as [VOLUME]
    , MONTH(lic.[Date of Importation]) as [SMONTH]
    , YEAR(lic.[Date of Importation]) as [SYEAR]
    , ref.Category
    , ref.grade2
from SIMA_LICENSE lic
join SIMA_SMP_BY_HTS_VW ref
on lic.[HTS Code] = ref.[HTS Code]
group by  lic.[Country of Origin], MONTH(lic.[Date of Importation]), YEAR(lic.[Date of Importation]), ref.Category, ref.grade2
go

create view SIMA_CENSUS_VW
as
select
    [CNTYDESC] as Country
    , [VALUE]
    , [UNKNOWN_FIELD_1]
    , [UNKNOWN_FIELD_2]
    , [QTYMT]
    , [UNKNOWN_FIELD_3]
    , [SMONTH]
    , [SYEAR]
    , CASE WHEN grade <> 'S' THEN 'C & A' ELSE [grade] END as grade2
    , CASE
        WHEN [ldesccen36] like '%LINE PIPE%' THEN 'LINE PIPE'
        ELSE SUBSTRING([ldesccen36], 1, CHARINDEX('.',[ldesccen36]) - 1)
    END as Category
from SIMA_CENSUS
go

create view SIMA_STEEL_IMPORT_RPT_VW
as
select [Country], [Category], [QTYMT] as [VOLUME], [VALUE], [SMONTH], [SYEAR], [grade2], 'census' as [SOURCE] from SIMA_CENSUS_VW union all
select [Country], [Category], [VOLUME], [VALUE], [SMONTH], [SYEAR], [grade2], 'license' as [SOURCE] from SIMA_LICENSE_VW
go
