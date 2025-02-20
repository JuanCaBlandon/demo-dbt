USE StateReporting
GO

CREATE OR ALTER PROCEDURE databricks.GetCustomersIA
    @START_DATE DATE,
    @END_DATE DATE,
    @EXECUTION_DATE DATE
AS

/*
	Autor: Yeison Ortiz
	Company: SourceMeridian
	Short description: SP to get active customers for IOWA State
	Creation date: 2024-11-25
	Modification date: 2025-02-20
*/

-- Get the customers for SQL and Databricks implementation

DELETE FROM StateReporting.databricks.TmpStateReportedCustomer;

INSERT INTO StateReporting.databricks.TmpStateReportedCustomer(
	CustomerReportingStateID ,
	CustomerID ,
	DriversLicenseNumber,
	FirstName,
    LastName,
    MiddleName,
    DateOfBirth,
    VIN,
    InstallDate,
    DeInstallDate,
	StateCode,
    ActiveStatus,
	ReportStatusCD,
	CustomerStatus,
    ActiveStatusStartDate ,
	EffectiveStartDate,
	EffectiveEndDate,
	DeviceLogRptgClassCd,
	CreateDate,
	CreateUser,
	ModifyDate,
	ModifyUser,
	RepeatOffender,
	OffenseDate,
	IIDStartDate,
	IIDEndDate,
	CreationDate
)

SELECT 
    custst.[CustomerReportingStateID],
    cus.[CustomerID],
    cus.[DriversLicenseNumber],
    cus.[FirstName],
    cus.[LastName],
    cus.[MiddleName],
    cus.[DateOfBirth],
    cus.[VIN],
    cus.[InstallDateConfirmed] AS InstallDate,
    cus.[DeInstallDateConfirmed] AS DeInstallDate,
    custst.[StateCode],
    1 AS ActiveStatus,
    'Active-NotReported' AS ReportStatusCD,
    cus.[StatusCd] AS CustomerStatus,
    @EXECUTION_DATE AS ActiveStatusStartDate,
    custst.[EffectiveStartDate],
    custst.[EffectiveEndDate],
    custst.[DeviceLogRptgClassCd],
    cus.[CreateDate],
    cus.[CreateUser],
    cus.[ModifyDate],
    cus.[ModifyUser],
    NULL AS RepeatOffender,
    NULL AS OffenseDate,
	NULL AS IIDStartDate,
	NULL AS IIDEndDate,
    CAST(@EXECUTION_DATE AS DATE) AS CreationDate
FROM [CustSrv].[dbo].[Customer] cus WITH (NOLOCK)
INNER JOIN [CustSrv].[dbo].[CustomerReportingStates] custst  WITH (NOLOCK)
    ON cus.CustomerID = custst.CustomerID
    AND Installdateconfirmed IS NOT NULL
    AND cus.RelayTypeCd != 924 -- Home Monitor
    AND custst.StateCode = 'IA'
	AND  custst.DeviceLogRptgClassCd NOT IN  (1333, 356) --Teen Voluntary, -- Voluntary
	AND cus.StatusCd NOT IN (506, 849, 507) --Demo, --Webdemo
WHERE
    cus.EffectiveStartDate IS NOT NULL AND
	((custst.[EffectiveEndDate] IS NULL AND cus.[DeInstallDateConfirmed] IS NULL)
	OR (custst.[EffectiveEndDate] IS NULL AND CAST(cus.[DeInstallDateConfirmed] AS DATE) > CAST(@EXECUTION_DATE AS DATE))
	OR (
        custst.[EffectiveEndDate] > CAST(@EXECUTION_DATE AS DATE)
        AND (cus.[DeInstallDateConfirmed] IS NULL OR CAST(cus.[DeInstallDateConfirmed] AS DATE) > CAST(@EXECUTION_DATE AS DATE))
    ));

-- This top one is only for databricks because spark needs something to return when the SP is running 
SELECT TOP 1 *
FROM StateReporting.databricks.TmpStateReportedCustomer
;