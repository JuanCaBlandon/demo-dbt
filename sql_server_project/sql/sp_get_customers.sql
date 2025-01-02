USE StateReporting
GO

CREATE OR ALTER PROCEDURE GetCustomersIA
AS

/*
	Autor: Yeison Ortiz
	Company: SourceMeridian
	Short description: SP to get active customers for IOWA State
	Creation date: 2024-11-25
	Modification date: 2024-12-18
*/

-- Get the customers for SQL and Databricks implementation

TRUNCATE TABLE StateReporting.dbo.TpmStateReportedCustomer;

INSERT INTO StateReporting.dbo.TpmStateReportedCustomer(
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
	StateCode ,
    ActiveStatus ,
	ReportStatusCD,
	CustomerStatus ,
    ActiveStatusStartDate ,
	EffectiveStartDate,
	EffectiveEndDate,
	DeviceLogRptgClassCd,
	CreateDate,
	CreateUser,
	ModifyDate,
	ModifyUser,
	RepeatOffender,
	OffenseDate ,
	IIDStartDate,
	IIDEndDate ,
	CreationDate)

SELECT *
FROM (
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
        CASE WHEN custst.[EffectiveEndDate] IS NULL AND cus.[DeInstallDateConfirmed] IS NULL THEN 1
             WHEN custst.[EffectiveEndDate] IS NULL AND CAST(cus.[DeInstallDateConfirmed] AS DATE) > CAST(GETDATE() AS DATE) THEN 1
             ELSE 0 END AS ActiveStatus,
        'Active-NoReported' AS ReportStatusCD,
        cus.[StatusCd] AS CustomerStatus,
        GETDATE() AS ActiveStatusStartDate,
        custst.[EffectiveStartDate],
        custst.[EffectiveEndDate],
        custst.[DeviceLogRptgClassCd],
        cus.[CreateDate],
        cus.[CreateUser],
        cus.[ModifyDate],
        cus.[ModifyUser],
        CAST(NULL AS DATETIME) AS OffenseDate,
        CAST(NULL AS DATETIME) AS IIDStartDate,
        CAST(NULL AS DATETIME) AS IIDEndDate,
        '' AS RepeatOffender,
        CAST(GETDATE() AS DATE) AS CreationDate

    FROM [CustSrv].[dbo].[Customer] cus
        INNER JOIN [CustSrv].[dbo].[CustomerReportingStates] custst on cus.CustomerID = custst.CustomerID
            AND Installdateconfirmed IS NOT NULL
            AND cus.RelayTypeCd != 924 -- Home Monitor
            AND custst.StateCode = 'IA'
            AND  custst.DeviceLogRptgClassCd
                NOT IN  (1333, --Teen Voluntary
                        356) -- Voluntary
            AND cus.StatusCd 
                NOT IN (506, --Demo
                        849,507) --Webdemo
    ) AS CU;

-- Update customers with incoming batch file data (FTP server)
MERGE StateReporting.dbo.TpmStateReportedCustomer  AS Target
USING StateReporting.dbo.FtpCustomerData AS Source
ON Source.DriversLicenseNumber = Target.DriversLicenseNumber
    AND UPPER(Source.FirstName) = UPPER(Target.FirstName)
    AND UPPER(Source.LastName) = UPPER(Target.LastName)
    AND UPPER(Source.VIN) = UPPER(target.VIN)
    AND Source.CreationDate = Target.CreationDate

-- For Updates
WHEN MATCHED THEN UPDATE SET
    Target.OffenseDate	= Source.OffenseDate,
    Target.IIDStartDate	= Source.IIDStartDate,
    Target.IIDEndDate	= Source.IIDEndDate,
    Target.RepeatOffender = Source.RepeatOffender;


--For SQL implementation
INSERT INTO StateReporting.dbo.StateReportedCustomer 
	(
    CustomerReportingStateID,
	CustomerID,
	StateCode,
    CustomerStatus,
    ActiveStatus,
    ReportStatusCD,
    ActiveStatusStartDate,
    InstallDate, 
    DeInstallDate,
	CreateDate,
	CreateUser,
	ModifyDate,
	ModifyUser,
    OffenseDate,
    IIDStartDate,
    IIDEndDate,
    RepeatOffender,
	CreationDate -- Indicate when this record is inserted 
	) 

SELECT
    CU.CustomerReportingStateID,
    CU.CustomerID,
	CU.StateCode,
    CU.CustomerStatus,
    CU.ActiveStatus,
    CASE WHEN CU.RepeatOffender = '1' THEN 'Active-Reported'
         WHEN CU.RepeatOffender = '0' THEN 'Inactive'
         ELSE CU.ReportStatusCd 
    END AS ReportStatusCD,
    CU.ActiveStatusStartDate,
    CU.InstallDate,
    CU.DeInstallDate,
	CU.CreateDate,
	CU.CreateUser,
	CU.ModifyDate,
	CU.ModifyUser,
    CU.OffenseDate,
    CU.IIDStartDate,
    CU.IIDEndDate,
    CU.RepeatOffender,
	CU.CreationDate

FROM StateReporting.dbo.StateReportedCustomer HC
	INNER JOIN StateReporting.dbo.TpmStateReportedCustomer CU ON HC.CustomerID = CU.CustomerID
WHERE (HC.CustomerStatus <> CU.CustomerStatus
	OR CASE WHEN HC.ModifyDate IS NULL THEN '' ELSE HC.ModifyDate END <> CASE WHEN CU.ModifyDate IS NULL THEN '' ELSE CU.ModifyDate END
    OR CASE WHEN HC.OffenseDate IS NULL THEN '' ELSE HC.OffenseDate END <> CASE WHEN CU.OffenseDate IS NULL THEN '' ELSE CU.OffenseDate END
    OR CASE WHEN HC.IIDStartDate IS NULL THEN '' ELSE HC.IIDStartDate END <> CASE WHEN CU.IIDStartDate IS NULL THEN '' ELSE CU.IIDStartDate END
    OR CASE WHEN HC.IIDEndDate IS NULL THEN '' ELSE HC.IIDEndDate END <> CASE WHEN CU.IIDEndDate IS NULL THEN '' ELSE CU.IIDEndDate END
    OR HC.ActiveStatus <> CU.ActiveStatus
    OR HC.RepeatOffender <> CU.RepeatOffender
    OR CASE WHEN HC.DeInstallDate IS NULL THEN '' ELSE HC.DeInstallDate END <> CASE WHEN CU.DeInstallDate IS NULL THEN '' ELSE CU.DeInstallDate END
	OR UPPER(HC.ModifyUser) <> UPPER(CU.ModifyUser)
    )
    AND CU.ActiveStatus = 1
    --AND CU.OffenseDate >= '2025-01-01'
    ;

-- Insert new customers
INSERT INTO StateReporting.dbo.StateReportedCustomer 
	(
    CustomerReportingStateID,
	CustomerID,
	StateCode,
    CustomerStatus,
    ActiveStatus,
    ReportStatusCD,
    ActiveStatusStartDate,
    InstallDate, 
    DeInstallDate,
	CreateDate,
	CreateUser,
	ModifyDate,
	ModifyUser,
    OffenseDate,
    IIDStartDate,
    IIDEndDate,
    RepeatOffender,
	CreationDate -- Indicate when this record is inserted 
	) 


SELECT
    CU.CustomerReportingStateID,
    CU.CustomerID,
	CU.StateCode,
    CU.CustomerStatus,
    CU.ActiveStatus,
    CU.ReportStatusCD,
    CU.ActiveStatusStartDate,
    CU.InstallDate,
    CU.DeInstallDate,
	CU.CreateDate,
	CU.CreateUser,
	CU.ModifyDate,
	CU.ModifyUser,
    CU.OffenseDate,
    CU.IIDStartDate,
    CU.IIDEndDate,
    CU.RepeatOffender,
	CU.CreationDate

FROM StateReporting.dbo.TpmStateReportedCustomer CU
WHERE NOT EXISTS (SELECT CustomerID FROM StateReporting.dbo.StateReportedCustomer ST WHERE CU.CustomerID =  ST.CustomerID)
    AND CU.ActiveStatus = 1
 -- AND CU.OffenseDate >= '2025-01-01'
    ;

-- Put date to inactive customers, ActiveStatus = 0
UPDATE ST
SET ST.ActiveStatusEndDate = GETDATE()
FROM StateReporting.dbo.TpmStateReportedCustomer CU
    INNER JOIN StateReporting.dbo.StateReportedCustomer ST ON CU.CustomerID = ST.CustomerID
WHERE CU.ActiveStatus = 0
    --AND CU.OffenseDate >= '2025-01-01'
    ;

-- This top one is only for databricks because spark needs something to return when the SP is running 
SELECT TOP 1 * 
FROM StateReporting.dbo.TpmStateReportedCustomer
;
