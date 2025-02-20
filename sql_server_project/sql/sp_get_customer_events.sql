USE StateReporting
GO

CREATE OR ALTER PROCEDURE databricks.GetCustomerEventsIA
    @START_DATE DATE,
    @END_DATE DATE,
	@EXECUTION_DATE DATE

AS

/*
	Author: Yeison Ortiz
	Company: SourceMeridian
	Short description: SP to get customers violations for IOWA State
	Creation date: 2025-01-12
	Modification date: 2025-02-11
	Modified by: Sebastian Osorno
*/


IF OBJECT_ID('tempdb..#TmpCustomerEvents') IS NOT NULL
        DROP TABLE #TmpCustomerEvents;

CREATE TABLE #TmpCustomerEvents(
	[EventType] [nvarchar](15) NULL,
	[CustomerID] [bigint] NULL,
	[DeviceUsageViolationID] [bigint] NULL,
	[DeviceUsageEventViolationID] [bigint] NULL,
	[CustomerTransactionID] [bigint] NULL,
	[DeviceUsageID] [int] NULL,
	[ViolationReportingApprovalCd] [int] NULL,
	[ViolationReportingApprovalUser] [nvarchar](50) NULL,
	[CreateDate] [datetime] NULL,
	[CreateUser] [nvarchar](20) NULL,
	[ModifyDate] [datetime] NULL,
	[ModifyUser] [nvarchar](20) NULL,
	[LogEntryTime] [datetime] NULL,
	[Eventdate] [datetime] NULL,
	[VIN] [nvarchar](50) NULL,
	[NewVIN] [nvarchar](50) NULL,
	[CreationDate] [date] NULL,
	[ModificationDate] [date] NULL
);

WITH MAX_US AS (
	SELECT
		CustomerID, -- Handle multiple vehicles, use driver license
		MAX(LastUsageDate) LastUsageDate
	FROM CustSrv.dbo.DeviceUsage WITH (NOLOCK)
	GROUP BY customerID
	-- Handle multiple vehicles, use driver license,
	-- Only if max usage date for both vehicles is past IID end date...
)
INSERT INTO #TmpCustomerEvents

-- Record Type 1 and 2
SELECT
	'TYPE 1-2' EventType,
	DU.CustomerId,
	DUV.DeviceUsageViolationID,
	NULL DeviceUsageEventViolationID,
	NULL CustomerTransactionID,
	DUV.DeviceUsageID,
	DUV.ViolationReportingApprovalCd,
	DUV.ViolationReportingApprovalUser,
	DUV.CreateDate,
	DUV.CreateUser,
	DUV.ModifyDate,
	DUV.ModifyUser,
	DLE.LogEntryTime,
	CAST(DLE.LogEntryTime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS datetime) EventDate,
	NULL VIN,
	NULL NewVIN,
	@EXECUTION_DATE CreationDate,
	NULL ModificationDate
FROM CustSrv.dbo.DeviceUsageViolation DUV WITH (NOLOCK)
INNER JOIN CustSrv.dbo.DeviceUsage DU WITH (NOLOCK)
	ON DU.DeviceUsageId = DUV.DeviceUsageId
INNER JOIN CustSrv.dbo.CustomerReportingStates CRS WITH (NOLOCK)
	ON DU.CustomerId = CRS.CustomerID
INNER JOIN DevicelogData.dbo.DeviceLogEntry DLE WITH (NOLOCK)
	ON DU.DeviceUsageID = DLE.DeviceUsageID 
	AND DLE.DeviceLogEntryID = StartingDeviceLogEntryID
LEFT JOIN StateReporting.databricks.CustomerEvents CE WITH (NOLOCK)
	ON DUV.DeviceUsageViolationID = CE.DeviceUsageViolationID
	AND CE.EventType = 'TYPE 1-2'
WHERE
	CE.DeviceUsageViolationID IS NULL
    AND DUV.ViolationID IN (1, 11)
    AND DUV.ViolationReportingApprovalCd IN (
				344, -- Approved
        345 -- Auto-Approved
		)
    AND CRS.StateCode = 'IA'
    AND CAST(DLE.LogEntryTime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS datetime)
        BETWEEN CRS.EffectiveStartDate AND COALESCE(DATEADD(DAY, 1, CRS.EffectiveEndDate), @EXECUTION_DATE)
		AND CAST(DLE.LogEntryTime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS datetime) BETWEEN @START_DATE AND @END_DATE
-- TODO: add repeatoffender filter


-- Record Type 3
-- Manual Event Violations to trigger Record
-- (Power Interruption - 30 min)  ID (2)
-- Lockout - Power Off After Car Start (ID 66)
UNION ALL
SELECT
	'TYPE 3' EventType,
	DU.CustomerId,
	DUV.DeviceUsageViolationID,
	NULL DeviceUsageEventViolationID,
	NULL CustomerTransactionID,
	DUV.DeviceUsageID,
	DUV.ViolationReportingApprovalCd,
	DUV.ViolationReportingApprovalUser,
	DUV.CreateDate,
	DUV.CreateUser,
	DUV.ModifyDate,
	DUV.ModifyUser,
	DLE.LogEntryTime,
	CAST(DLE.LogEntryTime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS datetime) Eventdate,
	NULL VIN,
	NULL NewVIN,
	@EXECUTION_DATE CreationDate,
	NULL ModificationDate
FROM CustSrv.dbo.DeviceUsageViolation DUV WITH (NOLOCK)
INNER JOIN CustSrv.dbo.DeviceUsage DU WITH (NOLOCK)
	ON DU.DeviceUsageId = DUV.DeviceUsageId
INNER JOIN CustSrv.dbo.CustomerReportingStates CRS WITH (NOLOCK)
	ON DU.CustomerId = CRS.CustomerID
INNER JOIN DevicelogData.dbo.DeviceLogEntry DLE WITH (NOLOCK)
	ON DU.DeviceUsageID = DLE.DeviceUsageID 
	AND DLE.DeviceLogEntryID = StartingDeviceLogEntryID
LEFT JOIN StateReporting.databricks.CustomerEvents CE WITH (NOLOCK)
	ON DUV.DeviceUsageViolationID = CE.DeviceUsageViolationID
	AND CE.EventType = 'TYPE 3'
WHERE
		CE.DeviceUsageViolationID IS NULL
    AND DUV.ViolationID IN (2, 66)
    AND DUV.ViolationReportingApprovalCd IN (344, 345) -- Approved,  -- Auto-Approved
    AND CRS.StateCode = 'IA'
    AND CAST(DLE.LogEntryTime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS datetime)
        BETWEEN CRS.EffectiveStartDate AND COALESCE(DATEADD(DAY, 1, CRS.EffectiveEndDate), @EXECUTION_DATE)
		AND CAST(DLE.LogEntryTime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS datetime) BETWEEN @START_DATE AND @END_DATE
		


UNION ALL
SELECT
	'TYPE 3' EventType,
	DU.CustomerId,
	NULL DeviceUsageViolationID,
	DUEV.DeviceUsageEventViolationID,
	NULL CustomerTransactionID,
	DUEV.DeviceUsageID,
	Null ViolationReportingApprovalCd,
	Null ViolationReportingApprovalUser,
	DUEV.CreateDate,
	DUEV.CreateUser,
	DUEV.ModifyDate,
	DUEV.ModifyUser,
	NULL LogEntryTime,
	DUEV.ViolationDate EventDate,
	NULL VIN,
	NULL NewVIN,
	@EXECUTION_DATE CreationDate,
	NULL ModificationDate
FROM CustSrv.dbo.DeviceUsageEventViolation DUEV WITH (NOLOCK)
INNER JOIN CustSrv.dbo.DeviceUsage DU WITH (NOLOCK) ON DU.DeviceUsageId  = DUEV.DeviceUsageId
INNER JOIN CustSrv.dbo.CustomerReportingStates CRS WITH (NOLOCK) ON DU.CustomerId = CRS.CustomerID
LEFT JOIN StateReporting.databricks.CustomerEvents CE WITH (NOLOCK)
	ON DUEV.DeviceUsageEventViolationID = CE.DeviceUsageEventViolationID
	AND CE.EventType = 'TYPE 3'
WHERE
	CE.DeviceUsageEventViolationID IS NULL
	AND DUEV.EventViolationCd = 965 -- Tamper
	AND CRS.StateCode = 'IA'
	AND DUEV.ViolationDate 
        BETWEEN CRS.EffectiveStartDate AND COALESCE(DATEADD(DAY, 1, CRS.EffectiveEndDate), @EXECUTION_DATE)
	AND DUEV.ViolationDate BETWEEN @START_DATE AND @END_DATE


-- Record Type 4 – Uninstall Violation 
UNION ALL
SELECT
	'TYPE 4' EventType,
	C.CustomerID,
	NULL DeviceUsageViolationID,
	NULL DeviceUsageEventViolationID,
	CT.CustomerTransactionID,
	NULL DeviceUsageID,
	NULL ViolationReportingApprovalCd,
	NULL ViolationReportingApprovalUser,
	CT.CreateDate,
	CT.CreateUser,
	CT.ModifyDate,
	CT.ModifyUser,
	NULL LogEntryTime,
	CT.TrnParm3 EventDate,
	NULL VIN,
	NULL NewVIN,
	@EXECUTION_DATE CreationDate,
	NULL ModificationDate
FROM CustSrv.dbo.Customer C WITH (NOLOCK)
INNER JOIN CustSrv.dbo.CustomerReportingStates CRS WITH (NOLOCK)
    ON C.CustomerID = CRS.CustomerID
INNER JOIN CustSrv.dbo.CustomerTransaction CT WITH (NOLOCK)
    ON C.CustomerID = CT.CustomerID 
    AND CT.TransactionCode = 'De-install' 
    AND CT.StatusCode <> 'C' 
    AND CT.TrnParm3 IS NOT NULL
INNER JOIN  CustSrv.Mongoose.DnAccountClosureDispositionDetails DACDD WITH (NOLOCK)
    ON CT.AccountClosureDispositionDetailId = DACDD.AccountClosureDispositionDetailId
INNER JOIN  CustSrv.Mongoose.DnAccountClosureDispositions DACD WITH (NOLOCK)
    ON DACDD.AccountClosureDispositionId = DACD.AccountClosureDispositionId
LEFT JOIN StateReporting.databricks.CustomerEvents CE WITH (NOLOCK)
	ON CT.CustomerTransactionID = CE.CustomerTransactionID
	AND CE.EventType = 'TYPE 4'
WHERE
	CE.CustomerTransactionID IS NULL
    AND CRS.StateCode = 'IA' 
    AND C.DeInstallDateConfirmed BETWEEN @START_DATE AND CONVERT(DATE, @EXECUTION_DATE) 
    AND DACD.AccountClosureDispositionId IN (2,3) --Incomplete, --Deceased
	AND CT.TrnParm3 BETWEEN @START_DATE AND @END_DATE
-- If ACD 2, and type 7 sent, then start compliance workflow
-- Check that we haven't sent a type 7 before
		

-- Record type 5 - Authorized Uninstall
UNION ALL
SELECT
	'TYPE 5' EventType,
	C.CustomerID,
	NULL DeviceUsageViolationID,
	NULL DeviceUsageEventViolationID,
	CT.CustomerTransactionID,
	NULL DeviceUsageID,
	NULL ViolationReportingApprovalCd,
	NULL ViolationReportingApprovalUser,
	CT.CreateDate,
	CT.CreateUser,
	CT.ModifyDate,
	CT.ModifyUser,
	NULL LogEntryTime,
	CT.TrnParm3 EventDate,
	NULL VIN,
	NULL NewVIN,
	@EXECUTION_DATE CreationDate,
	NULL ModificationDate
FROM CustSrv.dbo.Customer C
INNER JOIN CustSrv.dbo.CustomerReportingStates CRS WITH (NOLOCK)
    ON C.CustomerID = CRS.CustomerID
INNER JOIN CustSrv.dbo.CustomerTransaction CT WITH (NOLOCK)
    ON C.CustomerID = CT.CustomerID 
    AND CT.TransactionCode = 'De-install' 
    AND CT.StatusCode <> 'C' 
    AND CT.TrnParm3 IS NOT NULL
INNER JOIN  CustSrv.Mongoose.DnAccountClosureDispositionDetails DACDD WITH (NOLOCK)
    ON CT.AccountClosureDispositionDetailId = DACDD.AccountClosureDispositionDetailId
INNER JOIN  CustSrv.Mongoose.DnAccountClosureDispositions DACD WITH (NOLOCK)
    ON DACDD.AccountClosureDispositionId = DACD.AccountClosureDispositionId
LEFT JOIN StateReporting.databricks.CustomerEvents CE WITH (NOLOCK)
	ON CT.CustomerTransactionID = CE.CustomerTransactionID
	AND CE.EventType = 'TYPE 5'
WHERE
	CE.CustomerTransactionID IS NULL
    AND CRS.StateCode = 'IA' 
    AND C.DeInstallDateConfirmed BETWEEN @START_DATE AND CONVERT(DATE, @EXECUTION_DATE) 
    AND DACD.AccountClosureDispositionId IN (1,4)  -- Requirement Complete, --No Requirement
	AND CT.TrnParm3 BETWEEN @START_DATE AND @END_DATE
-- 7 hast to be sent fisrt
-- if type 7 hasn't been sent, compliance workflow
-- If I get ACD 4, sent compliance Workflow
-- ACD 4, no type 7, Customer is Active Reported, 
		

-- Record type 6 - switched_vehicle
UNION ALL
SELECT 
    'TYPE 6' EventType,
    C.CustomerID,
    NULL DeviceUsageEventViolationID,
    NULL DeviceUsageViolationID,
    CT.CustomerTransactionID,
    NULL DeviceUsageID,
    NULL ViolationReportingApprovalCd,
    NULL ViolationReportingApprovalUser,
    CT.CreateDate,
    CT.CreateUser,
    CT.ModifyDate,
    CT.ModifyUser,
    NULL LogEntryTime,
    CAST(CT.TrnParm3 AS DATE) EventDate,
    CT.VIN,
	CT.NewVIN,
	@EXECUTION_DATE CreationDate,
	NULL ModificationDate
FROM CustSrv.dbo.Customer c
INNER JOIN CustSrv.dbo.CustomerTransaction CT
    ON CT.CustomerID = C.CustomerID
    AND CT.CustomerTransactionTypeID = 49 -- Switch
INNER JOIN CustSrv.dbo.CustomerReportingStates CRS
	ON C.CustomerId = CRS.CustomerID
LEFT JOIN StateReporting.databricks.CustomerEvents CE WITH (NOLOCK)
	ON CT.CustomerTransactionID = CE.CustomerTransactionID
	AND CE.EventType = 'TYPE 6'
WHERE
	CE.CustomerTransactionID IS NULL
	AND CT.TrnParm3 IS NOT NULL
	AND CRS.StateCode = 'IA'
	AND CAST(CT.TrnParm3 AS DATE) BETWEEN @START_DATE AND @END_DATE
	

-- Record type 7 - Final Compliance
UNION ALL
SELECT
	'TYPE 7' EventType,
	C.CustomerID,
	NULL DeviceUsageViolationID,
	NULL DeviceUsageEventViolationID,
	DU.CustomerTransactionID,
	NULL DeviceUsageID,
	NULL ViolationReportingApprovalCd,
	NULL ViolationReportingApprovalUser,
	NULL CreateDate,
	NULL CreateUser,
	NULL ModifyDate,
	NULL ModifyUser,
	NULL LogEntryTime,
	MAX_US.LastUsageDate EventDate,
	NULL VIN,
	NULL NewVIN,
	@EXECUTION_DATE CreationDate,
	NULL ModificationDate
FROM [CustSrv].[dbo].[Customer] C WITH (NOLOCK)
INNER JOIN [CustSrv].[dbo].[CustomerReportingStates] CRS  WITH (NOLOCK)
 ON C.CustomerID = CRS.CustomerID
    AND Installdateconfirmed IS NOT NULL
    AND C.RelayTypeCd != 924 -- Home Monitor
    AND CRS.StateCode = 'IA'
	AND  CRS.DeviceLogRptgClassCd NOT IN  (1333, 356) --Teen Voluntary, -- Voluntary
	AND C.StatusCd NOT IN (506, 849, 507) --Demo, --Webdemo, --repair_center
INNER JOIN MAX_US
	ON C.CustomerID = max_us.CustomerID
INNER JOIN CustSrv.dbo.DeviceUsage  DU
	ON MAX_US.CustomerID = DU.CustomerID
	AND MAX_US.LastUsageDate = DU.LastUsageDate
INNER JOIN StateReporting.databricks.FtpCustomerData ftp
	ON C.DriversLicenseNumber = ftp.DriversLicenseNumber
	AND C.VIN = ftp.VIN
	AND ftp.CreationDate = CAST(@EXECUTION_DATE AS DATE)
LEFT JOIN StateReporting.databricks.CustomerEvents CE
	ON  DU.CustomerTransactionID = CE.CustomerTransactionID
	AND CE.EventType = 'TYPE 7'
WHERE
	CE.CustomerTransactionID IS NULL
	AND ftp.IIDEndDate BETWEEN @START_DATE AND @END_DATE
	AND max_us.LastUsageDate > ftp.IIDEndDate
;


MERGE databricks.CustomerEvents  AS TA
USING #TmpCustomerEvents AS SO
ON COALESCE(SO.DeviceUsageViolationID,SO.DeviceUsageEventViolationID,SO.CustomerTransactionID) = COALESCE(TA.DeviceUsageViolationID,TA.DeviceUsageEventViolationID,TA.CustomerTransactionID) -- TODO: What if a usageViolationId is the same as one of the others IDs? This will lead to duplicates

   -- For Inserts
WHEN NOT MATCHED THEN
	INSERT (	
		EventType,
		CustomerID,
		DeviceUsageViolationID,
		DeviceUsageEventViolationID,
		CustomerTransactionID,
		DeviceUsageID,
		ViolationReportingApprovalCd,
		ViolationReportingApprovalUser,
		CreateDate,
		CreateUser,
		ModifyDate,
		ModifyUser,
		LogEntryTime,
		Eventdate,
		VIN,
		NewVIN,
		CreationDate
	) 
	VALUES (
		EventType,
		CustomerID,
		DeviceUsageViolationID,
		DeviceUsageEventViolationID,
		CustomerTransactionID,
		DeviceUsageID,
		ViolationReportingApprovalCd,
		ViolationReportingApprovalUser,
		CreateDate,
		CreateUser,
		ModifyDate,
		ModifyUser,
		LogEntryTime,
		Eventdate,
		VIN,
		NewVIN,
		CAST(@EXECUTION_DATE AS DATE)
	)
;
SELECT count(*) FROM databricks.CustomerEvents'