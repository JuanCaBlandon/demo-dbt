USE StateReporting
GO

CREATE OR ALTER PROCEDURE databricks.GetCustomersEventsIA
AS

/*
		Autor: Yeison Ortiz
		Company: SourceMeridian
		Short description: SP to get customers violations for IOWA State
		Creation date: 2025-01-12
*/
TRUNCATE TABLE databricks.TmpCustomerEvents;

INSERT INTO databricks.TmpCustomerEvents(
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
	CAST(DLE.LogEntryTime AS datetime) AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' EventDate,
	NULL VIN,
	NULL NewVIN
FROM 
    CustSrv.dbo.DeviceUsageViolation DUV
INNER JOIN CustSrv.dbo.DeviceUsage DU
	ON DU.DeviceUsageId = DUV.DeviceUsageId
INNER JOIN CustSrv.dbo.CustomerReportingStates CRS
	ON DU.CustomerId = CRS.CustomerID
INNER JOIN DevicelogData.dbo.DeviceLogEntry DLE
	ON DU.DeviceUsageID = DLE.DeviceUsageID 
	AND DLE.DeviceLogEntryID = StartingDeviceLogEntryID
WHERE
    DUV.ViolationID IN (1, 11)
    AND (
        DUV.ViolationReportingApprovalCd = 344 -- Approved
        OR DUV.ViolationReportingApprovalCd = 345 -- Auto-Approved
    )
    AND CRS.StateCode = 'IA'
    AND CAST(DLE.LogEntryTime AS datetime) AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time'
        BETWEEN CRS.EffectiveStartDate AND COALESCE(CRS.EffectiveEndDate, GETDATE())


UNION ALL
-- Record Type 3
-- Manual Event Violations to trigger Record
-- (Power Interruption - 30 min)  ID (2)
-- Lockout - Power Off After Car Start (ID 66)
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
	CAST(DLE.LogEntryTime AS datetime) AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' Eventdate,
	NULL VIN,
	NULL NewVIN
FROM 
    CustSrv.dbo.DeviceUsageViolation DUV
INNER JOIN CustSrv.dbo.DeviceUsage DU
	ON DU.DeviceUsageId = DUV.DeviceUsageId
INNER JOIN CustSrv.dbo.CustomerReportingStates CRS
	ON DU.CustomerId = CRS.CustomerID
INNER JOIN DevicelogData.dbo.DeviceLogEntry DLE
	ON DU.DeviceUsageID = DLE.DeviceUsageID 
	AND DLE.DeviceLogEntryID = StartingDeviceLogEntryID
WHERE
    DUV.ViolationID IN (2, 66)
    AND (
        DUV.ViolationReportingApprovalCd = 344 -- Approved
        OR DUV.ViolationReportingApprovalCd = 345 -- Auto-Approved
    )
    AND CRS.StateCode = 'IA'
    AND CAST(DLE.LogEntryTime AS datetime) AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time'
        BETWEEN CRS.EffectiveStartDate AND COALESCE(CRS.EffectiveEndDate, GETDATE())


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
	NULL NewVIN
FROM CustSrv.dbo.DeviceUsageEventViolation DUEV
INNER JOIN CustSrv.dbo.DeviceUsage DU ON DU.DeviceUsageId  = DUEV.DeviceUsageId
INNER JOIN CustSrv.dbo.CustomerReportingStates CRS ON DU.CustomerId = CRS.CustomerID
WHERE
	DUEV.EventViolationCd = 965 -- Tamper
	AND CRS.StateCode = 'IA'
	AND DUEV.ApprovalStatusInd = 1 -- TODO: should I add this? it's on record type 4 
    AND DUEV.ViolationDate 
        BETWEEN CRS.EffectiveStartDate AND COALESCE(CRS.EffectiveEndDate, GETDATE())


UNION ALL
-- Record Type 4 – Uninstall Violation 
SELECT
	'TYPE 4' EventType,
	DU.CustomerId,
	NULL DeviceUsageViolationID,
	DUEV.DeviceUsageEventViolationID,
	NULL CustomerTransactionID,
	DUEV.DeviceUsageID,
	NULL ViolationReportingApprovalCd,
	NULL ViolationReportingApprovalUser,
	DUEV.CreateDate,
	DUEV.CreateUser,
	DUEV.ModifyDate,
	DUEV.ModifyUser,
	NULL LogEntryTime,
	DUEV.ViolationDate EventDate,
	NULL VIN,
	NULL NewVIN
FROM CustSrv.dbo.DeviceUsageEventViolation DUEV
INNER JOIN CustSrv.dbo.DeviceUsage DU ON DU.DeviceUsageId  = DUEV.DeviceUsageId
INNER JOIN CustSrv.dbo.CustomerReportingStates CRS ON DU.CustomerId = CRS.CustomerID
WHERE
	DUEV.EventViolationCd = 1099 -- Unauthorized Removal  
	AND DUEV.ApprovalStatusInd = 1
	AND DUEV.ViolationDate 
		BETWEEN CRS.EffectiveStartDate AND COALESCE(CRS.EffectiveEndDate, GETDATE())
		
UNION ALL
-- Record type 5 - Authorized Uninstall
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
	NULL NewVIN
FROM CustSrv.dbo.Customer C
INNER JOIN CustSrv.dbo.CustomerReportingStates CRS
    ON C.CustomerID = CRS.CustomerID
INNER JOIN CustSrv.dbo.CustomerTransaction CT
    ON C.CustomerID = CT.CustomerID 
    AND CT.TransactionCode = 'De-install' 
    AND CT.StatusCode <> 'C' 
    AND CT.TrnParm3 IS NOT NULL
INNER JOIN  Mongoose.DnAccountClosureDispositionDetails DACDD
    ON CT.AccountClosureDispositionDetailId = DACDD.AccountClosureDispositionDetailId
INNER JOIN  Mongoose.DnAccountClosureDispositions DACD
    ON DACDD.AccountClosureDispositionId = DACD.AccountClosureDispositionId
WHERE 
    CRS.StateCode = 'IA' 
    AND C.DeInstallDateConfirmed BETWEEN '2024-01-01' AND CONVERT(DATE, GETDATE()) 
    AND DACD.AccountClosureDispositionId = 1  -- Requirement Complete

UNION ALL
-- Record type 6 - switched_vehicle
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
    CT.TrnParm3 EventDate,
    CT.VIN,
    CT.NewVIN
FROM CustSrv.dbo.Customer c
INNER JOIN CustSrv.dbo.CustomerTransaction CT
    ON CT.CustomerID = C.CustomerID
    AND CT.CustomerTransactionTypeID = 49 -- Switch
WHERE
	CT.TrnParm3 IS NOT NULL
;


MERGE databricks.CustomerEvents  AS TA
USING databricks.TmpCustomerEvents AS SO
ON COALESCE(SO.DeviceUsageViolationID,SO.DeviceUsageEventViolationID,SO.CustomerTransactionID) = COALESCE(TA.DeviceUsageViolationID,TA.DeviceUsageEventViolationID,TA.CustomerTransactionID) -- TODO: What if a usageViolationId is the same as one of the others IDs? This will lead to duplicates

-- For Updates
WHEN MATCHED THEN UPDATE SET
	TA.CustomerID = SO.CustomerID,
	TA.DeviceUsageID = SO.DeviceUsageID,
	TA.ViolationReportingApprovalCd = SO.ViolationReportingApprovalCd,
	TA.ViolationReportingApprovalUser = SO.ViolationReportingApprovalUser,
	TA.CreateDate = SO.CreateDate,
	TA.CreateUser = SO.CreateUser,
	TA.ModifyDate = SO.ModifyDate,
	TA.ModifyUser = SO.ModifyUser,
	TA.LogEntryTime = SO.LogEntryTime,
	TA.Eventdate = SO.Eventdate,
	TA.VIN = SO.VIN,
	TA.NewVIN = SO.NewVIN,
	TA.ModificationDate = CAST(GETDATE() AS DATE)

   -- For Inserts
WHEN NOT MATCHED BY TARGET THEN
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
		CAST(GETDATE() AS DATE)
	)
;

SELECT TOP 1 * 
FROM databricks.TmpCustomerEvents;