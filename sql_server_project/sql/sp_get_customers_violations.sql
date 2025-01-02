USE [StateReporting]
GO
/****** Object:  StoredProcedure [dbo].[GetCustomersViolationsIA]    Script Date: 1/2/2025 5:10:06 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[GetCustomersViolationsIA]
AS

/*
	Autor: Yeison Ortiz
	Company: SourceMeridian
	Short description: SP to get customers violations for IOWA State
	Creation date: 2024-12-27
	Modification date: 2024-12-31
*/

INSERT INTO StateReporting.dbo.CustomerViolations(
	ViolationType,
	CustomerID,
	DeviceUsageViolationID,
	DeviceUsageID,
    ViolationID,
    StartingDeviceLogEntryID,
    EndingDeviceLogEntryID,
    ViolationReportingApprovalCd,
    ViolationReportingApprovalUser,
    ViolationReportingApprovalDate,
    ViolationReported,
    RegulatoryRptgDt,
    Comments,
    CreateDate,
    CreateUser,
    ModifyDate,
    ModifyUser,
	NormalizedDate
	)

-- Violation type 1 and 2
SELECT
	'TYPE 1-2' AS ViolationType,
	[DeviceUsage].CustomerId, 
    [DeviceUsageViolation].* ,
	CAST(FORMAT(ViolationReportingApprovalDate, 'yyyy-MM-dd HH:mm:ss.000') AS DATETIME) AS NormalizedDate

FROM [CustSrv].[dbo].[DeviceUsageViolation]  
INNER JOIN  [CustSrv].[dbo].[DeviceUsage] ON [DeviceUsage].DeviceUsageId  = [DeviceUsageViolation].DeviceUsageId
INNER JOIN [CustSrv].[dbo].[CustomerReportingStates] AS custst ON [DeviceUsage].CustomerId = custst.CustomerID
WHERE
	([DeviceUsageViolation].[ViolationID] = 1 OR [DeviceUsageViolation].[ViolationID] = 11)
	AND (
			[DeviceUsageViolation].[ViolationReportingApprovalCd] = 344 --Approved 
			OR [DeviceUsageViolation].[ViolationReportingApprovalCd] = 345  --Auto-Approved 
		)
	AND custst.StateCode = 'IA'
    AND [DeviceUsageViolation].violationReportingApprovalDate 
        BETWEEN custst.EffectiveStartDate
        AND COALESCE(custst.EffectiveEndDate, GETDATE())

UNION ALL
-- Violation type 3
-- Manual Event Violations to trigger Record
-- (Power Interruption - 30 min)  ID (2)
-- Lockout - Power Off After Car Start (ID 66)
SELECT
	'TYPE 3' AS ViolationType,
	[DeviceUsage].CustomerId, 
    [DeviceUsageViolation].* ,
	CAST(FORMAT(ViolationReportingApprovalDate, 'yyyy-MM-dd HH:mm:ss.000') AS DATETIME) AS NormalizedDate

FROM [CustSrv].[dbo].[DeviceUsageViolation]  
INNER JOIN  [CustSrv].[dbo].[DeviceUsage] ON [DeviceUsage].DeviceUsageId  = [DeviceUsageViolation].DeviceUsageId 
INNER JOIN [CustSrv].[dbo].[CustomerReportingStates] AS custst ON [DeviceUsage].CustomerId = custst.CustomerID
WHERE
	[DeviceUsageViolation].[ViolationID] IN (2,66) 
	AND (
			[DeviceUsageViolation]. [ViolationReportingApprovalCd] = 344 --Approved 
			OR [DeviceUsageViolation]. [ViolationReportingApprovalCd] = 345 --Auto-Approved 
	)
	AND custst.StateCode = 'IA'
    AND [DeviceUsageViolation].violationReportingApprovalDate 
        BETWEEN custst.EffectiveStartDate
        AND COALESCE(custst.EffectiveEndDate, GETDATE())

UNION ALL
SELECT
	'TYPE 3' AS ViolationType,
    DU.CustomerId, 
    DUEV.DeviceUsageEventViolationID AS DeviceUsageViolationID,
    DUEV.DeviceUsageID,
    DUEV.EventViolationCd AS ViolationID,
    Null StartingDeviceLogEntryID,
    Null EndingDeviceLogEntryID,
    Null ViolationReportingApprovalCd,
    Null ViolationReportingApprovalUser,
    DUEV.ViolationDate AS ViolationReportingApprovalDate,
    DUEV.ApprovalStatusInd AS ViolationReported,-- Is it the real field?
    Null RegulatoryRptgDt,
    DUEV.Comments,
    DUEV.ViolationDate AS CreateDate,
    DUEV.CreateUser,
    DUEV.ModifyDate,
    DUEV.ModifyUser,
	CAST(FORMAT(ViolationDate, 'yyyy-MM-dd HH:mm:00.000') AS DATETIME) AS NormalizedDate
FROM [CustSrv].[dbo].[DeviceUsageEventViolation] AS DUEV
INNER JOIN [CustSrv].[dbo].[DeviceUsage] AS DU ON DU.DeviceUsageId  = DUEV.DeviceUsageId
INNER JOIN [CustSrv].[dbo].[CustomerReportingStates] AS custst ON DU.CustomerId = custst.CustomerID
WHERE
	DUEV.EventViolationCd = 965 -- Tamper
	AND custst.StateCode = 'IA'
    AND DUEV.ViolationDate 
        BETWEEN custst.EffectiveStartDate
        AND COALESCE(custst.EffectiveEndDate, GETDATE())

UNION ALL
SELECT
	'TYPE 4' AS ViolationType,
	[DeviceUsage].CustomerId, 
	[DeviceUsageEventViolation].DeviceUsageEventViolationID AS DeviceUsageViolationID,
	[DeviceUsageEventViolation].DeviceUsageID,
	[DeviceUsageEventViolation].EventViolationCd AS ViolationID,
	NULL AS StartingDeviceLogEntryID,
	NULL AS EndingDeviceLogEntryID,
	[DeviceUsageEventViolation].ApprovalStatusInd AS ViolationReportingApprovalCd,
	NULL AS ViolationReportingApprovalUser,
	[DeviceUsageEventViolation].ViolationDate AS ViolationReportingApprovalDate,
	NULL AS ViolationReported,
	NULL AS RegulatoryRptgDt,
	[DeviceUsageEventViolation].Comments,
	[DeviceUsageEventViolation].CreateDate,
	[DeviceUsageEventViolation].CreateUser,
	[DeviceUsageEventViolation].ModifyDate,
	[DeviceUsageEventViolation].ModifyUser,
	CAST(FORMAT(ViolationDate, 'yyyy-MM-dd HH:mm:00.000') AS DATETIME) AS NormalizedDate
	--[DeviceUsageEventViolation].CustomerReportingStateID

FROM [CustSrv].[dbo].[DeviceUsageEventViolation] 
	INNER JOIN [CustSrv].[dbo].[DeviceUsage] ON [DeviceUsage].DeviceUsageId  = [DeviceUsageEventViolation].DeviceUsageId 
WHERE [DeviceUsageEventViolation].[EventViolationCd] = 1099 -- Unauthorized Removal  
	AND [DeviceUsageEventViolation].[ApprovalStatusInd] = 1 ;

SELECT * FROM StateReporting.dbo.CustomerViolations;