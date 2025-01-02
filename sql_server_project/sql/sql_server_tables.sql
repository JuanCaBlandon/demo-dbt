USE [StateReporting]
GO
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- Table into SQL Server
CREATE TABLE [dbo].[StateReportedCustomer](
	[CustomerReportingStateID] [int] NULL,
	[CustomerID] [int]  NULL,
	[StateCode] [nvarchar](2) NULL,
    [ActiveStatus] [bit] NULL,
	[CustomerStatus] [int] NULL,
    [ActiveStatusStartDate] [datetime] NULL,
    [ActiveStatusEndDate ] [datetime] NULL,
    [ReportStatusCD] [nvarchar](30) NULL,
	[FirstReportDate] [datetime] NULL,
	[StopReportDate] [datetime] NULL,
    [InstallDate] [datetime] NULL,
    [DeInstallDate] [datetime] NULL,
	[CreateDate] [datetime] NULL,
	[CreateUser] [nvarchar] (20) NULL,
	[ModifyDate] [datetime] NULL,
	[ModifyUser] [nvarchar] (20) NULL,
	[RepeatOffender] [nvarchar](1)  NULL,
	[OffenseDate] [datetime] NULL,
	[IIDStartDate] [datetime] NULL,
	[IIDEndDate] [datetime] NULL,
	[CreationDate] [date] NULL
) ON [PRIMARY]


CREATE TABLE [dbo].[FtpCustomerData](
	[VendorName] [nvarchar](30) NULL,
	[DriversLicenseNumber] [nvarchar] (20) NULL,
	[LastName] [nvarchar] (20) NULL,
    [FirstName] [nvarchar] (20) NULL,
	[MiddleName] [nvarchar] (20) NULL,
    [DateOfBirth] [datetime] NULL,
	[VIN] [nvarchar] (20) NULL,
	[OffenseDate] [datetime] NULL,
    [RepeatOffender] [nvarchar](1)  NULL,
    [IIDStartDate] [datetime] NULL,
    [IIDEndDate] [datetime] NULL,
    [CreationDate] [date] NULL
) ON [PRIMARY]

CREATE TABLE [dbo].[TpmStateReportedCustomer](
	[CustomerReportingStateID] [int] NULL,
	[CustomerID] [int]  NULL,
	[DriversLicenseNumber] [nvarchar](30),
	[FirstName] [nvarchar](50),
    [LastName] [nvarchar](50),
    [MiddleName] [nvarchar](50),
    [DateOfBirth][datetime],
    [VIN] [nvarchar](50),
    [InstallDate] [datetime],
    [DeInstallDate] [datetime],
	[StateCode] [nvarchar](2) NULL,
    [ActiveStatus] [bit] NULL,
	[ReportStatusCD] [nvarchar](20),
	[CustomerStatus] [int] NULL,
    [ActiveStatusStartDate] [datetime] NULL,
	[EffectiveStartDate] [datetime] NULL,
	[EffectiveEndDate] [datetime] NULL,
	[DeviceLogRptgClassCd] [int] NULL,
	[CreateDate] [datetime] NULL,
	[CreateUser] [nvarchar] (20) NULL,
	[ModifyDate] [datetime] NULL,
	[ModifyUser] [nvarchar] (20) NULL,
	[RepeatOffender] [nvarchar](1)  NULL,
	[OffenseDate] [datetime] NULL,
	[IIDStartDate] [datetime] NULL,
	[IIDEndDate] [datetime] NULL,
	[CreationDate] [date] NULL
) ON [PRIMARY]

CREATE TABLE [dbo].[CustomerViolations](
	[ViolationType] [nvarchar](15) NULL,
	[CustomerID] [bigint]  NULL,
    [DeviceUsageViolationID] [bigint]  NULL,
	[DeviceUsageID] [int]  NULL,
    [ViolationID] [int]  NULL,
    [StartingDeviceLogEntryID] [bigint]  NULL,
    [EndingDeviceLogEntryID] [bigint]  NULL,
    [ViolationReportingApprovalCd] [int]  NULL,
    [ViolationReportingApprovalUser] [nvarchar](50) NULL,
    [ViolationReportingApprovalDate][datetime] NULL,
    [ViolationReported] [bit] NULL,
    [RegulatoryRptgDt] [datetime] NULL,
    [Comments] [nvarchar](255) NULL,
    [CreateDate] [datetime] NULL,
    [CreateUser] [nvarchar](20) NULL,
    [ModifyDate] [datetime] NULL,
    [ModifyUser] [nvarchar](20) NULL,
) ON [PRIMARY]

GO
