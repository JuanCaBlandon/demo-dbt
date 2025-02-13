USE [StateReporting]
GO
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- Table into SQL Server
CREATE TABLE [databricks].[StateReportedCustomer](
	[CustomerReportingStateID] [int] NULL,
	[CustomerID] [int] NULL,
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
	[CreateUser] [nvarchar](20) NULL,
	[ModifyDate] [datetime] NULL,
	[ModifyUser] [nvarchar](20) NULL,
	[RepeatOffender] [nvarchar](1) NULL,
	[OffenseDate] [date] NULL,
	[IIDStartDate] [date] NULL,
	[IIDEndDate] [date] NULL,
	[CreationDate] [date] NULL
) ON [PRIMARY];


CREATE TABLE [databricks].[FtpCustomerData](
	[VendorName] [nvarchar](30) NULL,
	[DriversLicenseNumber] [nvarchar](50) NULL,
	[LastName] [nvarchar](80) NULL,
	[FirstName] [nvarchar](80) NULL,
	[MiddleName] [nvarchar](80) NULL,
	[DateOfBirth] [date] NULL,
	[VIN] [nvarchar](30) NULL,
	[OffenseDate] [date] NULL,
	[RepeatOffender] [nvarchar](1) NULL,
	[IIDStartDate] [date] NULL,
	[IIDEndDate] [date] NULL,
	[CreationDate] [date] NULL
) ON [PRIMARY];

CREATE TABLE [StateReporting].[databricks].[TmpStateReportedCustomer](
	[CustomerReportingStateID] [int] NULL,
	[CustomerID] [int] NULL,
	[DriversLicenseNumber] [nvarchar](30) NULL,
	[FirstName] [nvarchar](80) NULL,
	[LastName] [nvarchar](80) NULL,
	[MiddleName] [nvarchar](80) NULL,
	[DateOfBirth] [date] NULL,
	[VIN] [nvarchar](50) NULL,
	[InstallDate] [datetime] NULL,
	[DeInstallDate] [datetime] NULL,
	[StateCode] [nvarchar](2) NULL,
	[ActiveStatus] [bit] NULL,
	[ReportStatusCD] [nvarchar](20) NULL,
	[CustomerStatus] [int] NULL,
	[ActiveStatusStartDate] [datetime] NULL,
	[EffectiveStartDate] [datetime] NULL,
	[EffectiveEndDate] [datetime] NULL,
	[DeviceLogRptgClassCd] [int] NULL,
	[CreateDate] [datetime] NULL,
	[CreateUser] [nvarchar](20) NULL,
	[ModifyDate] [datetime] NULL,
	[ModifyUser] [nvarchar](20) NULL,
	[RepeatOffender] [nvarchar](1) NULL,
	[OffenseDate] [date] NULL,
	[IIDStartDate] [date] NULL,
	[IIDEndDate] [date] NULL,
	[CreationDate] [date] NULL
) ON [PRIMARY];

--Can be suer for Tmp
CREATE TABLE [databricks].[CustomerEvents](
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
) ON [PRIMARY]

GO
