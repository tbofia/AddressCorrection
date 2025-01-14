USE [STAGINGDB]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[star].[dim_Address_USPS]') AND type in (N'U'))
DROP TABLE [star].[dim_Address_USPS]
GO


CREATE TABLE [star].[dim_Address_USPS](
	[loadkey] [int] IDENTITY(1,1) NOT NULL,
	[responsedate] [datetime] NOT NULL,
	[responsemessage] [varchar](8000) NULL,
	[responseaccuracy] [varchar](8000) NULL,
	[responsetypes] [varchar](8000) NULL,
	[responsematchcount] [int] NULL,
	[NOTES] [char](255) NULL,
	[SOURCE_SYSTEM_KEY] [int] NOT NULL,
	[SOURCE_SYSTEM_ADDRESS_LINE_1] [char](255) NULL,
	[SOURCE_SYSTEM_ADDRESS_LINE_2] [char](255) NULL,
	[SOURCE_SYSTEM_CITY] [char](255) NULL,
	[SOURCE_SYSTEM_STATE] [varchar](2) NULL,
	[SOURCE_SYSTEM_COUNTY] [varchar](255) NULL,
	[SOURCE_SYSTEM_ZIP] [varchar](11) NULL,
	[ADDRESS_LINE_1] [char](255) NULL,
	[ADDRESS_LINE_2] [char](255) NULL,
	[CITY] [char](255) NULL,
	[STATE] [varchar](2) NULL,
	[COUNTY] [varchar](255) NULL,
	[ZIP] [varchar](11) NULL,
	[COUNTRY] [varchar](10) NULL,
	[LATITUDE] [varchar](50) NULL,
	[LONGITUDE] [varchar](50) NULL
) ON [PRIMARY]
GO


