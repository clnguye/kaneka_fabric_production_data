CREATE TABLE [dbo].[unpivot_nutrients] (

	[category] varchar(8000) NULL, 
	[last_edit_datetime] datetime2(6) NULL, 
	[LastDayOfMonth] date NULL, 
	[Attribute] varchar(8000) NULL, 
	[Value] float NULL
);