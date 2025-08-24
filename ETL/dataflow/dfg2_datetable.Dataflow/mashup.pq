[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "date_table_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Date", DestinationColumnName = "Date"], [SourceColumnName = "Day", DestinationColumnName = "Day"], [SourceColumnName = "Quarter", DestinationColumnName = "Quarter"], [SourceColumnName = "Month Name", DestinationColumnName = "Month Name"], [SourceColumnName = "Month", DestinationColumnName = "Month"], [SourceColumnName = "Month Yr", DestinationColumnName = "Month Yr"], [SourceColumnName = "Yr Month", DestinationColumnName = "Yr Month"], [SourceColumnName = "Year", DestinationColumnName = "Year"], [SourceColumnName = "Weekend", DestinationColumnName = "Weekend"], [SourceColumnName = "Week of year", DestinationColumnName = "Week of year"], [SourceColumnName = "Week of month", DestinationColumnName = "Week of month"], [SourceColumnName = "Start of quarter", DestinationColumnName = "Start of quarter"], [SourceColumnName = "End of quarter", DestinationColumnName = "End of quarter"], [SourceColumnName = "Start of month", DestinationColumnName = "Start of month"], [SourceColumnName = "End of month", DestinationColumnName = "End of month"], [SourceColumnName = "Days in month", DestinationColumnName = "Days in month"], [SourceColumnName = "Fiscal year", DestinationColumnName = "Fiscal year"], [SourceColumnName = "Fiscal month", DestinationColumnName = "Fiscal month"], [SourceColumnName = "Fiscal quarter", DestinationColumnName = "Fiscal Quarter"], [SourceColumnName = "Fiscal Year End", DestinationColumnName = "Fiscal Year End"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared date_table = let
  #"Min Date" = #date(2023, 1, 1),
  #"Max Date" = #date(Date.Year(DateTime.LocalNow()), 12, 31),
  Source = {Number.From(#"Min Date")..Number.From(#"Max Date")},
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Changed column type" = Table.TransformColumnTypes(#"Converted to table", {{"Column1", type date}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Column1", "Date"}}),
  #"Inserted day name" = Table.AddColumn(#"Renamed columns", "Day", each Date.DayOfWeekName([Date]), type nullable text),
  #"Inserted quarter" = Table.AddColumn(#"Inserted day name", "Quarter", each "Q" & Number.ToText(Date.QuarterOfYear([Date])), type nullable text),
  #"Inserted month name" = Table.AddColumn(#"Inserted quarter", "Month Name", each Date.MonthName([Date]), type nullable text),
  #"Inserted month" = Table.AddColumn(#"Inserted month name", "Month", each Date.Month([Date]), type nullable number),
  #"Inserted month year" = Table.AddColumn(#"Inserted month", "Month Yr", each Date.MonthName([Date]) & " " & Number.ToText(Date.Year([Date])), type nullable text),
  #"Inserted numerical year.month" = Table.AddColumn(#"Inserted month year", "Yr Month", each  Number.ToText(Date.Year([Date])) & "." & Number.ToText(Date.Month([Date]),"00"), type nullable text),
  #"Inserted year" = Table.AddColumn(#"Inserted numerical year.month", "Year", each Date.Year([Date]), type nullable number),
  #"Inserted conditional column" = Table.AddColumn(#"Inserted year", "Weekend", each if Text.StartsWith([Day], "S") then "Yes" else "No", type nullable text),
  #"Inserted week of year" = Table.AddColumn(#"Inserted conditional column", "Week of year", each Date.WeekOfYear([Date]), type nullable number),
  #"Inserted week of month" = Table.AddColumn(#"Inserted week of year", "Week of month", each Date.WeekOfMonth([Date]), type nullable number),
  #"Inserted start of quarter" = Table.AddColumn(#"Inserted week of month", "Start of quarter", each Date.StartOfQuarter([Date]), type nullable date),
  #"Inserted end of quarter" = Table.AddColumn(#"Inserted start of quarter", "End of quarter", each Date.EndOfQuarter([Start of quarter]), type nullable date),
  #"Inserted start of month" = Table.AddColumn(#"Inserted end of quarter", "Start of month", each Date.StartOfMonth([Date]), type nullable date),
  #"Inserted end of month" = Table.AddColumn(#"Inserted start of month", "End of month", each Date.EndOfMonth([Start of month]), type nullable date),
  #"Inserted days in month" = Table.AddColumn(#"Inserted end of month", "Days in month", each Date.DaysInMonth([End of month]), type nullable number),
  #"Inserted fiscal year" = Table.AddColumn(#"Inserted days in month", "Fiscal year", each if [Month]>=fiscal_year_start then [Year] else [Year]-1, type nullable number),
  #"Inserted fiscal month" = Table.AddColumn(#"Inserted fiscal year", "Fiscal month", each if [Month]>=fiscal_year_start then [Month]-(fiscal_year_start-1) else [Month]+(12-fiscal_year_start+1), type nullable number),
  #"Inserted fiscal quarter" = Table.AddColumn(#"Inserted fiscal month", "Fiscal quarter", each "Q" & Number.ToText(Number.RoundUp([Fiscal month]/3)), type nullable text),
  #"Inserted fiscal year end" = Table.TransformColumnTypes(Table.AddColumn(#"Inserted fiscal quarter", "Fiscal Year End", each if [Date]=[End of month] and [Fiscal month] = 12 then 1 else 0), {{"Fiscal Year End", type number}})
  in
   #"Inserted fiscal year end";
shared fiscal_year_start = 4 meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type number, List = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, DefaultValue = 1];
shared date_table_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "35e0b5a5-904f-46bc-b35c-0f6b5c993a5c"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "dd3f1275-a11a-4482-b230-ef1761a63b82"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "date_table"]}[Data]
in
  TableNavigation;
