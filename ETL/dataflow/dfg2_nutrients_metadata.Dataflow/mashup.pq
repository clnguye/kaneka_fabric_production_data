[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "table_column_metadata_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "table", DestinationColumnName = "table"], [SourceColumnName = "column", DestinationColumnName = "column"], [SourceColumnName = "description", DestinationColumnName = "description"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared table_column_metadata = let
  Source = Lakehouse.Contents([]),
  #"Navigation 1" = Source{[workspaceId = "35e0b5a5-904f-46bc-b35c-0f6b5c993a5c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "368c303b-7cea-4aea-8bfd-4130827f5b28"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "table_column_metadata", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
shared table_column_metadata_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "35e0b5a5-904f-46bc-b35c-0f6b5c993a5c"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "dd3f1275-a11a-4482-b230-ef1761a63b82"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "table_column_metadata"]}[Data]
in
  TableNavigation;
