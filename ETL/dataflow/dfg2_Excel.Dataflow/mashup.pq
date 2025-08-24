[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Nutrients Scorecard FY2024 KPIs xlsx_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Nutrients FY2024", DestinationColumnName = "Nutrients FY2024"], [SourceColumnName = "Target", DestinationColumnName = "Target"], [SourceColumnName = "Month", DestinationColumnName = "Month"], [SourceColumnName = "Value", DestinationColumnName = "Value"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"Nutrients Scorecard FY2024 KPIs xlsx" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "35e0b5a5-904f-46bc-b35c-0f6b5c993a5c"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "368c303b-7cea-4aea-8bfd-4130827f5b28"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "OneDrive_1_5-22-2025"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Nutrients Scorecard FY2024 KPIs.xlsx"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(#"Navigation 4", null, true),
  #"Navigation 5" = #"Imported Excel workbook"{[Item = "2024 KPI Dashboard", Kind = "Sheet"]}[Data],
  #"Choose columns" = Table.SelectColumns(#"Navigation 5", {"Column1", "Column2", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16"}),
  #"Kept range of rows" = Table.Range(#"Choose columns", 1, 20),
  #"Filled down" = Table.FillDown(#"Kept range of rows", {"Column1"}),
  #"Replaced value" = Table.ReplaceValue(#"Filled down", "N/A", null, Replacer.ReplaceValue, {"Column1", "Column2", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16"}),
  Custom = Table.InsertRows(#"Replaced value", 17, {[
    Column1 = "Variable cost Q10", 
    Column2 = "Q10 prod. actual", 
    Column4 = (if #"Replaced value"{15}[Column4] = null then 0 else #"Replaced value"{15}[Column4]) + (if #"Replaced value"{16}[Column4] = null then 0 else #"Replaced value"{16}[Column4]), 
    Column5 = (if #"Replaced value"{15}[Column5] = null then 0 else #"Replaced value"{15}[Column5]) + (if #"Replaced value"{16}[Column5] = null then 0 else #"Replaced value"{16}[Column5]), 
    Column6 = (if #"Replaced value"{15}[Column6] = null then 0 else #"Replaced value"{15}[Column6]) + (if #"Replaced value"{16}[Column6] = null then 0 else #"Replaced value"{16}[Column6]), 
    Column7 = (if #"Replaced value"{15}[Column7] = null then 0 else #"Replaced value"{15}[Column7]) + (if #"Replaced value"{16}[Column7] = null then 0 else #"Replaced value"{16}[Column7]),  
    Column8 = (if #"Replaced value"{15}[Column8] = null then 0 else #"Replaced value"{15}[Column8]) + (if #"Replaced value"{16}[Column8] = null then 0 else #"Replaced value"{16}[Column8]), 
    Column9 = (if #"Replaced value"{15}[Column9] = null then 0 else #"Replaced value"{15}[Column9]) + (if #"Replaced value"{16}[Column9] = null then 0 else #"Replaced value"{16}[Column9]),  
    Column10 = (if #"Replaced value"{15}[Column10] = null then 0 else #"Replaced value"{15}[Column10]) + (if #"Replaced value"{16}[Column10] = null then 0 else #"Replaced value"{16}[Column10]), 
    Column11 = (if #"Replaced value"{15}[Column11] = null then 0 else #"Replaced value"{15}[Column11]) + (if #"Replaced value"{16}[Column11] = null then 0 else #"Replaced value"{16}[Column11]),  
    Column12 = (if #"Replaced value"{15}[Column12] = null then 0 else #"Replaced value"{15}[Column12]) + (if #"Replaced value"{16}[Column12] = null then 0 else #"Replaced value"{16}[Column12]),  
    Column13 = (if #"Replaced value"{15}[Column13] = null then 0 else #"Replaced value"{15}[Column13]) + (if #"Replaced value"{16}[Column13] = null then 0 else #"Replaced value"{16}[Column13]),  
    Column14 = (if #"Replaced value"{15}[Column14] = null then 0 else #"Replaced value"{15}[Column14]) + (if #"Replaced value"{16}[Column14] = null then 0 else #"Replaced value"{16}[Column14]), 
    Column15 = (if #"Replaced value"{15}[Column15] = null then 0 else #"Replaced value"{15}[Column15]) + (if #"Replaced value"{16}[Column15] = null then 0 else #"Replaced value"{16}[Column15]), 
    Column16 = null]}),
  #"Custom 1" = Table.InsertRows(Custom, 18, {[
    Column1 = "Variable cost Q10", 
    Column2 = "Q10 prod. target", 
    Column4 = (if Custom{17}[Column4] = Custom{15}[Column4] then 259 else 262), 
    Column5 = (if Custom{17}[Column5] = Custom{15}[Column5] then 259 else 262),
    Column6 = (if Custom{17}[Column6] = Custom{15}[Column6] then 259 else 262),
    Column7 = (if Custom{17}[Column7] = Custom{15}[Column7] then 259 else 262),
    Column8 = (if Custom{17}[Column8] = Custom{15}[Column8] then 259 else 262),
    Column9 = (if Custom{17}[Column9] = Custom{15}[Column9] then 259 else 262),
    Column10 = (if Custom{17}[Column10] = Custom{15}[Column10] then 259 else 262),
    Column11 = (if Custom{17}[Column11] = Custom{15}[Column11] then 259 else 262),
    Column12 = (if Custom{17}[Column12] = Custom{15}[Column12] then 259 else 262),
    Column13 = (if Custom{17}[Column13] = Custom{15}[Column13] then 259 else 262),
    Column14 = (if Custom{17}[Column14] = Custom{15}[Column14] then 259 else 262),
    Column15 = (if Custom{17}[Column15] = Custom{15}[Column15] then 259 else 262),
    Column16 = null]}),
  #"Removed alternate rows" = Table.AlternateRows(#"Custom 1", 15, 2, 20),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed alternate rows", [PromoteAllScalars = true]),
  #"Added custom" = Table.AddColumn(#"Promoted headers", "Timestamp", each DateTime.LocalNow()),
  #"Unpivoted columns" = Table.UnpivotOtherColumns(#"Added custom", {"Nutrients FY2024", "Target"}, "Month", "Value"),
  #"Changed column type" = Table.TransformColumnTypes(#"Unpivoted columns", {{"Nutrients FY2024", type text}, {"Target", type text}, {"Value", type number}})
in
  #"Changed column type";
shared #"Nutrients Scorecard FY2024 KPIs xlsx_DataDestination" = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "35e0b5a5-904f-46bc-b35c-0f6b5c993a5c"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "dd3f1275-a11a-4482-b230-ef1761a63b82"]}[Data],
  TableNavigation = Navigation_2{[Item = "nutrient_kpi_dataflow", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
