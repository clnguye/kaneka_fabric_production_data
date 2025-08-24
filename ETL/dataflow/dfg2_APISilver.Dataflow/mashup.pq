[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared nutrients = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "35e0b5a5-904f-46bc-b35c-0f6b5c993a5c"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "368c303b-7cea-4aea-8bfd-4130827f5b28"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "nutrients", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "pivot_nutrients_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "category", DestinationColumnName = "category"], [SourceColumnName = "last_edit_datetime", DestinationColumnName = "last_edit_datetime"], [SourceColumnName = "LastDayOfMonth", DestinationColumnName = "LastDayOfMonth"], [SourceColumnName = "actual_q10_kgs", DestinationColumnName = "actual_q10_kgs"], [SourceColumnName = "planned_q10_kgs", DestinationColumnName = "planned_q10_kgs"], [SourceColumnName = "actual_qh_kgs", DestinationColumnName = "actual_qh_kgs"], [SourceColumnName = "planned_qh_kgs", DestinationColumnName = "planned_qh_kgs"], [SourceColumnName = "oee_net_percent", DestinationColumnName = "oee_net_percent"], [SourceColumnName = "oee_gross_percent", DestinationColumnName = "oee_gross_percent"], [SourceColumnName = "scheduled_downtime_hrs", DestinationColumnName = "scheduled_downtime_hrs"], [SourceColumnName = "scheduled_downtime_percent", DestinationColumnName = "scheduled_downtime_percent"], [SourceColumnName = "equipment_downtime_hrs", DestinationColumnName = "equipment_downtime_hrs"], [SourceColumnName = "equipment_downtime_percent", DestinationColumnName = "equipment_downtime_percent"], [SourceColumnName = "process_downtime_hrs", DestinationColumnName = "process_downtime_hrs"], [SourceColumnName = "process_downtime_percent", DestinationColumnName = "process_downtime_percent"], [SourceColumnName = "trouble_downtime_hrs", DestinationColumnName = "trouble_downtime_hrs"], [SourceColumnName = "trouble_downtime_percent", DestinationColumnName = "trouble_downtime_percent"], [SourceColumnName = "packaging_yield_percent", DestinationColumnName = "packaging_yield_percent"], [SourceColumnName = "yield_qh_percent", DestinationColumnName = "yield_qh_percent"], [SourceColumnName = "yield_q10_percent", DestinationColumnName = "yield_q10_percent"], [SourceColumnName = "var_cost_q10_qh_prod", DestinationColumnName = "var_cost_q10_qh_prod"], [SourceColumnName = "var_cost_q10_q10_prod", DestinationColumnName = "var_cost_q10_q10_prod"], [SourceColumnName = "var_cost_qh_prod", DestinationColumnName = "var_cost_qh_prod"], [SourceColumnName = "maintenance_cost_percent", DestinationColumnName = "maintenance_cost_percent"], [SourceColumnName = "fixed_cost_percent", DestinationColumnName = "fixed_cost_percent"], [SourceColumnName = "global_safety_environmental_comments", DestinationColumnName = "global_safety_environmental_comments"], [SourceColumnName = "global_equipment_process_downtime_comments", DestinationColumnName = "global_equipment_process_downtime_comments"], [SourceColumnName = "global_process_yield_comments", DestinationColumnName = "global_process_yield_comments"], [SourceColumnName = "global_scheduled_downtime_comments", DestinationColumnName = "global_scheduled_downtime_comments"], [SourceColumnName = "global_project_x_comments", DestinationColumnName = "global_project_x_comments"], [SourceColumnName = "ntr_quality_issues", DestinationColumnName = "ntr_quality_issues"], [SourceColumnName = "global_summary_significant_items_month", DestinationColumnName = "global_summary_significant_items_month"], [SourceColumnName = "global_fixed_costs_total_maintenance_comments", DestinationColumnName = "global_fixed_costs_total_maintenance_comments"], [SourceColumnName = "global_variable_cost_comments", DestinationColumnName = "global_variable_cost_comments"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared pivot_nutrients = let
  Source = nutrients,
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(Source, "LastDayOfMonth", each Date.EndOfMonth(#date([year], Date.Month(Date.FromText("01 " & [month] & " " & Number.ToText([year]))), 1))), {{"LastDayOfMonth", type date}}),
  #"Removed columns" = Table.RemoveColumns(#"Added custom", {"event_start_datetime", "year", "month"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns", {"category", "last_edit_datetime", "LastDayOfMonth", "actual_q10_kgs", "planned_q10_kgs", "actual_qh_kgs", "planned_qh_kgs", "oee_net_percent", "oee_gross_percent", "scheduled_downtime_hrs", "scheduled_downtime_percent", "equipment_downtime_hrs", "equipment_downtime_percent", "process_downtime_hrs", "process_downtime_percent", "trouble_downtime_hrs", "trouble_downtime_percent", "packaging_yield_percent", "yield_qh_percent", "yield_q10_percent", "var_cost_q10_qh_prod", "var_cost_q10_q10_prod", "var_cost_qh_prod", "maintenance_cost_percent", "fixed_cost_percent"})
in
  #"Reordered columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "unpivot_nutrients_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "category", DestinationColumnName = "category"], [SourceColumnName = "last_edit_datetime", DestinationColumnName = "last_edit_datetime"], [SourceColumnName = "LastDayOfMonth", DestinationColumnName = "LastDayOfMonth"], [SourceColumnName = "Attribute", DestinationColumnName = "Attribute"], [SourceColumnName = "Value", DestinationColumnName = "Value"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared unpivot_nutrients = let
  Source = pivot_nutrients,
  #"Unpivoted other columns" = Table.UnpivotOtherColumns(Source, {"category", "LastDayOfMonth", "last_edit_datetime"}, "Attribute", "Value"),
  Custom = #"Unpivoted other columns",
  #"Filtered rows" = Table.SelectRows(Custom, each not Text.StartsWith([Attribute], "global")),
  #"Filtered rows 1" = Table.SelectRows(#"Filtered rows", each not Text.EndsWith([Attribute], "issues")),
  #"Changed column type" = Table.TransformColumnTypes(#"Filtered rows 1", {{"Value", type number}})
in
  #"Changed column type";
shared NavigationQuery = let
  Source = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation = Source{[workspaceId = "35e0b5a5-904f-46bc-b35c-0f6b5c993a5c"]}[Data]
in
  Navigation;
shared unpivot_nutrients_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "35e0b5a5-904f-46bc-b35c-0f6b5c993a5c"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "dd3f1275-a11a-4482-b230-ef1761a63b82"]}[Data],
  TableNavigation = Navigation_2{[Item = "unpivot_nutrients", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
shared pivot_nutrients_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "35e0b5a5-904f-46bc-b35c-0f6b5c993a5c"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "dd3f1275-a11a-4482-b230-ef1761a63b82"]}[Data],
  TableNavigation = Navigation_2{[Schema = "dbo", Item = "pivot_nutrients"]}[Data]
in
  TableNavigation;
