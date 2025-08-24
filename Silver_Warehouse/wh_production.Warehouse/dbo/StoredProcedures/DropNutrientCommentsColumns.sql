CREATE PROC DropNutrientCommentsColumns
AS
BEGIN
ALTER TABLE [wh_production].[dbo].[pivot_nutrients] 
DROP COLUMN 
global_safety_environmental_comments,
global_equipment_process_downtime_comments,
global_process_yield_comments,
global_scheduled_downtime_comments,
global_project_x_comments,
ntr_quality_issues,
global_summary_significant_items_month,
global_fixed_costs_total_maintenance_comments,
global_variable_cost_comments
END;