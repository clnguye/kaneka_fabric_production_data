CREATE PROC AddNutrientCommentsColumns
AS
BEGIN
ALTER TABLE [wh_production].[dbo].[pivot_nutrients] 
ADD global_safety_environmental_comments VARCHAR(8000),
global_equipment_process_downtime_comments VARCHAR(8000),
global_process_yield_comments VARCHAR(8000),
global_scheduled_downtime_comments VARCHAR(8000),
global_project_x_comments VARCHAR(8000),
ntr_quality_issues VARCHAR(8000),
global_summary_significant_items_month VARCHAR(8000),
global_fixed_costs_total_maintenance_comments VARCHAR(8000),
global_variable_cost_comments VARCHAR(8000)
END;