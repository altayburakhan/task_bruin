/* @bruin

name: analytics.gsheets_mongodb_join
description: Join Google Sheets and MongoDB data for employee and user analysis
type: duckdb.sql
connection: duckdb_output

materialization:
    type: table

@bruin */

-- Google Sheets + MongoDB Employee-User Analysis
-- FULLY OPTIMIZED VERSION: Maximum performance and readability

WITH 
-- Google Sheets employee data 
gsheets_employees AS (
    SELECT 
        employee_id as gsheets_id,
        name as employee_name,
        city as employee_city,
        salary as employee_salary,
        position,
        experience_years,
        -- Employee categories 
        CASE 
            WHEN salary >= 80000 THEN 'Senior'
            WHEN salary >= 60000 THEN 'Mid-Level'
            ELSE 'Junior'
        END as experience_level,
        
        -- Optimized position grouping 
        CASE 
            WHEN position LIKE '%Engineer%' OR position LIKE '%Developer%' OR position LIKE '%DevOps%' THEN 'Tech'
            WHEN position LIKE '%Manager%' OR position LIKE '%Product%' THEN 'Business'
            WHEN position LIKE '%Analyst%' OR position LIKE '%Data%' THEN 'Data'
            ELSE 'Other'
        END as position_group
    FROM public.gsheets
    WHERE employee_id IS NOT NULL 
      AND salary > 0
),

-- MongoDB user summary 
mongodb_user_summary AS (
    SELECT 
        user_id as mongodb_user_id,
        name as mongodb_user_name,
        city as mongodb_user_city,
        age as mongodb_user_age,
        salary as mongodb_user_salary
    FROM public.mongo_users
    WHERE user_id IS NOT NULL
      AND name IS NOT NULL
),

-- Pre-processed names for faster JOIN 
clean_names AS (
    SELECT 
        gsheets_id,
        employee_name,
        employee_city,
        employee_salary,
        position,
        experience_years,
        experience_level,
        position_group,
        -- Pre-clean names for JOIN
        LOWER(TRIM(employee_name)) as clean_employee_name,
        LOWER(TRIM(employee_city)) as clean_employee_city
    FROM gsheets_employees
),

clean_users AS (
    SELECT 
        mongodb_user_id,
        mongodb_user_name,
        mongodb_user_city,
        mongodb_user_age,
        mongodb_user_salary,
        -- Pre-clean names for JOIN
        LOWER(TRIM(mongodb_user_name)) as clean_user_name,
        LOWER(TRIM(mongodb_user_city)) as clean_user_city
    FROM mongodb_user_summary
)

-- Final optimized result
SELECT 
    -- Employee information 
    cn.gsheets_id,
    cn.employee_name,
    cn.employee_city,
    cn.employee_salary,
    cn.position,
    cn.experience_years,
    cn.experience_level,
    cn.position_group,
    
    -- MongoDB user information 
    cu.mongodb_user_id,
    cu.mongodb_user_name,
    cu.mongodb_user_city,
    cu.mongodb_user_age,
    cu.mongodb_user_salary,
    
    -- Cross-platform analysis 
    CASE 
        WHEN cu.mongodb_user_id IS NOT NULL THEN 'Employee + Customer'
        ELSE 'Employee Only'
    END as user_category,
    
    -- Salary comparison 
    CASE 
        WHEN cu.mongodb_user_salary IS NOT NULL AND cn.employee_salary > cu.mongodb_user_salary THEN 'Employee Higher'
        WHEN cu.mongodb_user_salary IS NOT NULL AND cn.employee_salary < cu.mongodb_user_salary THEN 'Customer Higher'
        WHEN cu.mongodb_user_salary IS NOT NULL THEN 'Equal'
        ELSE 'No Customer Data'
    END as salary_comparison,
    
    -- Geographic analysis 
    CASE 
        WHEN cn.clean_employee_city = cu.clean_user_city THEN 'Same City'
        WHEN cu.clean_user_city IS NOT NULL THEN 'Different City'
        ELSE 'No Customer Location'
    END as location_match,
    
    -- Age comparison 
    CASE 
        WHEN cu.mongodb_user_age IS NOT NULL AND cn.experience_years > cu.mongodb_user_age THEN 'Employee More Experienced'
        WHEN cu.mongodb_user_age IS NOT NULL AND cn.experience_years < cu.mongodb_user_age THEN 'Customer Older'
        WHEN cu.mongodb_user_age IS NOT NULL THEN 'Similar Age'
        ELSE 'No Customer Age Data'
    END as age_comparison

FROM clean_names cn
LEFT JOIN clean_users cu ON 
    cn.clean_employee_name = cu.clean_user_name
    AND cn.clean_employee_city = cu.clean_user_city

ORDER BY cn.employee_salary DESC, cu.mongodb_user_salary DESC NULLS LAST;
