/* @bruin

name: analytics.employee_salary_insights
type: duckdb.sql
connection: duckdb_output

materialization:
    type: table

columns:
  - name: employee_id
    type: text
    checks:
      - name: not_null
      - name: unique
  - name: salary
    type: bigint
    checks:
      - name: positive
  - name: salary_percentile
    type: double
    checks:
      - name: not_null

@bruin */

-- Employee Salary Analysis and Statistics
-- Simplified and optimized for DuckDB performance

WITH base_data AS (
    SELECT 
        employee_id,
        company,
        position,
        city,
        experience_years,
        salary,
        is_remote,
        -- Pre-calculate all averages in one CTE
        AVG(salary) OVER () as overall_avg,
        AVG(salary) OVER (PARTITION BY city) as city_avg,
        AVG(salary) OVER (PARTITION BY company) as company_avg,
        AVG(salary) OVER (PARTITION BY position) as position_avg
    FROM public.gsheets 
    WHERE employee_id IS NOT NULL AND salary > 0
)

SELECT 
    employee_id,
    company,
    position,
    city,
    experience_years,
    salary,
    CASE WHEN is_remote THEN 'Remote' ELSE 'Office' END as work_mode,
    
    -- Salary comparisons (simplified)
    ROUND((salary - overall_avg) / overall_avg * 100, 2) as salary_vs_avg_pct,
    ROUND((salary - city_avg) / city_avg * 100, 2) as salary_vs_city_pct,
    ROUND((salary - company_avg) / company_avg * 100, 2) as salary_vs_company_pct,
    ROUND((salary - position_avg) / position_avg * 100, 2) as salary_vs_position_pct,
    
    -- Percentile hesaplaması (window functions korundu)
    PERCENT_RANK() OVER (ORDER BY salary) * 100 as salary_percentile,
    PERCENT_RANK() OVER (PARTITION BY city ORDER BY salary) * 100 as city_percentile,
    PERCENT_RANK() OVER (PARTITION BY company ORDER BY salary) * 100 as company_percentile,
    PERCENT_RANK() OVER (PARTITION BY position ORDER BY salary) * 100 as position_percentile,
    
    -- Kategoriler (simplified)
    CASE 
        WHEN salary < overall_avg * 0.7 THEN 'Below Market'
        WHEN salary < overall_avg * 0.9 THEN 'Below Average'
        WHEN salary < overall_avg * 1.1 THEN 'Average'
        WHEN salary < overall_avg * 1.3 THEN 'Above Average'
        ELSE 'Premium'
    END as salary_category,
    
    CASE 
        WHEN experience_years < 2 THEN 'Junior'
        WHEN experience_years < 5 THEN 'Mid-Level'
        WHEN experience_years < 10 THEN 'Senior'
        ELSE 'Expert'
    END as experience_level,
    
    -- Performans göstergeleri (simplified)
    CASE 
        WHEN salary > position_avg * 1.2 THEN 'High Performer'
        WHEN salary > position_avg * 1.1 THEN 'Good Performer'
        WHEN salary > position_avg * 0.9 THEN 'Average Performer'
        ELSE 'Needs Review'
    END as performance_indicator

FROM base_data
ORDER BY salary DESC;
