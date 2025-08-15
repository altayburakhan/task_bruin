/* @bruin

name: analytics.user_order_analysis
type: duckdb.sql
connection: duckdb_output

materialization:
    type: table

columns:
  - name: user_id
    type: text
    checks:
      - name: not_null
      - name: unique
  - name: total_orders
    type: bigint
    checks:
      - name: not_null
  - name: total_spent
    type: decimal
    checks:
      - name: not_null
  - name: avg_order_value
    type: decimal
    checks:
      - name: not_null

@bruin */

-- User Order Analysis
-- This transformation analyzes users ordering behavior.

WITH user_orders AS (
    SELECT 
        o.user_id,
        COUNT(*) as total_orders,
        SUM(o.total_amount) as total_spent,
        AVG(o.total_amount) as avg_order_value,
        COUNT(DISTINCT o.category) as unique_categories
    FROM public.mysql_orders o
    GROUP BY o.user_id
),

user_profiles AS (
    SELECT 
        u.user_id,
        u.age,
        u.city,
        u.is_active
    FROM public.mysql_users u
),

-- Pre-calculated metrics for better performance
user_metrics AS (
    SELECT 
        up.user_id,
        up.age,
        up.city,
        up.is_active,
        
        -- Order metrics 
        COALESCE(uo.total_orders, 0) as total_orders,
        COALESCE(uo.total_spent, 0) as total_spent,
        COALESCE(uo.avg_order_value, 0) as avg_order_value,
        COALESCE(uo.unique_categories, 0) as unique_categories_purchased,
        
        -- Pre-calculated flags
        CASE WHEN uo.user_id IS NOT NULL THEN 1 ELSE 0 END as has_orders
        
    FROM user_profiles up
    LEFT JOIN user_orders uo ON up.user_id = uo.user_id
)

-- Final optimized result
SELECT 
    um.user_id,
    um.age,
    um.city,
    
    -- User status 
    CASE WHEN um.is_active = 1 THEN 'Active' ELSE 'Inactive' END as user_status,
    
    -- Order metrics
    um.total_orders,
    um.total_spent,
    um.avg_order_value,
    um.unique_categories_purchased,
    
    -- Customer segmentation 
    CASE 
        WHEN um.has_orders = 0 THEN 'No Orders'
        WHEN um.total_spent < 1000 THEN 'Low Value'
        WHEN um.total_spent < 5000 THEN 'Medium Value'
        WHEN um.total_spent < 10000 THEN 'High Value'
        ELSE 'VIP'
    END as customer_segment,
    
    -- Order frequency analysis 
    CASE 
        WHEN um.has_orders = 0 THEN 'No Orders'
        WHEN um.total_orders = 1 THEN 'One-time Buyer'
        WHEN um.total_orders <= 3 THEN 'Occasional Buyer'
        WHEN um.total_orders <= 7 THEN 'Regular Buyer'
        ELSE 'Frequent Buyer'
    END as purchase_frequency,
    
    -- Age groups 
    CASE 
        WHEN um.age < 25 THEN '18-24'
        WHEN um.age < 35 THEN '25-34'
        WHEN um.age < 45 THEN '35-44'
        WHEN um.age < 55 THEN '45-54'
        ELSE '55+'
    END as age_group

FROM user_metrics um
ORDER BY um.total_spent DESC, um.total_orders DESC;
