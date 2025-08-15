/* @bruin

name: analytics.mysql_mongodb_join
description: Join MySQL and MongoDB data for cross-platform user analysis
type: duckdb.sql
connection: duckdb_output

materialization:
    type: table

@bruin */

-- MySQL + MongoDB Cross-Platform User Analysis
-- This transformation joins data from MySQL and MongoDB to create unified user profiles

WITH 
-- MySQL user order summary 
mysql_summary AS (
    SELECT 
        u.id as mysql_user_id,
        u.email as mysql_email,
        CONCAT(u.first_name, ' ', u.last_name) as mysql_name,
        u.city as mysql_city,
        u.age as mysql_age,
        COUNT(o.id) as mysql_order_count,
        COALESCE(SUM(o.total_amount), 0) as mysql_total_spent,
        AVG(o.total_amount) as mysql_avg_order_value
    FROM public.mysql_users u
    LEFT JOIN public.mysql_orders o ON u.user_id = o.user_id
    WHERE u.email IS NOT NULL
    GROUP BY u.id, u.email, u.first_name, u.last_name, u.city, u.age
),

-- MongoDB user order summary 
mongodb_summary AS (
    SELECT 
        u.user_id as mongodb_user_id,
        u.email as mongodb_email,
        u.name as mongodb_name,
        u.city as mongodb_city,
        u.age as mongodb_age,
        u.salary as mongodb_salary,
        COUNT(o.order_id) as mongodb_order_count,
        COALESCE(SUM(o.price * o.quantity), 0) as mongodb_total_spent,
        AVG(o.price * o.quantity) as mongodb_avg_order_value
    FROM public.mongo_users u
    LEFT JOIN public.mongo_orders o ON u.user_id = o.user_id
    WHERE u.email IS NOT NULL
    GROUP BY u.user_id, u.email, u.name, u.city, u.age, u.salary
),

-- Pre-calculated totals for better performance
combined_metrics AS (
    SELECT 
        COALESCE(mysql.mysql_email, mongo.mongodb_email) as primary_email,
        COALESCE(mysql.mysql_name, mongo.mongodb_name) as primary_name,
        COALESCE(mysql.mysql_city, mongo.mongodb_city) as primary_city,
        COALESCE(mysql.mysql_age, mongo.mongodb_age) as primary_age,
        
        -- MySQL data
        mysql.mysql_user_id,
        mysql.mysql_name,
        mysql.mysql_city,
        mysql.mysql_age,
        mysql.mysql_order_count,
        mysql.mysql_total_spent,
        mysql.mysql_avg_order_value,
        
        -- MongoDB data
        mongo.mongodb_user_id,
        mongo.mongodb_name,
        mongo.mongodb_city,
        mongo.mongodb_age,
        mongo.mongodb_salary,
        mongo.mongodb_order_count,
        mongo.mongodb_total_spent,
        mongo.mongodb_avg_order_value,
        
        -- Pre-calculated totals 
        (COALESCE(mysql.mysql_order_count, 0) + COALESCE(mongo.mongodb_order_count, 0)) as total_orders,
        (COALESCE(mysql.mysql_total_spent, 0) + COALESCE(mongo.mongodb_total_spent, 0)) as total_spent
        
    FROM mysql_summary mysql
    FULL OUTER JOIN mongodb_summary mongo ON mysql.mysql_email = mongo.mongodb_email
)

-- Final optimized result
SELECT 
    -- Primary user info
    primary_email as user_email,
    primary_name as user_name,
    primary_city as user_city,
    primary_age as user_age,
    
    -- MySQL profile
    mysql_user_id,
    mysql_name,
    mysql_city,
    mysql_age,
    mysql_order_count,
    mysql_total_spent,
    mysql_avg_order_value,
    
    -- MongoDB profile
    mongodb_user_id,
    mongodb_name,
    mongodb_city,
    mongodb_age,
    mongodb_salary,
    mongodb_order_count,
    mongodb_total_spent,
    mongodb_avg_order_value,
    
    -- Cross-platform metrics
    total_orders,
    total_spent,
    
    -- Platform presence 
    CASE WHEN mysql_user_id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_mysql_profile,
    CASE WHEN mongodb_user_id IS NOT NULL THEN 'Yes' ELSE 'No' END as has_mongodb_profile,
    
    -- User type 
    CASE 
        WHEN mysql_user_id IS NOT NULL AND mongodb_user_id IS NOT NULL THEN 'Cross-Platform User'
        WHEN mysql_user_id IS NOT NULL THEN 'MySQL Only'
        WHEN mongodb_user_id IS NOT NULL THEN 'MongoDB Only'
        ELSE 'Unknown'
    END as user_type,
    
    -- Revenue tier 
    CASE 
        WHEN total_spent >= 1000 THEN 'High Value'
        WHEN total_spent >= 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END as revenue_tier

FROM combined_metrics
ORDER BY total_spent DESC, total_orders DESC;
