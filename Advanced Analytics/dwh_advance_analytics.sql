--- Advanced Data Analytics

-- 1) Change Over Time

SELECT 
	   YEAR(order_date) as year,
	   MONTH(order_date) as month,
	   SUM(sales_amount) as revenue,
	   COUNT(distinct customer_key) as  total_customers,
	   SUM(quantity) as total_quantity
FROM gold.fact_sales
where order_date is not null
group by YEAR(order_date), MONTH(order_date)
order by 1,2


-- 2) Cumulative analysis 
	
		-- calculate the total sales per month
		-- and the running total of sales over time



select 
	order_date,
	total_sales,
	sum(total_sales) over(order by order_date) as running_sales_total,
	avg(avg_price) over(order by order_date) as moving_average
from
(select 
	DATETRUNC(year, order_date) as order_date,
	sum(sales_amount) as total_sales,
	avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by DATETRUNC(year, order_date))t
go




-- 3) Performance Analysis


-- Analyse the yearly performance of products by comparing their sales 
-- to both the average sales perfomance and previous year's sales

with yearly_product_sales as (
select year(fs.order_date) as order_year,pr.product_name,sum(fs.sales_amount) as current_sales
from gold.fact_sales fs
left join gold.dim_products pr
on pr.product_key = fs.product_key
where order_date is not null
group by year(order_date),product_name
)
select 
	order_year,
	product_name,
	current_sales,
	avg(current_sales) over (partition by product_name) as avg_yearly_sales,
	current_sales - avg(current_sales) over (partition by product_name) as diff_avg,
	case when current_sales - avg(current_sales) over (partition by product_name) > 0  then 'Above Avg'
		 when current_sales - avg(current_sales) over (partition by product_name) < 0  then 'Below Avg'
		 else 'Avg'
	end as avg_change,
    lag(current_sales) over(partition by product_name order by order_year) as py_sales,
	current_sales -lag(current_sales) over(partition by product_name order by order_year) as yoy_difference,
	case when current_sales - -lag(current_sales) over(partition by product_name order by order_year) > 0  then 'Increase'
		 when current_sales - -lag(current_sales) over(partition by product_name order by order_year) < 0  then 'Decrease'
		 else 'No change'
	end as year_change
from yearly_product_sales
order by product_name,order_year


--- 4) Proportional Analysis

--- which categories contribute the most to overall sales


with cte as (
select  
	pr.category, 
	sum(fs.sales_amount) as total_sales
from gold.fact_sales fs
left join gold.dim_products pr
on fs.product_key = pr.product_key
group by pr.category
)
select 
	category,total_sales, 
	sum(total_sales) over() overall_sales,
	concat(round((cast(total_sales as float)/ sum(total_sales) over()) * 100,2),' %') percentage_of_total
from cte
order by total_sales desc


--- 5) Data Segmentation

with product_segments as (
Select 
	product_key,
	product_name ,
	cost,
	case when cost < 100 then 'Below 100'
		 when cost between 100 and 500 then '100-500'
		 when cost between 500 and 1000 then '500-1000'
		 else 'Above 1000'
    end segment
from gold.dim_products)

select segment, count(product_key) as total_products
from  product_segments
group by segment
order by total_products desc	


--- Grouping customers based on their spending behavior : 
with custoemr_spending as (
select
	c.customer_key,
	sum(fs.sales_amount) as total_spending,
	min(order_date)as  first_order,
	max(order_date) as last_order,
	DATEDIFF(month,min(order_date),max(order_date)) as lifespan
from gold.fact_sales fs
left join gold.dim_customers c
on fs.customer_key = c.customer_key
group by c.customer_key)

select customer_segment,
	   count(customer_key) as total_customers
from (
select 
	customer_key,
	case when lifespan>= 12 and total_spending>5000 then 'VIP'
		 when lifespan>= 12 and total_spending<=5000 then 'Regular'
	     else 'New'
	end customer_segment
from custoemr_spending
) t
group by customer_segment
order by total_customers desc

/*
============================================== 
			   Customer Report
==============================================
*/

CREATE VIEW gold.report_customers as  
WITH base_query AS 
(
SELECT
		s.order_number,
		s.product_key,
		s.order_date,
		s.sales_amount,
		s.quantity,
		c.customer_key,
		c.customer_number,
		CONCAT(c.first_name,' ',c.last_name) customer_name,
		DATEDIFF(year,c.birth_date,GETDATE()) age
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
WHERE s.order_date is not null
),
customer_aggregation as (
SELECT 
	    customer_key,
		customer_number,
		customer_name,
		age,
		COUNT(distinct order_number) as total_orders,
		SUM(sales_amount) as total_sales,
		SUM(quantity) as total_quantity,
		COUNT(distinct product_key) as total_products,
		MAX(order_date) as last_order_date,
		DATEDIFF(MONTH, MIN(order_date),MAX(order_date)) as lifespan
FROM base_query
GROUP BY 
		customer_key,
		customer_number,
		customer_name,
		age)

SELECT 	
		customer_key,
		customer_number,
		customer_name,
		age,
		CASE 
			WHEN age < 20 THEN 'Under 20'
			WHEN age BETWEEN 20 AND 29 THEN '20-29'
			WHEN age BETWEEN 30 AND 39 THEN '30-39'
			WHEN age BETWEEN 40 AND 49 THEN '40-49'
			ELSE 'Above 50'
		END AS age_group,
		CASE 
			WHEN lifespan >=12 AND total_sales >5000 THEN 'VIP'
			WHEN lifespan >=12 AND total_sales <= 5000 THEN 'Regular'
			ELSE 'New'
		END AS customer_segment,
		last_order_date,
		DATEDIFF(MONTH,last_order_date,GETDATE()) AS recency,
	    total_orders,
		total_sales,
		total_quantity,
		total_products,
		lifespan,
		CASE WHEN total_sales = 0 then 0 
			 ELSE total_sales/total_orders
		END AS avg_order_value,
		CASE WHEN lifespan = 0 then 0
			 ELSE total_sales/lifespan
		END AS avg_monthly_spend
FROM customer_aggregation




/*
=======================================
	 		PRODUCT REPORT
=======================================
*/


WITH base_query AS 
(
SELECT
		s.order_number,
		s.order_date,
		s.customer_key,
		s.sales_amount,
		s.quantity,
		s.product_key,
		p.product_name,	
		p.category,
		p.subcategory,
		p.cost
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p 
ON s.product_key = p.product_key
WHERE s.order_date is not null
),
product_aggregation as (
SELECT 
	    product_key,
		product_name,
		category,
		subcategory,
		cost,
		DATEDIFF(MONTH, MIN(order_date),MAX(order_date)) as lifespan,
		COUNT(distinct order_number) as total_orders,
		COUNT(distinct customer_key) as total_customers,
		SUM(sales_amount) as total_sales,
		SUM(quantity) as total_quantity,
		MAX(order_date) as last_sale_date,
		round(avg(cast(sales_amount as float)/nullif(quantity,0)),1) as avg_selling_price
FROM base_query
GROUP BY 
	    product_key,
		product_name,
		category,
		subcategory,
		cost		
)

SELECT 	
	    product_key,
		product_name,
		category,
		subcategory,
		cost,
		last_sale_date,
		DATEDIFF(MONTH,last_sale_date,GETDATE()) AS recency,
		CASE 
			WHEN total_sales > 50000 THEN 'High-Perfomer'
			WHEN total_sales >= 10000 THEN 'Mid Range'
			ELSE 'Low-Performer'
		END AS product_segment,
		lifespan,
	    total_orders,
		total_sales,
		total_quantity,
		total_customers,
		avg_selling_price,
		CASE 
			WHEN total_orders = 0 THEN 0
			ELSE total_sales / total_orders
		END AS avg_order_revenue,
		CASE 
			WHEN lifespan = 0 THEN 0
			ELSE total_sales/lifespan
		END AS avg_monthly_revenue
FROM product_aggregation
