select team_name,count(*) as no_of_matches_played,sum(flag) as no_of_matches_won,(count(*)-sum(flag)) as no_of_matches_loss from (
select team_1 as team_name,
case 
when team_1=winner 
then 1 else 0 end as flag from icc_world_cup
union all
select team_2 as team_name,
case when team_2=winner then 1 else 0 end as flag from icc_world_cup
) a
group by team_name
order by no_of_matches_won desc;

--------------------------------------------------------------------------------------------------------------------------------

select a.order_date,
sum(case when a.order_date=a.first_order_date then 1 else 0 end) as new_customer,
sum(case when a.order_date!=a.first_order_date then 1 else 0 end) as old_customer
from (
select customer_id,order_date,min(order_date) over (partition by customer_id) as first_order_date from customer_orders
) a
group by  a.order_date

-------------------------------------------------------------------------------------------------------------------------
--1) which product has the highest price? only return a single row.
SELECT TOP 1 product_name,price from products order by price desc;

--2) which customer has made most orders?
select o.customer_id,first_name,count(c.customer_id) as [total orders]
from customers c
join orders o on c.customer_id=o.customer_id
GROUP BY o.customer_id,first_name

--3) Total revevenue by each product
select p.product_id,p.product_name,sum(price*quantity) as total_revenue_per_product from products p
inner join order_items o on p.product_id=o.product_id
group by p.product_id,p.product_name
order by sum(price*quantity) desc

--4)Find the day with highest revenue
select order_date,sum(quantity*price) as revenue 
from orders o
join order_items oi on o.order_id=oi.order_id
join products p on p.product_id=oi.product_id
group by order_date 
order by 2 desc

--5)Find the first order for each customer
select o.customer_id,concat(first_name,' ',last_name) as name,
min(order_date) as first_order_date
from customers c
join orders o on c.customer_id=o.customer_id
group by o.customer_id ,first_name,last_name;

---------------------------------------------------------------------------------------------------------------------------------------

SELECT	
		DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) 
			AS dept_ranking,
		department,
		employee_id, 
		full_name, 
		salary
FROM employee;
