-- Graph 1: distribution of orders delivered on time, early and late
select case 
when r_date.date < c_date.date then 'early' 
when r_date.date = c_date.date then 'on time' 
when r_date.date > c_date.date then 'late'
end delivery_status,
count(*) as total_count
from fact_sales f
join dim_date c_date 
on f.commitdatekey = c_date.iddate
join dim_date r_date 
on f.receiptdatekey = r_date.iddate
group by delivery_status
order by delivery_status;

-- Graph 2: total revenue by discount category
select case 
when f.discount = 0 then 'no discount'
when f.discount between 0.01 and 0.03 then 'low (1-3%)'
when f.discount between 0.04 and 0.07 then 'medium (4-7%)'
when f.discount > 0.07 then 'high (>7%)'
end discount_category,
sum(f.total_amount) total_revenue
from fact_sales f
group by discount_category
order by total_revenue desc;


-- Graph 3: monthly revenue in 1992
select d.month, sum(f.total_amount) as monthly_revenue
from fact_sales f
join dim_date d 
on f.shipdatekey = d.iddate
where d.year = 1992
group by d.month
order by d.month;

-- Graph 4: the number of orders grouped by their value
select case 
when total_amount < 50000 then '0. <50k'
when total_amount between 50000 and 100000 then '1. 50k-100k'
when total_amount between 100000 and 150000 then '2. 100k-150k'
when total_amount between 150000 and 200000 then '3. 150k-200k'
when total_amount between 250000 and 300000 then '4. 250k-300k'
when total_amount > 300000 then '5. >300k'
end order_category, count(*) number_of_orders
from fact_sales
where total_amount is not null
group by order_category
order by order_category;

-- Graph 5: top best-selling products
select p.name as product_name, sum(f.quantity) as total_sold
from fact_sales f
join dim_part p 
on f.partkey = p.partkey
group by p.name
order by sum(f.quantity) desc
limit 5;

-- Graph 6: distribution of delivery delays
select datediff(day, c_date.date, r_date.date) days_late,
count(*) total_orders
from fact_sales f
join dim_date c_date 
on f.commitdatekey = c_date.iddate
join dim_date r_date 
on f.receiptdatekey = r_date.iddate
group by days_late
order by days_late;