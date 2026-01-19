--rownumber, rank, dense_rank functions

Select *,ROW_NUMBER() over (partition by amount order by amount desc) as rn
from customers;

Select *,RANK() over (partition by amount order by amount desc) as rn
from customers;

Select *,DENSE_RANK() over (partition by amount order by amount desc) as rn
from customers;


-- running total per customer

Select customer_id,  amount, sum(amount) over (partition by customer_id  order by order_date) as running_total
from customers;


-- rows vs range
-- 7 day rolling sum using rows
Select  date,sum(revenue) over (order by date 
rows between 6 preceding and current row) AS rolling_7_day_revenue
from daily_revenue;

-- 7 day rolling sum using range
Select  date,sum(revenue) over (order by date 
range between 6 preceding and current row) AS rolling_7_day_revenue
from daily_revenue;



-- lag and lead functions
-- compute day over day revenue growth

select date, revenue, revenue - lag(revenue) over (order by date) as revenue_growth
from daily_revenue;


-- de-duplication using window functions

select * from (
    select *, ROW_NUMBER() over (partition by customer_id order by update_ts desc) as rn
    from customers
) t
where rn = 1;


-- identifying top N records per group
---For each customer, return top 3 orders by amount.

Select a.* from 
(Select *, rownumber() over (partition by customer order by amount desc) as 
rn )a where rn<=3