select *
from ecommerce_transactions;

select distinct Payment_Method
from ecommerce_transactions;

-- created a new table for working on data
create table ecommerce_transactions_stagging
like ecommerce_transactions;

select *
from ecommerce_transactions_stagging;

insert ecommerce_transactions_stagging
select * from ecommerce_transactions;

---- DATA CLEANING ----
-- 1. Remove Duplicates
-- 2. Standardize the Data\
-- 3. Null values or blank values
-- 4. Remove Any columns

select Distinct User_Name
from ecommerce_transactions_stagging;

-- to find any duplicates present in data or not
select *,
row_number() OVER(PARTITION BY Transaction_ID,User_Name,Country,Product_Category,Purchase_Amount,Payment_Method,Transaction_Date) as row_num
from ecommerce_transactions_stagging;

with duplicate_cte as
(
select *,
row_number() OVER(PARTITION BY Transaction_ID,User_Name,Country,Product_Category,Purchase_Amount,Payment_Method,Transaction_Date) as row_num
from ecommerce_transactions_stagging
)
select * from duplicate_cte
where row_num >1;

select Transaction_Date
from ecommerce_transactions_stagging;

-- changing the datatype of date from text to date
alter table ecommerce_transactions_stagging
modify column Transaction_Date Date;


----- EDA
select *
from ecommerce_transactions_stagging;

-- Calculate total purchase amount by country
select Country,sum(Purchase_Amount)
from ecommerce_transactions_stagging
group by Country;


-- Calculate the max, min, sum, and average purchase amounts by country
select Country,max(Purchase_Amount) max_amt,
min(Purchase_Amount) min_amt,
sum(Purchase_Amount) sum_amt,
avg(Purchase_Amount) avg_amt
from ecommerce_transactions_stagging
group by Country;


-- Count the total number of transactions
select count(Transaction_ID) as total_sales
from ecommerce_transactions_stagging;


-- Calculate total purchase amount by country and payment method, and show percentage contribution
with country_cte as
(
select Country as country,Payment_Method as method,sum(Purchase_Amount) as sum_amt
from ecommerce_transactions_stagging
group by Country,Payment_Method
order by Country, sum_amt Desc
)
select country, 
method,
sum_amt,
concat(format((sum_amt/sum(sum_amt) over(partition by country)*100),2),"%") as Percentage_by_eachCountryTotal
from country_cte;


-- Count the transactions per product category, ordered by transaction count
select distinct Product_Category, 
count(Transaction_ID) over(partition by Product_Category) as transaction_count
from ecommerce_transactions_stagging
order by transaction_count Desc;


-- Create a temporary table to hold product category transaction counts (session-based)
-- this will work only till this session, run again when u comeback
create temporary table product_count as
select distinct Product_Category, 
count(Transaction_ID) over(partition by Product_Category) as transaction_count
from ecommerce_transactions_stagging
order by transaction_count Desc;


-- Rank product categories based on transaction count in ascending order
-- this will work only till this session, run again when u comeback
select Product_Category ,
rank() over( order by transaction_count asc) as Rank_
from product_count;


-- Calculate revenue by age brackets and show the percentage contribution
with percentage_cte as
(
select
distinct
case
	when Age > 60 then "Old"
    when Age < 30 then "Young"
    when Age >=30 then "Middle age"
end as Age_Brackets,
sum(Purchase_Amount) as revenue
from ecommerce_transactions_stagging
group by Age_Brackets
)
select
Age_Brackets,
revenue,
concat(format((revenue*100.0/(select sum(revenue) from percentage_cte)),2),"%") as percentage
from percentage_cte
order by percentage DESC;


-- Calculate revenue by payment method and show the percentage contribution
with payment_method_cte as
(
select distinct Payment_Method as method,
sum(Purchase_Amount) as revenue
from ecommerce_transactions_stagging
group by Payment_Method
)
select 
method,
revenue,
concat(format((revenue*100.0/(select sum(revenue) from payment_method_cte)),2),"%") as percentage
from payment_method_cte
order by percentage Desc;

-- Calculate revenue by month and show the percentage contribution
with month_cte as
(
select distinct monthname(Transaction_Date) as month_,
month(Transaction_Date) as month_order,
sum(Purchase_Amount) as revenue
from ecommerce_transactions_stagging
group by month_,month_order
)
select month_,
revenue,
concat(format((revenue*100.0/(select sum(revenue) from month_cte)),2),"%") as percentage
from month_cte
order by month_order asc;

-- Calculate revenue and average revenue by year, with percentage contribution to average revenue
with year_cte as
(
select distinct Year(Transaction_Date) as year_,
sum(Purchase_Amount) as revenue,
avg(Purchase_Amount) as avg_revenue
from ecommerce_transactions_stagging
group by year_
)
select year_,
revenue,
avg_revenue,
concat(format((avg_revenue*100.0/(select sum(avg_revenue) from year_cte)),2),"%") as percentage
from year_cte;
