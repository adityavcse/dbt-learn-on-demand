-- import CTEs

with customers as 
(
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

paid_orders as 
(
select * from {{ ref('int_orders') }}
),


final as 
(
select
order_id,
customers.customer_id,
order_placed_at,
order_status,
total_amount_paid,
payment_finalized_date,
customer_first_name,
customer_last_name,
-- sales txn seq
ROW_NUMBER() OVER (ORDER BY order_id) as transaction_seq,
-- customer sales swq
ROW_NUMBER() OVER (PARTITION BY customers.customer_id ORDER BY order_id) as customer_sales_seq,

-- new vs returning
case when (rank() over(partition by customers.customer_id order by order_placed_at,order_id ) =1)
    then 'New' 
    else 'return' end as nvsr,

sum(total_amount_paid) over(partition by customers.customer_id order by order_placed_at) as customer_lifetime_value,

first_value(order_placed_at) over(partition by customers.customer_id order by order_placed_at) as fdos
FROM paid_orders
left join 
customers on paid_orders.customer_id=customers.customer_id

)

select * from final
order by order_id