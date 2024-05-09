-- What was the first item from the menu purchased by each customer?

SELECT customer_id, MIN(order_date) as first_order_date
FROM sales
GROUP BY
    customer_id
