-- Which item was purchased first by the customer after they became a member?

SELECT *
FROM sales
    JOIN members on sales.customer_id = members.customer_id
WHERE
    sales.customer_id = 'A'
    AND order_date >= join_date
ORDER BY order_date
LIMIT 1;

SELECT *
FROM sales
    JOIN members on sales.customer_id = members.customer_id
WHERE
    sales.customer_id = 'B'
    AND order_date >= join_date
ORDER BY order_date
LIMIT 1;
