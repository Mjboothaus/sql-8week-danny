--  Which item was purchased just before the customer became a member?

SELECT *
FROM sales
    JOIN members on sales.customer_id = members.customer_id
WHERE
    sales.customer_id = 'A'
    AND order_date < join_date
ORDER BY order_date
LIMIT 1
