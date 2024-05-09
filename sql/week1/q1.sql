-- What is the total amount each customer spent at the restaurant?

SELECT customer_id, SUM(price) as total_amount_spent
FROM sales as s
    JOIN menu as m ON s.product_id = m.product_id
GROUP BY
    customer_id
