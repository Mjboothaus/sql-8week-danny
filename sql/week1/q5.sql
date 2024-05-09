-- Which item was the most popular for each customer?

SELECT customer_id, product_id, COUNT(product_id) as n_purchase
FROM sales
GROUP BY
    customer_id,
    product_id
ORDER BY n_purchase DESC
