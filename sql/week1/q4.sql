-- What is the most purchased item on the menu and how many times was it purchased by all customers?

SELECT product_id, COUNT(product_id) as n_purchase
FROM sales
GROUP BY
    product_id
ORDER BY n_purchase DESC
