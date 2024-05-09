-- What is the total items and amount spent for each member before they became a member?
SELECT count(*)
FROM sales
    JOIN members on sales.customer_id = members.customer_id
WHERE
    sales.customer_id = 'A'
    AND order_date < join_date
