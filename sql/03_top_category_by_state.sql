-- п.3. Категория наибольшей транзакции по каждому штату.
-- argMax(cat_id, amount) возвращает cat_id строки с максимальной суммой в группе.
SELECT
    us_state,
    argMax(cat_id, amount) AS category,
    max(amount)            AS max_amount
FROM hw2.transactions
GROUP BY us_state
ORDER BY us_state;
