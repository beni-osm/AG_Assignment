SELECT 
    transaction_date,
    user_id,
    SUM(CASE WHEN type = 'deposit' THEN amount ELSE 0 END) AS total_deposit,
    SUM(CASE WHEN type = 'withdrawal' THEN -amount ELSE 0 END) AS total_withdrawal
FROM 
    `assignment-arben.AssignmentDataset.Transactions`
GROUP BY 
    transaction_date, user_id
ORDER BY 
    transaction_date, user_id;
