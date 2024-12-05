SELECT u.id, u.name,t.transaction_date, t.amount
FROM `assignment-arben.AssignmentDataset.Users` u
JOIN `assignment-arben.AssignmentDataset.Transactions` t
ON u.id = t.user_id
WHERE t.type = 'deposit'
AND PARSE_DATE('%Y-%m-%d', t.transaction_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);
