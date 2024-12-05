WITH LatestPreferences AS (
    SELECT 
        user_id,
        preferred_language,
        notifications_enabled,
        marketing_opt_in,
        updated_at,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS rn
    FROM `assignment-arben.AssignmentDataset.UserPreferences`
)
SELECT 
    u.id AS user_id,
    u.name,
    u.email,
    u.registration_date,
    p.preferred_language,
    p.notifications_enabled,
    p.marketing_opt_in,
    p.updated_at AS latest_updated_at
FROM `assignment-arben.AssignmentDataset.Users` u
LEFT JOIN LatestPreferences p
ON u.id = p.user_id
WHERE p.rn = 1 OR p.rn IS NULL
ORDER BY u.id;
