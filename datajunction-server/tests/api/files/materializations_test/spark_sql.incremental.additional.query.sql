WITH default_DOT_hard_hat_2 AS (
  SELECT
    default_DOT_hard_hats.last_name,
    default_DOT_hard_hats.first_name,
    default_DOT_hard_hats.birth_date,
    default_DOT_hard_hats.country
  FROM roads.hard_hats AS default_DOT_hard_hats
  WHERE
    DATE_FORMAT(default_DOT_hard_hats.birth_date, 'yyyyMMdd') =
    DATE_FORMAT(${dj_logical_timestamp}, 'yyyyMMdd')
)
SELECT
  default_DOT_hard_hat_2.last_name,
  default_DOT_hard_hat_2.first_name,
  default_DOT_hard_hat_2.birth_date,
  default_DOT_hard_hat_2.country
FROM default_DOT_hard_hat_2
