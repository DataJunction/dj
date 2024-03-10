SELECT
  default_DOT_hard_hat.last_name,
  default_DOT_hard_hat.first_name,
  default_DOT_hard_hat.birth_date
FROM (
  SELECT
    default_DOT_hard_hats.last_name,
    default_DOT_hard_hats.first_name,
    default_DOT_hard_hats.birth_date
  FROM roads.hard_hats AS default_DOT_hard_hats
  WHERE
    DATE_FORMAT(default_DOT_hard_hats.birth_date, 'yyyyMMdd') =
    DATE_FORMAT(${dj_logical_timestamp}, 'yyyyMMdd')
) AS default_DOT_hard_hat
