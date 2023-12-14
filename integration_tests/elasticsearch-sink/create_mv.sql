CREATE MATERIALIZED VIEW bhv_mv AS
SELECT
    user_id,
    target_id
    event_timestamptz
FROM
    user_behaviors;
