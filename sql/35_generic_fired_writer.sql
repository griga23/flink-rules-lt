INSERT INTO rg_fired
SELECT CONCAT(rule_id, '|', device_id), rule_id, rule_version, device_id, action, raw_value, `value`, fire_count, first_fired, event_ts
FROM rg_processed
WHERE event_kind = 'FIRED';
