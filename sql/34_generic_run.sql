-- function (replace <ARTIFACT>):
-- CREATE FUNCTION apply_rules_generic AS 'demo.iot.ApplyRulesGenericPtf' USING JAR 'confluent-artifact://<ARTIFACT>' USING CONNECTIONS(`<ENV_NAME>`.`<CLUSTER_NAME>`.`lightning_connection`);
INSERT INTO rg_processed
SELECT event_kind, device_id, sensor_type, raw_value, `value`, unit, alert, rule_id, rule_version, action, fire_count, first_fired, event_ts, status, device_seen
FROM apply_rules_generic(
  input => TABLE rg_readings PARTITION BY device_id,
  env => '<ENV_ID>', cluster => '<CLUSTER_ID>',
  rulesTable => 'rg_rules', firedTable => 'rg_fired',
  uid => 'generic-v1');
