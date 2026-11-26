CREATE TABLE rg_fired (
  fired_key STRING NOT NULL,
  rule_id STRING,
  rule_version BIGINT,
  device_id STRING,
  action STRING,
  raw_value DOUBLE,
  result_value DOUBLE,
  fire_count BIGINT,
  first_fired BIGINT,
  last_fired BIGINT,
  PRIMARY KEY (fired_key) NOT ENFORCED
) WITH (
  'changelog.mode' = 'upsert',
  'kafka.cleanup-policy' = 'compact',
  'value.format' = 'json-registry',
  'key.format' = 'json-registry'
);
