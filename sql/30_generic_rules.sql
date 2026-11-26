CREATE TABLE rg_rules (
  rule_id STRING NOT NULL,
  rule_scope STRING,
  rule_type STRING,
  params STRING,
  enabled BOOLEAN,
  one_shot BOOLEAN,
  priority INT,
  version BIGINT,
  PRIMARY KEY (rule_id) NOT ENFORCED
) WITH (
  'changelog.mode' = 'upsert',
  'kafka.cleanup-policy' = 'compact',
  'value.format' = 'json-registry',
  'key.format' = 'json-registry'
);
