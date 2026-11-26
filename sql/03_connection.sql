-- PLACEHOLDER credentials first (expect HTTP 401 = network reachable); replace with a Global API key later
CREATE CONNECTION lightning_connection WITH (
  'type' = 'REST',
  'endpoint' = 'https://sql.eu-central-1.aws.confluent.cloud/query/v1',
  'username' = 'placeholder',
  'password' = 'placeholder'
);
