INSERT INTO rg_rules VALUES
 ('r-temp-filter',  'temperature', 'FILTER',    '{"min":-50,"max":150}',  TRUE,  FALSE, 10, 1),
 ('r-temp-convert', 'temperature', 'TRANSFORM', '{"fn":"C_TO_F"}',        TRUE,  TRUE,  20, 1),
 ('r-temp-alert',   'temperature', 'ALERT',     '{"above":30}',           TRUE,  FALSE, 30, 1),
 ('r-hum-filter',   'humidity',    'FILTER',    '{"min":0,"max":100}',    TRUE,  FALSE, 10, 1),
 ('r-hum-alert',    'humidity',    'ALERT',     '{"above":85}',           TRUE,  FALSE, 30, 1),
 ('r-press-filter', 'pressure',    'FILTER',    '{"min":300,"max":1100}', TRUE,  FALSE, 10, 1),
 ('r-press-convert','pressure',    'TRANSFORM', '{"fn":"HPA_TO_PSI"}',    TRUE,  TRUE,  20, 1),
 ('r-press-alert',  'pressure',    'ALERT',     '{"above":1050}',         TRUE,  FALSE, 30, 1),
 ('r-vib-filter',   'vibration',   'FILTER',    '{"min":0,"max":50}',     FALSE, FALSE, 10, 1);
