# flink-rules-lt — Flink PTF + Lightning Tables (Confluent Cloud)

Process Faker-generated IoT data in Confluent Cloud Flink with a Process Table Function (PTF) that applies
processing rules (filter / transform / alert / one-shot) stored in **Lightning Tables**. The record of what
already fired is also kept in a Lightning Table, so one-shot rules survive a statement recreate.
First written 2026-11-20, generic version re-documented 2026-11-23. Everything below was verified on a live
cluster unless marked *(docs only)* or *(reasoning)*. Full write-up with diagram: `docs/flink-rules-lt.html`.

Only the generic version (`apply_rules_generic`) exists in the source tree. The earlier prototype classes (`ApplyRulesPtf`, `ApplyRulesRemotePtf`,
`ApplyRulesStatefulPtf`, `ApplyRulesDurablePtf`, `LookupRule`), the unused parts of `RuleClient` and the old SQL files (`sql/00`–`24` except `03_connection.sql`)
were deleted on 2026-11-23. The folder is not a git repository, so they are not recoverable.

## TL;DR findings

| Question | Answer |
|---|---|
| Can a PTF call a remote service directly (raw `HttpClient`)? | **No.** Connect times out (`HttpConnectTimeoutException`). Egress is blocked. |
| Can a PTF use external connectivity (`CREATE CONNECTION` + `USING CONNECTIONS` + `getJobParameter`)? | **Yes, in practice.** **Docs say it is unsupported for `ProcessTableFunction`** (only scalar/table UDFs), so this is undocumented behaviour and may change. |
| Can Flink SQL join/look up a Lightning Table directly? | Not documented; Lightning queries reject joins/subqueries/aggregates. We call the Lightning **HTTP query API** from inside the PTF. |
| Does PTF state survive delete → recreate (same `uid`)? | **No.** State belongs to the statement (`device_seen` restarts). |
| How do one-shot rules survive a recreate? | `rg_fired` (Lightning) is scanned at startup and restores per-(rule, device) counts into state. **Works.** |

## Environment

- Org: `<ORG_NAME>` (`<ORG_ID>`)
- Environment: `<ENV_NAME>` = `<ENV_ID>` (Flink catalog name `<ENV_NAME>`)
- Kafka cluster: `<CLUSTER_ID>` = `<CLUSTER_NAME>` (Basic, AWS `eu-central-1`; Flink database name = cluster name)
- Flink compute pool: `<POOL_ID>` (default pool, AWS eu-central-1)
- Lightning query endpoint: `https://sql.eu-central-1.aws.confluent.cloud/query/v1`
- Lightning Tables require AWS + a supported region (11 listed in docs), a Schema Registry schema on the topic.

## Architecture

```
+-----------------------------------------------+          +----------------------------------------+
| PTF (Java: ApplyRulesGenericPtf + RuleClient) |          | SQL (Flink statements and DDL)         |
|                                               |          |                                        |
| open(): RuleClient reads connection params    |          | rg_readings (Faker table, 3 rows/s)    |
| first eval (2), once per subtask:             |          |    |                                   |
|   SELECT * rg_rules + rg_fired (HTTPS,        |          |    v                                   |
|   via lightning_connection)                   |          | Statement 1: INSERT INTO rg_processed  |
| per-device state: seen, fireCount,            |          |   SELECT ... FROM apply_rules_generic( |
|   firstFired (TTL 30 d), restored from        | <--- (3) |     input => TABLE rg_readings         |
|   history on first touch                      |          |       PARTITION BY device_id, ...)     |
| eval (4): rules by priority                   |          |                                        |
|   FILTER / TRANSFORM / ALERT,                 |          | Statement 2 (writer):                  |
|   one-shot skipped if count > 0               | (5) ---> |   INSERT INTO rg_fired SELECT ...      |
| returns FIRED per rule that acted,            |          |   FROM rg_processed                    |
|   then RESULT (unless dropped)                |          |   WHERE event_kind = 'FIRED'           |
|                                               |          | DDL: TABLE / CONNECTION / FUNCTION     |
+-----------------------------------------------+          +----------------------------------------+

Kafka topics (separate from the SQL; the first two are also Lightning Tables):
  rg_rules      compacted, key rule_id            <- (1) INSERT INTO rg_rules (SQL); PTF scans it at (2)
  rg_fired      compacted, key rule_id|device_id  <- (7) Statement 2 upserts; PTF scans it at (2)
  rg_processed  plain topic, keyless, 1 d         <- (5) Statement 1 inserts FIRED + RESULT; (6) Statement 2 reads it
```

`docs/flink-rules-lt.html` has the full diagram: PTF box (Java) on the left, SQL box on the right, Kafka topics below, numbered steps match the ones above.

Flow: (1) rules are written to `rg_rules`; (2) at startup each PTF subtask scans `rg_rules` and `rg_fired` once
(`SELECT *`) and fails fast if a scan fails; (3) readings arrive keyed by `device_id`; (4) per reading the PTF walks the
sensor's rules in `priority` order, restoring count/first-fired from the loaded history on first touch of a (rule, device);
(5) it emits one `FIRED` row per rule that acted, then a `RESULT` row unless a FILTER dropped the reading; (6)–(7) the writer
copies `FIRED` rows into `rg_fired`.

### Tables

- `rg_rules` (Lightning, compacted upsert): `rule_id` PK, `rule_scope` (sensor type; **`scope` is reserved in Flink SQL**),
  `rule_type` FILTER|TRANSFORM|ALERT, `params` JSON, `enabled`, `one_shot`, `priority`, `version`.
- `rg_fired` (Lightning, compacted upsert): `fired_key` PK = `rule_id|device_id`, `rule_id`, `rule_version`, `device_id`, `action`,
  `raw_value`, `result_value`, `fire_count`, `first_fired`, `last_fired`. Monitoring table AND durable one-shot record.
- `rg_readings` (Faker, 3 rows/s, devices dev-A/B/C, sensors temperature/humidity/pressure, value uniform 0–1100).
- `rg_processed` (topic, `json-registry`, retention 1 d): `event_kind`, `device_id`, `sensor_type`, `raw_value`, `value`, `unit`, `alert`,
  `rule_id`, `rule_version`, `action`, `fire_count`, `first_fired`, `event_ts`, `status`, `device_seen`.

### Rule semantics

All rules compare the **raw** reading. Disabled rules are not loaded (no enabled rule ⇒ `NO_RULE`).
- `FILTER {"min","max"}`: outside range ⇒ action `DROPPED`, no `RESULT` row, later rules not evaluated.
- `TRANSFORM {"fn":"C_TO_F"|"HPA_TO_PSI"}`: sets `value` and `unit`. Unknown fn ⇒ `BAD_RULE:<id>` in status.
- `ALERT {"above"}`: raw above threshold ⇒ `alert = true`.
- `one_shot`: acts only the first time per (rule, device); later readings get `SKIPPED_ONE_SHOT:<rule_id>` in `RESULT.status`.
- `RESULT.status` values: `OK`, `NO_RULE`, `SKIPPED_ONE_SHOT:<rule_id>`, `BAD_RULE:<rule_id>` (comma-joined if several).

Example data (`sql/32`): per sensor a filter (temperature -50..150, humidity 0..100, pressure 300..1100), one-shot
convert for temperature (`C_TO_F`) and pressure (`HPA_TO_PSI`), alerts (temp > 30, humidity > 85, pressure > 1050);
`r-vib-filter` for vibration is disabled. Because Faker values are 0–1100, most temperature/humidity readings are dropped.

## Code (`src/main/java/demo/iot/`)

| Class | Registered as | Purpose |
|---|---|---|
| `ApplyRulesGenericPtf` | `apply_rules_generic` | Set-semantic PTF (`PARTITION BY device_id`). Loads rules + fired history once (first `eval`, because env/cluster/table names are eval arguments), `DeviceState{seen, fireCount, firstFired}` with `@StateHint(ttl="30 days")`, emits `FIRED` + `RESULT`. |
| `RuleClient` | – | Reads connection params in `open()`, runs `SELECT *` against the Lightning query API with Basic auth. `scanAll` = one HTTP request (60 s timeout, no paging), lowercases column names, values are strings. Throws on any error. |

Build: `mvn -q clean package` → `target/flink-rules-lt-1.0.0.jar` (shaded; Flink API is `provided`,
Jackson is bundled). Must compile with `-parameters` (already in `pom.xml`) or PTF state/argument names fail.
Java release 11, Flink API 2.1.0. The jar holds 5 classes (`ApplyRulesGenericPtf` + 3 inner classes, `RuleClient`).
Deployed artifact: `iot-rules-generic-v2` = `<ARTIFACT_ID>` (2026-11-23), the trimmed jar. It replaced `<OLD_ARTIFACT_ID>`, which is deleted.
To ship new code: `mvn -q clean package`, `confluent flink artifact create <name> --artifact-file target/flink-rules-lt-1.0.0.jar --cloud aws --region eu-central-1 --environment <ENV_ID> --runtime-language java`,
`DROP FUNCTION` + `CREATE FUNCTION` on the new artifact, recreate statements. Verified after the 2026-11-23 deploy: the new function started (RUNNING, healthy), loaded rules
through the trimmed `RuleClient` (`rule_version` 1 on the filters) and restored the fired history (`fire_count` 362, 394, 417 carried over).

Connection parameters are read via
`context.getJobParameter("lightning_connection.endpoint|username|password", null)`.

## SQL (`sql/`) — run order

| File | What |
|---|---|
| `03_connection.sql` | `CREATE CONNECTION lightning_connection` (**placeholder creds in the file**) |
| `30_generic_rules.sql`, `31_generic_fired.sql` | `rg_rules`, `rg_fired` (then enable Lightning on both topics) |
| `32_generic_rules_data.sql` | 9 example rules |
| `33_generic_io.sql` | `rg_readings` (Faker) and `rg_processed` |
| `34_generic_run.sql` | `CREATE FUNCTION` (comment, replace artifact id) + the PTF `INSERT INTO rg_processed` |
| `35_generic_fired_writer.sql` | writer `INSERT INTO rg_fired … WHERE event_kind = 'FIRED'` |

Function: `CREATE FUNCTION apply_rules_generic AS 'demo.iot.ApplyRulesGenericPtf' USING JAR 'confluent-artifact://<ARTIFACT>' USING CONNECTIONS(\`<ENV_NAME>\`.\`<CLUSTER_NAME>\`.\`lightning_connection\`);`

Faker: `'connector'='faker'`, `fields.<col>.expression='#{...}'` (doubled single quotes inside SQL strings).
Enable Lightning on a topic: `confluent rtce rtce-topic create --cloud aws --region eu-central-1 --topic-name <t> --environment <ENV_ID> --cluster <CLUSTER_ID>` (phase PENDING → ACTIVE in ~1–3 min).

## Test results

1. **Raw HTTP in PTF**: every lookup `HttpConnectTimeoutException`.
2. **Connection from a UDF (placeholder creds)**: `HTTP 401` ⇒ network reachable via connection.
3. **Connection-bound PTF, real key (per-row lookup prototype)**: 88 rows correct (°C→°F, hPa→psi, alerts, disabled sensor filtered, unknown → `NO_RULE`).
4. **Generic PTF, first run** (`uid` `generic-v1`, 2026-11-23): 6 one-shot pairs (3 devices × temperature/pressure convert) each fired once;
   after delete/recreate (same uid) no re-firing, `SKIPPED_ONE_SHOT` emitted.
5. **Recreate test and performance, 2026-11-23** — see next section.
6. **Rules version update, 2026-11-23** — see the section after that.

## Recreate test and performance (2026-11-23, statement `rg-run-v3`, uid `generic-v1`)

Setup: new statement `rg-run-v3` (same SQL and uid as the earlier `rg-run-v2`, which was stopped) created 08:19:09Z, via the Confluent MCP.
The writer was NOT run during this test (it would replay the whole `rg_processed` topic into `rg_fired`). Evidence statements read `rg_processed`
for rows with `event_ts >= 1795421949000` (08:19:09Z).

- **Startup:** PENDING → RUNNING in **40 s** (08:19:09 → 08:19:49). Task graph: `Source rg_readings → Calc → AsyncProcessTableFunction → Calc → Sink`, 3 vertices, 6 operators.
- **State reset confirmed:** first rows after the restart had `device_seen` 8–38 (restarts from 1), while `fire_count` continued from `rg_fired` history
  (e.g. 378–398 for `r-hum-filter`) and `first_fired` kept the old value (e.g. 1795419850997).
- **No one-shot re-fired:** filter statement `rg-ev-v3` (`FIRED` rows with `rule_id LIKE '%convert%'` or `fire_count = 1`) read the whole backlog (4,348 rows)
  and then ~190–200 rows/min up to 08:26, with **0 output rows** in every minute.
- **Per-device skip (all 6 pairs):** dedupe statement `rg-ev-skip-run` (first `SKIPPED_ONE_SHOT` `RESULT` row per device × sensor after the restart) produced exactly 6 rows:
  dev-A/B/C × temperature (`SKIPPED_ONE_SHOT:r-temp-convert`, first seen at `device_seen` 62 / 5 / 6) and × pressure (`SKIPPED_ONE_SHOT:r-press-convert`, at `device_seen` 3 / 4 / 1).
  Earliest row 08:20:04Z, i.e. ~55 s after statement creation (40 s to RUNNING + ~15 s to the first record). Temperature pairs show up later because the temperature FILTER drops ~86% of readings before the convert rule runs.
- **Count regression observed:** `r-temp-filter|dev-A` was at `fire_count` 341 when the old run stopped but 338 on the first row after the restart.
  Consistent with the documented rebuild race (last firings had not reached `rg_fired`); it does not affect one-shot decisions (they test count > 0). *(reasoning: cause not isolated)*
- **Performance** (Metrics API, 1-min buckets, 08:20–08:26, steady state):

| Metric | Value |
|---|---|
| Records in | ~180/min (= Faker's 3 rows/s) |
| Records out (`FIRED` + `RESULT`) | ~191/min (~3.2/s) |
| Busy time | 3–12 ms/s (first minute 677 ms/s, startup scan) |
| Idle time | 999 ms/s |
| Backpressure | 0 ms/s |
| State size | 3–11 KB (sawtooth, 3 devices) |
| CFUs | 0.5 |

  The Faker caps input at 3 rows/s, so this shows headroom (>99% idle), not maximum throughput. Max throughput, per-row latency,
  and the cost of the startup scans at larger table sizes were **not measured**. `udf_external_egress_bytes` returned no data.

## Rules version update (2026-11-23)

Question: does an edited rule take effect, and when? Rule `r-temp-alert` was changed from version 1 (`{"above":30}`) to version 2 (`{"above":100}`) with
`INSERT INTO rg_rules VALUES (...)` (an upsert on `rule_id`) at 09:02:52Z while statement `rg-run-v4` (created 08:59:41Z, same uid) was running.
An evidence filter wrote every `r-temp-alert` `FIRED` row (device, raw value, `rule_version`, `device_seen`) to a small topic. The temperature FILTER runs first
and drops readings above 150, so alerts exist for raw values between the threshold and 150.

- **Before the edit:** `rule_version` 1, alerts at raw 35.59, 51.17, 70.72 and so on (threshold 30).
- **Running statement, after the edit: no reload.** `rg-run-v4` kept emitting `rule_version` 1 for more than 4 minutes after the edit (raw 34.39, 36.72, 38.74, 42.7, 62.75, 70.08)
  until it was stopped at 09:10:26Z.
- **Recreated statement: new version applies.** `rg-run-v5` (created 09:07:55Z, same SQL and uid) emitted `rule_version` 2 only for raw values above 100
  (100.06, 100.69, 103.34, 107.49, 109.4, … 148.62). A violation filter (`rule_version = 2 AND raw_value <= 100`) read 77 rows in its first minute and emitted 0; its output topic stayed empty.
- **Propagation:** `rg-run-v5` scanned about 1 minute after creation (roughly 6 minutes after the edit) and already saw version 2. The real edit-to-Lightning ingestion latency was not narrowed.
- **Not checked:** reading the `rg_rules` row back from Lightning (CLI 4.81 has no `confluent lightning` command), a rule edit on a one-shot rule, and the `fire_count` history across a version change.
- The table was restored afterwards to the documented row (`r-temp-alert`, version 1, `{"above":30}`), so `rg_rules` matches `sql/32`.

## Limits and delivery guarantees

- **No reload**: rules and history are read once per subtask; rule edits need a statement recreate. Tested 2026-11-23, see the rules version section.
- **Rebuild race**: a firing not yet through output topic → writer → `rg_fired` topic → Lightning ingestion is invisible to a recreated statement, so a one-shot rule can fire twice.
- **30-day window**: history rows with `last_fired` older than 30 days are ignored and state TTL is 30 days, so a (rule, device) idle for longer can fire again.
- **Startup failure is fatal by design** (fail fast instead of running without rules/history, which would re-fire one-shot rules).
- **Memory/size**: each scan is one HTTP response (60 s timeout, no paging); the whole fired table within the window is held in memory per subtask. Lightning response limits and retention not tested.
- State: exactly-once per statement *(Flink design)*; output topic exactly-once only for committed-read consumers *(docs only, not tested)*;
  writer upserts are idempotent, but `rg_processed` is keyless so two `FIRED` rows of one key may reach the writer out of order and a lower `fire_count` could win *(reasoning)*.
- Faker cannot replay; end-to-end guarantees only apply to a real Kafka source.

## Gotchas learned (read before extending)

- **Lightning SQL folds unquoted column names to uppercase**; do **not** backtick lowercase column names in `WHERE`
  (`WHERE \`sensor_type\` = …` → `Column 'sensor_type' not found`). Table names may be backticked. `RuleClient` lowercases result column names.
- Lightning API values come back as strings; JSON result shape: `result.schema.columns[].name`, `result.data[][]`.
- `ALTER CONNECTION … SET ('username'=…,'password'=…)` failed ("multiple authentication credentials") → **drop and recreate** the connection.
- **UDFs are immutable**: to ship new code upload a new artifact, `DROP FUNCTION`, `CREATE FUNCTION … USING JAR … USING CONNECTIONS(…)`, recreate dependent statements.
- Lightning needs a **Global API key** (`--resource global`). Per-user limit is **2**; creation fails when full.
  The key sits inside the connection definition; statements containing it should be deleted afterwards.
- `confluent lightning query` needs CLI ≥ 4.82 (we had 4.81). `confluent flink shell` needs a TTY. The CLI cannot fetch SELECT results.
- **Confluent Cloud MCP** (`confluent-mcp-regional`, `confluent-mcp-global`, both authenticated via `/mcp`):
  `list_flink_statements` (paged), `check_flink_statement_health`, `get_flink_statement_exceptions`, `get_flink_statement_profile` (task graph),
  `create_flink_statement` / `validate_flink_sql` (pass `properties` `sql.current-catalog=<ENV_NAME>`, `sql.current-database=<CLUSTER_NAME>`),
  `consume_kafka_messages` (**no Kafka key needed**, but max 10 messages, returns the **newest** rows of the range even when you pass an offset, cannot filter),
  and `get_flink_statement_metrics` (global server; needs `compute_pool_id`; **empty for the first 1–2 min**; use `list_metrics` with `resource_type=flink_statement` for names).
  There is no stop/delete-by-name for a *running* statement besides `delete_flink_statement`; stop/resume still needs the CLI.
- For exhaustive checks write a Flink filter statement's output to a small evidence topic and read that. Dedupe pattern that works:
  `ROW_NUMBER() OVER (PARTITION BY … ORDER BY $rowtime ASC) … WHERE rn = 1` into an explicitly created append table.
  `CREATE TABLE … AS` with `PARTITION BY` columns fails ("Invalid primary key … nullable").
- **CLI sessions expire** ("not logged in"); polling loops then spin silently. In **zsh**, store flags in an array (`E=(--environment … )`, `"${E[@]}"`) —
  an unquoted `$E` string is not word-split and the command fails.
- `confluent flink statement create` needs `--compute-pool`, `--database <cluster name>`, `--environment`, and for stop/resume/delete `--cloud aws --region eu-central-1`.
- Statement names must be unique per environment, so a recreate needs a new name (e.g. `rg-run-v3`) unless the old one is deleted first.
  Names allow only lowercase letters, digits and hyphens (no underscores).
- The CLI session expired twice during one day (`stop` then prints a login notice and does nothing). Check the statement phase after every `stop`.
  The MCP can delete a statement (which also stops it) but its tool asks for confirmation first.
- Timers fire only on watermark advance; PTF state POJO fields must be public with defaults; `ListView`/`MapView` unsupported; max 10 UDFs per statement *(docs)*.

## Current state of the org (cleanup list)

Cleaned up 2026-11-23: all 84 earlier statements were deleted. Nothing is running. About a dozen stopped or completed statements from the later tests remain
(`rg-run-v4`/`v5`/`v6`, `rv-*`, `deploy-*`); an attempt to bulk-delete them was denied by the permission check and not retried. They are harmless.
One more statement, `i-s-…` (an interactive one), is not from this experiment; leave it alone.
Kept for further testing, in `<ENV_ID>` / `<CLUSTER_ID>`:

- Tables/topics: `rg_rules`, `rg_fired` (both Lightning-enabled), `rg_readings` (Faker), `rg_processed`.
- Function `apply_rules_generic`, artifact `iot-rules-generic-v2` = `<ARTIFACT_ID>`.
- Connection `lightning_connection` (holds Global API key **`<API_KEY_ID>`**, owned by the user, description "flink-rules-lt lightning (delete after)").
  The user's old Global key `<OLD_API_KEY_ID>` was deleted (at the user's explicit choice) to make room.

Removed 2026-11-23: all `iot_*` tables/topics and the `iot_enriched` view, functions `lookup_rule`, `apply_rules`, `apply_rules_remote`,
`apply_rules_stateful`, `apply_rules_durable`, artifacts `iot-rules-ptf` and `iot-rules-v2`…`v6`, evidence tables `rg_ev`, `rg_ev_conv`, `rg_ev_after`,
`rg_ev_v3`, `rg_ev_skip`, and Lightning on `iot_rules`, `iot_rules_v2`, `iot_applied`. Everything else in the environment (`customers`, `transactions`,
`udf_example-*` artifacts, …) belongs to other work and was not touched.

**Before the next one-shot test:** `rg_fired` still holds today's firing history. A new statement loads it at startup, so every one-shot rule is skipped
from the first reading. For a clean test write tombstones for its keys first, or point the PTF at a fresh fired table. *(reasoning from the code, not tried)*

Teardown (when finished):
```
-- Flink SQL
DROP FUNCTION apply_rules_generic;
DROP CONNECTION lightning_connection;
DROP TABLE rg_rules; DROP TABLE rg_fired; DROP TABLE rg_readings; DROP TABLE rg_processed;
```
```
confluent rtce rtce-topic delete rg_rules rg_fired --environment <ENV_ID> --cluster <CLUSTER_ID>   # do this before DROP TABLE
confluent flink artifact delete <ARTIFACT_ID> --cloud aws --region eu-central-1 --environment <ENV_ID>
confluent api-key delete <API_KEY_ID>
```

## Sources (Confluent docs)

- Lightning Tables: https://docs.confluent.io/cloud/current/lightning-tables/overview.html (+ get-started, limitations, query/api, query/jdbc, query/cli)
- UDFs / external connectivity: https://docs.confluent.io/cloud/current/flink/concepts/user-defined-functions.html
- PTFs: https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html , https://docs.confluent.io/cloud/current/flink/how-to-guides/create-ptf.html
- Faker: https://docs.confluent.io/cloud/current/flink/how-to-guides/custom-sample-data.html
- External-connection UDF pattern: https://github.com/griga23/flink-udf-examples/blob/main/docs/scalar/SchemaRegistryAvroDeserializer.md
