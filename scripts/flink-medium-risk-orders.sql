-- Flink SQL Statement 3: ol-medium-risk-orders
-- Reads from ol-orders-enriched, filters for MEDIUM risk_tier orders.
-- Source:  ol-orders-enriched
-- Sink:    ol-medium-risk-orders
--
-- Deploy:
--   confluent flink statement create ol-medium-risk-orders \
--     --sql "$(cat scripts/flink-medium-risk-orders.sql)" \
--     --compute-pool <pool-id> \
--     --environment <env-id> \
--     --database <cluster-id>

INSERT INTO `ol-medium-risk-orders`
SELECT
  CAST(CAST(orderid AS STRING) AS BYTES) AS key,
  ordertime,
  orderid,
  itemid,
  orderunits,
  city,
  state,
  risk_tier
FROM `ol-orders-enriched`
WHERE risk_tier = 'MEDIUM';
