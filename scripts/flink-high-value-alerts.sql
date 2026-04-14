-- Flink SQL Statement 2: ol-high-value-alerts
-- Reads from ol-orders-enriched, filters for HIGH risk_tier orders only.
-- Source:  ol-orders-enriched
-- Sink:    ol-high-value-alerts

INSERT INTO `ol-high-value-alerts`
SELECT
  ordertime,
  orderid,
  itemid,
  orderunits,
  city,
  state,
  risk_tier
FROM `ol-orders-enriched`
WHERE risk_tier = 'HIGH';
