-- Flink SQL Statement 1: ol-enrich-orders
-- Reads raw orders, adds risk_tier classification, flattens address.
-- Source:  ol-raw-orders     (schema: ordertime, orderid, itemid, orderunits, address{city,state,zipcode})
-- Sink:    ol-orders-enriched

INSERT INTO `ol-orders-enriched`
SELECT
  ordertime,
  orderid,
  itemid,
  orderunits,
  address.city    AS city,
  address.state   AS state,
  address.zipcode AS zipcode,
  CASE
    WHEN orderunits > 10 THEN 'HIGH'
    WHEN orderunits > 5  THEN 'MEDIUM'
    ELSE                      'LOW'
  END AS risk_tier
FROM `ol-raw-orders`;
