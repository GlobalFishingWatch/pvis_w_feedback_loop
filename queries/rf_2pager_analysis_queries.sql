### this includes the queries used to create data tables for looker dashboard

WITH

-- set source table 
--df AS (SELECT *  FROM `world-fishing-827.scratch_willa_ttl100.pvis_w_rf_20260123` WHERE year IN NOT NULL),
ddf AS (SELECT *  FROM `world-fishing-827.scratch_willa_ttl100.pvis_w_rf_20260209`),

-- all the values we are interested infor reference 
-- ssvid,
-- year,
-- rf_vessel_class,
-- prod_shiptype,
-- prod_geartype,
-- self_reported_shiptype,
-- core_geartype,
-- prod_shiptype_rf,
-- prod_geartype_rf
-- FROM df
-- WHERE
-- prod_shiptype_rf = "insufficient_data"
-- prod_shiptype = "discrepancy"
-- prod_geartype = "inconclusive"
-- in_support_list
-- core_is_carrier
-- core_is_bunker

## INSUFFICIENT

-- insufficient data vessels (all time & by year)
-- rf_insufficient_feb6
-- rf_insufficient_byyear_feb6

rf_insufficient AS (SELECT DISTINCT
ssvid,
prod_shiptype_rf,
prod_shiptype,
prod_geartype,
core_geartype,
self_reported_shiptype,
FROM df
WHERE
prod_shiptype_rf = "insufficient_data"
),

rf_insufficient_byyear AS (SELECT DISTINCT
ssvid,
year,
prod_shiptype_rf,
prod_shiptype,
prod_geartype,
core_geartype,
self_reported_shiptype,
FROM df
WHERE
prod_shiptype_rf = "insufficient_data"
),

## DISCREPANCY

-- discrepancy vessels (all time & by year)
-- rf_discrepancy_feb6
-- rf_discrepancy_byyear_feb6

rf_discrepancy AS (SELECT DISTINCT
ssvid,
prod_shiptype_rf,
prod_geartype_rf,
rf_vessel_class,
core_geartype,
self_reported_shiptype
FROM df
WHERE
prod_shiptype = "discrepancy"
),

rf_discrepancy_byyear AS (SELECT DISTINCT
ssvid,
year,
prod_shiptype_rf,
prod_geartype_rf,
rf_vessel_class,
core_geartype,
self_reported_shiptype
FROM df
WHERE
prod_shiptype = "discrepancy"
),

## INCONCLUSIVE

-- inconclusive gear type (all time & by year)
-- rf_inconclusive_feb6
-- rf_inconclusive_byyear_feb6

rf_inconclusive AS (SELECT DISTINCT
ssvid,
prod_shiptype_rf,
prod_geartype_rf,
rf_vessel_class,
core_geartype,
self_reported_shiptype
FROM df
WHERE
prod_geartype = "inconclusive"
),

rf_inconclusive_byyear AS (SELECT DISTINCT
ssvid,
year,
prod_shiptype_rf,
prod_geartype_rf,
rf_vessel_class,
core_geartype,
self_reported_shiptype
FROM df
WHERE
prod_geartype = "inconclusive"
),

## SUPPORT

-- support vessels (TMT purse seine support vessels) (all time & by year)
-- rf_support_feb6
-- rf_support_byyear_feb6

rf_support AS (SELECT DISTINCT
ssvid,
prod_shiptype_rf,
prod_geartype_rf,
rf_vessel_class,
core_geartype,
self_reported_shiptype
FROM df
WHERE
in_support_list
),

rf_support_byyear AS (SELECT DISTINCT
ssvid,
year,
prod_shiptype_rf,
prod_geartype_rf,
rf_vessel_class,
core_geartype,
self_reported_shiptype
FROM df
WHERE
in_support_list
),

## CARRIER

-- carrier vessel list (all time & by year)
-- rf_carrier_feb6
-- rf_carrier_byyear_feb6

rf_carrier AS (SELECT DISTINCT
ssvid,
prod_shiptype_rf,
prod_geartype_rf,
rf_vessel_class,
core_geartype,
self_reported_shiptype
FROM df
WHERE
core_is_carrier
),

rf_carrier_byyear AS (SELECT DISTINCT
ssvid,
year,
prod_shiptype_rf,
prod_geartype_rf,
rf_vessel_class,
core_geartype,
self_reported_shiptype
FROM df
WHERE
core_is_carrier
),

## BUNKER

-- bunker list (all time & by year)
-- rf_bunker_feb6
-- rf_bunker_byyear_feb6

rf_bunker AS (SELECT DISTINCT
ssvid,
prod_shiptype_rf,
prod_geartype_rf,
rf_vessel_class,
core_geartype,
self_reported_shiptype
FROM df
WHERE
core_is_bunker
),

rf_bunker_byyear AS (SELECT DISTINCT
ssvid,
year,
prod_shiptype_rf,
prod_geartype_rf,
rf_vessel_class,
core_geartype,
self_reported_shiptype
FROM df
WHERE
core_is_bunker
),

## CARGO OR TANKER

-- cargo or tanker compound vessel class
-- cargo_or_tanker  
-- -- bunker_or_tanker  
-- -- -- bunker | tanker
-- -- cargo_or_reefer
-- -- -- cargo
-- -- -- reefer
-- -- -- -- specialized_reefer
-- -- -- -- container_reefer
-- -- -- fish_tender
-- -- -- -- well_boat

rf_cargo_or_tanker AS (SELECT DISTINCT
ssvid,
year,
prod_shiptype_rf,
prod_geartype_rf,
rf_vessel_class,
core_geartype,
self_reported_shiptype
FROM df
WHERE
rf_vessel_class IN ('cargo_or_tanker', 'bunker_or_tanker', 'bunker', 'tanker', 
'cargo_or_reefer', 'cargo', 'reefer', 'specialized_reefer', 'container_reefer', 'fish_tender', 'well_boat')
OR in_support_list
OR core_is_carrier
OR core_is_bunker)

### results

SELECT * FROM rf_carrier