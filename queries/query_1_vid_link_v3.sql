#standardSQL
################################################################################################
WITH

### SOURCE TABLES
### vessels used in product 

  source_all_vessels AS (
    SELECT
      *
    FROM 
    --`world-fishing-827.pipe_production_v20201001.all_vessels_byyear_v20221001`
    `world-fishing-827.pipe_production_v20201001.all_vessels_byyear`
    -- only keep vessels currently shown in products
    WHERE (shiptype = 'carrier' OR shiptype = 'support' OR shiptype = 'fishing')
    -- filter out known spoofy MMSI 
    -- these MMSI are filtered out of fishing vessel list but not carriers
    AND ssvid NOT in ('0', '525000000', '111111111', '888888888', '1193046')
  ),

### vessel identity core

  source_vessel_identity_core AS (
    SELECT
      *
    FROM `vessel_identity.identity_core_v20221101`
  ),

### vessel info

  source_pipe_vessel_info AS (
    SELECT 
      *
    FROM `pipe_production_v20201001.vessel_info`
  ),

### research segs

  source_research_segs AS (
    SELECT 
      *
    FROM `world-fishing-827.pipe_production_v20201001.research_segs`
  ),
  
### research segment info
### this is the table that should be used to get vessel_ids

  source_seg_info AS (
    SELECT 
      *
    FROM `pipe_production_v20201001.segment_info`
  ),

  ############

### matching on core vessel identity ssvid's to seg_id
### if seg_id connected to matching ssvid
### during overlapping time range for ssvid in core vessel identity table
vid_segs as(
SELECT
    core_vessel_identity.*,
    segs.first_timestamp as segs_ft,
    segs.last_timestamp as segs_lt,
    segs.seg_id
  FROM source_vessel_identity_core AS core_vessel_identity
    JOIN source_research_segs as  segs ON (
        core_vessel_identity.ssvid = segs.ssvid
		# Original time check from Hannah and one we want to move forward with
        AND (segs.first_timestamp BETWEEN core_vessel_identity.first_timestamp AND core_vessel_identity.last_timestamp
        OR segs.last_timestamp BETWEEN core_vessel_identity.first_timestamp AND core_vessel_identity.last_timestamp)
   )
   ## WILLA ADDED
   ## add in good segs filter -- this is used in products
     WHERE seg_id IN (
    SELECT seg_id
    FROM source_research_segs
    WHERE good_seg
      AND NOT overlapping_and_short
   )
    ),

### joining with vessel_id using vessel_id's and time ranges in the pipe segment info table
### eg multiple shorter time segments linked to vessel id
### as opposed to in pipe vessel info where there is a single record with larger aggregated time range
## also link additional AIS identity transmission infomation using pipe vessel info 
## linking with vessel_id
vid_segs_link_vessel_id as (
  SELECT
  vid_segs.vessel_record_id,
  source_seg_info.vessel_id,
  vid_segs.ssvid,
  vid_segs.n_shipname as core_n_shipname,
  vid_segs.n_callsign as core_n_callsign,
  vid_segs.imo as core_imo,
  vid_segs.first_timestamp as core_first_timestamp,
  vid_segs.last_timestamp as core_last_timestamp,
  vid_segs.source_code AS core_source_code,
  ## add vessel info
  vi.n_shipname.value as vi_shipname,
	vi.callsign.value as vi_callsign, 
	udfs.normalize_callsign(CAST(vi.callsign.value AS string)) AS vi_norm_callsign,
	vi.imo.value as vi_imo,
	vi.first_timestamp as vi_first_timestamp,
	vi.last_timestamp as vi_last_timestamp,
	vi.msg_count as vi_msg_count,
	vi.pos_count as vi_pos_count,
  ## add in are names close UDF
  udfs.are_names_close(CAST(vid_segs.n_shipname AS string), CAST(vi.n_shipname.value AS string), 0.2) AS are_names_close_02,
  udfs.are_names_close(CAST(vid_segs.n_shipname AS string), CAST(vi.n_shipname.value AS string), 0.3) AS are_names_close_03,
  ## add attributes about vessel type
  ## note in vessel database publication (the v20220701 vessel identity table) 
  ## there were only carriers, bunkers, fishing vessels
  ## in the updated vessel database (vessel_identity.identity_core_v20221101) 
  ## there are all vessel types, including non fishing vessels
  vid_segs.is_fishing,
  vid_segs.is_carrier,
  vid_segs.is_bunker,
  ## adding this boolean to identify if vessel is included in products
  source_seg_info.vessel_id IN (SELECT vessel_id FROM source_all_vessels) AS carrier_fishing_support,
  FROM vid_segs
    JOIN source_seg_info 
    ON (vid_segs.seg_id = source_seg_info.seg_id)
	JOIN source_pipe_vessel_info as vi 
	ON (source_seg_info.vessel_id = vi.vessel_id) 
GROUP BY 1,2,3,4,5,6,7,8,9 ,10,11,12,13,14,15,16,17,18,19,20,21,22
	),

## calculate different types of matches
## note are names close incorrect matches currently when real value and null value are flagged as match
## need to fix matching logic or create boolean to flag when this false matching is happening 
results_matches AS (
  SELECT * EXCEPT(are_names_close_02, are_names_close_03),
  ## extra information for analysis if interested
    (TIMESTAMP_DIFF(vi_last_timestamp, vi_first_timestamp, second)/3600) as vi_identity_duration_hr,
  ## other identity comparisons
  core_n_shipname = vi_shipname AS shipname_match,
  ## bandaid to fix how are_names_close UDF handles NULL values
  IF (vi_shipname IS NULL OR core_n_shipname IS NULL, NULL, are_names_close_02) AS are_names_close_02_fixed,
  IF (vi_shipname IS NULL OR core_n_shipname IS NULL, NULL, are_names_close_03) AS are_names_close_03_fixed,
  core_n_callsign = vi_norm_callsign AS norm_callsign_match,
  core_imo = vi_imo AS imo_match,
  FROM
  vid_segs_link_vessel_id
),

## assign confidence
## imo, shipname, fuzzy shipname match, or callsign match are high
## no shipname, callign, or imo is no confidence 
## else is low
results_confidence AS (SELECT
*,
CASE WHEN (imo_match OR are_names_close_03_fixed OR norm_callsign_match) THEN "high"
WHEN (vi_imo IS NULL AND vi_shipname IS NULL AND vi_norm_callsign IS NULL) THEN "no confidence"
ELSE "low"
END as confidence 
FROM
results_matches),

## if vessel_id is linked to low and high confidence match, then only keep high confidence match record 
## this happens when there are multiple registry records for a vessel
## and there is only one set of AIS identity information per vessel_id
## so the vessel_id can high confidence match to one of the registry records
## and low confidence match to the other registry record.
remove AS (
SELECT * 
FROM results_confidence
WHERE 
confidence = 'low'
AND vessel_id IN (SELECT vessel_id FROM results_confidence WHERE confidence = 'high')
),

dedup_results AS (SELECT * 
FROM results_confidence 
WHERE 
NOT (confidence = 'low' AND vessel_id IN (SELECT vessel_id FROM remove)))

## match results
SELECT 
*
FROM 
dedup_results
## add this filter to limit results to only pull vessel_ids used in products currently
-- WHERE
-- carrier_fishing_support