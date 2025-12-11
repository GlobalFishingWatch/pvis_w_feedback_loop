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
    FROM `vessel_identity.identity_core_v20220701`
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
    ## add normalized shipname
    ## not sure if this is a good field addition 
    udfs.normalize_shipname(CAST(vid_segs.n_shipname AS string)) AS core_n_norm_shipname,
    vid_segs.n_callsign as core_n_callsign,
    vid_segs.imo as core_imo,
    vid_segs.first_timestamp as core_first_timestamp,
    vid_segs.last_timestamp as core_last_timestamp,
    ## add vessel info
    vi.n_shipname.value as vi_shipname,
	## add callsign and imo
	udfs.normalize_shipname(CAST(vi.n_shipname.value AS string)) AS vi_norm_shipname,
	vi.callsign.value as vi_callsign, 
	udfs.normalize_callsign(CAST(vi.callsign.value AS string)) AS vi_norm_callsign,
	vi.imo.value as vi_imo,
	-- commented out as don't really need, and makes query too complex
	--udfs.normalize_imo(CAST(vi.imo.value AS string)) AS vi_norm_imo,
	vi.first_timestamp as vi_first_timestamp,
	vi.last_timestamp as vi_last_timestamp,
	vi.msg_count as vi_msg_count,
	vi.pos_count as vi_pos_count,
  FROM vid_segs
    JOIN source_seg_info 
    ON (vid_segs.seg_id = source_seg_info.seg_id)
	JOIN source_pipe_vessel_info as vi 
	ON (source_seg_info.vessel_id = vi.vessel_id) 
GROUP BY 1,2,3,4,5,6,7,8,9 ,10,11,12,13,14,15,16,17,18--,19
	)--,

#### commented out because it makes to query too complex 

## calculate different types of matches
## note false matches currently when real value and null value are flagged as match
## need to fix matching logic or create boolean to flag when this false matching is happening 
-- results_matches AS (
--   SELECT *,
--   core_n_shipname = vi_shipname AS shipname_match,
--   core_n_norm_shipname = vi_norm_shipname AS norm_shipname_match,
--   ## add fuzzy matching / are names close udf
--   ## udf currently does not work
--   udfs.are_names_close(CAST(core_n_shipname AS string), CAST(vi_shipname AS string), 0.2) AS are_names_close_02,
--   udfs.are_names_close(CAST(core_n_shipname AS string), CAST(vi_shipname AS string), 0.3) AS are_names_close_03,
--   core_n_callsign = vi_callsign AS callsign_match,
--   core_n_callsign = vi_norm_callsign AS norm_callsign_match,
--   core_imo = vi_imo AS imo_match,
--   --cast(core_imo as string) = cast(vi_norm_imo as string) AS norm_imo_match,
--   vi_shipname IS NULL as vi_shipname_null,
--   (TIMESTAMP_DIFF(vi_last_timestamp, vi_first_timestamp, second)/3600) as vi_identity_duration_hr,
--   FROM
--   --vid_merge_vessel_info_identity
--   vid_segs_link_vessel_id
-- )--,

## records with no identity transmitted on AIS (according to vessel info table)

-- no_ais_identity AS (SELECT * FROM results_matches
-- WHERE 
-- vi_norm_shipname IS NULL 
-- AND vi_norm_callsign IS NULL
-- AND vi_norm_imo IS NULL)

## match results
SELECT * FROM 
vid_segs_link_vessel_id

