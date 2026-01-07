/*

product_vessel_info_match -> base_vessel_identity_info_match (pipe2.5)

Variables to replace
====================
date_YYYYMMDD => It is the date of the version that we want to generate

source_identity_core
    pipe2_5 -> world-fishing-827.vessel_identity.identity_core
    pipe3 -> world-fishing-827.pipe_ais_v3_published.identity_core

source_vessel_info
    pipe2_5 -> world-fishing-827.pipe_production_v20201001.vessel_info
    pipe3 -> world-fishing-827.pipe_ais_v3_published.vessel_info

source_research_segs
    pipe2_5 -> world-fishing-827.pipe_production_v20201001.research_segs
    pipe3 -> world-fishing-827.pipe_ais_v3_published.segs_activity

source_segment_info
    pipe2_5 -> world-fishing-827.pipe_production_v20201001.segment_info
    pipe3 ->world-fishing-827.pipe_ais_v3_published.segment_info
*/

WITH

    ### SOURCE TABLES

    ### vessel identity core
    source_vessel_identity_core AS (
        SELECT
            vessel_record_id,
            ssvid,
            shipname,
            n_shipname,
            n_callsign,
            imo,
            flag,
            geartype,
            is_fishing,
            is_carrier,
            is_bunker,
            length_m,
            tonnage_gt,
            source_code,
            first_timestamp,
            last_timestamp,
        FROM `{{source_identity_core}}`
    ),

    ### vessel info
    source_pipe_vessel_info AS (
        SELECT
            vessel_id,
            ssvid,
            shipname,
            n_shipname,
            callsign,
            imo,
            first_timestamp,
            last_timestamp,
            msg_count,
            pos_count
        FROM `{{source_vessel_info}}`
    ),

    ### research segs
    source_research_segs AS (
        SELECT
            seg_id,
            first_timestamp,
            last_timestamp,
            good_seg,
            overlapping_and_short,
        FROM `{{source_research_segs}}`
    ),

    ### research segment info
    ### this is the table that should be used to get vessel_ids
    source_seg_info AS (
        SELECT
            seg_id,
            vessel_id
        FROM `{{source_segment_info}}`
    ),

    ##########################
    # Step 1 - Match vessels #
    ##########################

    ### Map seg_id to vessel_id
    ### Only keep vessel_id connected to non-noisy segs
    clean_vid_segs AS(
        SELECT
            vi.vessel_id,
            vi.ssvid,
            vi.shipname.value as vi_shipname,
            vi.n_shipname.value as vi_n_shipname,
            vi.callsign.value as vi_callsign,
            udfs.normalize_callsign(CAST(vi.callsign.value AS string)) AS vi_norm_callsign,
            vi.imo.value as vi_imo,
            `world-fishing-827.udfs.mmsi_to_iso3`(vi.ssvid) AS vi_flag,
            vi.first_timestamp as vi_first_timestamp,
            vi.last_timestamp as vi_last_timestamp,
            vi.msg_count as vi_msg_count,
            vi.pos_count as vi_pos_count,
            rs.first_timestamp,
            rs.last_timestamp,
            rs.seg_id,
        FROM source_research_segs AS rs
                 JOIN source_seg_info AS si ON rs.seg_id = si.seg_id
                 JOIN source_pipe_vessel_info AS vi ON si.vessel_id = vi.vessel_id
        -- add in good segs and not overlapping and short filter -- this is used in products
        WHERE good_seg
          AND NOT overlapping_and_short
    ),

    ### vessel_record_id (eg unique hull id) and vessel_id matching step
    ### matching identity core and AIS records using SSVID and seg_id time range
    ### if SSVID matches but identity core and AIS record time range DOES NOT overlap
    ### then there will NOT be a match
    ### NOTE: using seg_id time ranges as opposed to vessel_id time ranges to match with identity core
    ### since seg_id is a more exact time range, where as the single vessel_id time range
    ### is the aggregate time range of all seg_ids connected to vessel_id
    match_vid_segs AS (
        SELECT
            core_vessel_identity.*,
            segs.vessel_id,
            segs.first_timestamp AS segs_ft,
            segs.last_timestamp AS segs_lt,
            segs.seg_id
        FROM source_vessel_identity_core AS core_vessel_identity
                 JOIN clean_vid_segs AS segs ON (
            core_vessel_identity.ssvid = segs.ssvid
                AND (
                (segs.first_timestamp BETWEEN core_vessel_identity.first_timestamp AND core_vessel_identity.last_timestamp
                    OR segs.last_timestamp BETWEEN core_vessel_identity.first_timestamp AND core_vessel_identity.last_timestamp)
                    OR
                (segs.first_timestamp <= core_vessel_identity.first_timestamp
                    AND segs.last_timestamp >= core_vessel_identity.last_timestamp)
                )
            )
    ),

    ### pull fields of interest from identity core table
    ### pull fields of interest from AIS transmission (from pipe production vessel info)
    ### add UDF are names close to see if identity core (registry) and AIS shipname fuzzy match
    vid_segs_link_vessel_id as (
        SELECT
            'CORE_AND_REGISTRY' AS source,
            match_vid_segs.vessel_record_id AS vessel_record_id,
            clean_vid_segs.vessel_id AS vessel_id,
            match_vid_segs.ssvid AS ssvid,
            match_vid_segs.shipname AS core_shipname,
            match_vid_segs.n_shipname AS core_n_shipname,
            match_vid_segs.n_callsign AS core_n_callsign,
            match_vid_segs.imo AS core_imo,
            -- add flag
            match_vid_segs.flag AS core_flag,
            -- add information on gear type
            match_vid_segs.geartype AS core_geartype,
            -- add attributes about vessel type
            match_vid_segs.is_fishing AS core_is_fishing,
            match_vid_segs.is_carrier AS core_is_carrier,
            match_vid_segs.is_bunker AS core_is_bunker,
            match_vid_segs.length_m AS core_length_m,
            match_vid_segs.tonnage_gt AS core_tonnage_gt,
            match_vid_segs.first_timestamp as core_first_timestamp,
            match_vid_segs.last_timestamp as core_last_timestamp,
            match_vid_segs.source_code AS core_source_code,
            -- add vessel info
            clean_vid_segs.vi_shipname AS vi_shipname,
            clean_vid_segs.vi_n_shipname AS vi_n_shipname,
            clean_vid_segs.vi_callsign AS vi_callsign,
            clean_vid_segs.vi_norm_callsign AS vi_norm_callsign,
            clean_vid_segs.vi_imo AS vi_imo,
            -- ADD FLAG
            clean_vid_segs.vi_flag AS vi_flag,
            clean_vid_segs.vi_first_timestamp AS vi_first_timestamp,
            clean_vid_segs.vi_last_timestamp AS vi_last_timestamp,
            clean_vid_segs.vi_msg_count AS vi_msg_count,
            clean_vid_segs.vi_pos_count AS vi_pos_count,
            -- add in are names close UDF
            udfs.are_names_close(CAST(match_vid_segs.n_shipname AS string), CAST(vi_shipname AS string), 0.3) AS are_names_close_03,
        FROM match_vid_segs
                 JOIN clean_vid_segs ON (match_vid_segs.seg_id = clean_vid_segs.seg_id)
        GROUP BY
            vessel_record_id, vessel_id, ssvid, core_shipname, core_n_shipname, core_n_callsign,
            core_imo, core_flag, core_geartype, core_is_fishing, core_is_carrier, core_is_bunker,
            core_length_m, core_tonnage_gt, core_first_timestamp, core_last_timestamp,
            core_source_code, vi_shipname, vi_n_shipname, vi_callsign, vi_norm_callsign,
            vi_imo, vi_flag, vi_first_timestamp, vi_last_timestamp,vi_msg_count, vi_pos_count
    ),

    ### calculate different types of matches between identity core (registry) and AIS
    ### look at shipname match, shipname fuzzy match, callsign match, imo match
    results_matches AS (
        SELECT
            * EXCEPT(are_names_close_03),
            core_n_shipname = vi_shipname AS shipname_match,
            IF (vi_shipname IS NULL OR core_n_shipname IS NULL, NULL, are_names_close_03) AS are_names_close_03_fixed,
            core_n_callsign = vi_norm_callsign AS norm_callsign_match,
            core_imo = vi_imo AS imo_match,
        FROM vid_segs_link_vessel_id
    ),

    ## assign match_fields
    ## SSVID and 1+ identity matches (high) = several_fields
    ## that will include 2 existing cases: SSVID match only (low) or SSVID match only and No AIS identifiers (no_confidence) = id_match_only
    results_match_fields AS (
        SELECT
            *,
            CASE
                WHEN (imo_match OR are_names_close_03_fixed OR norm_callsign_match) THEN "SEVERAL_FIELDS"
                ELSE "ID_MATCH_ONLY"
                END as match_fields
        FROM results_matches
    ),

    ### assign confidence of match
    ### imo, shipname, fuzzy shipname match, or callsign match are high
    ### no shipname, callign, or imo is no confidence
    ### else is low
    results_confidence AS (
        SELECT
            *,
            CASE
                WHEN (imo_match OR are_names_close_03_fixed OR norm_callsign_match) THEN "HIGH"
                WHEN (vi_imo IS NULL AND vi_shipname IS NULL AND vi_norm_callsign IS NULL) THEN "NO_CONFIDENCE"
                ELSE "LOW"
                END as confidence
        FROM results_match_fields
    ),

    ### if vessel_id is linked to low and high confidence match, then only keep high confidence match record
    ### this happens when there are multiple registry records for a vessel
    ### and there is only one set of AIS identity information per vessel_id
    ### so the vessel_id can high confidence match to one of the registry records
    ### and low confidence match to the other registry record.
    remove AS (
        SELECT
            *
        FROM results_confidence
        WHERE confidence = 'LOW'
          AND vessel_id IN (SELECT vessel_id FROM results_confidence WHERE confidence = 'HIGH')
    ),

    dedup_results AS (
        SELECT *
        FROM results_confidence
        WHERE NOT (confidence = 'LOW' AND vessel_id IN (SELECT vessel_id FROM remove))
    ),

    ### match results
    core_and_registry AS (
        SELECT
            *
        FROM dedup_results
    ),

    ####################################################################################################
    # Step 2 - ONLY CORE (AIS) data - AIS records that do not match to identity core table (registry)  #
    ####################################################################################################
    only_core AS (
        SELECT
            'ONLY_CORE' as source,
            cast(null as string) AS vessel_record_id,
            vessel_id,
            ssvid,
            cast(null as string) AS core_shipname,
            cast(null as string) AS core_n_shipname,
            cast(null as string) AS core_n_callsign,
            cast(null as string) AS core_imo,
            cast(null as string) AS core_flag,
            cast(null as string) AS core_geartype,
            cast(null as bool) AS core_is_fishing,
            cast(null as bool) AS core_is_carrier,
            cast(null as bool) AS core_is_bunker,
            cast(null as float64) AS core_length_m,
            cast(null as float64) AS core_tonnage_gt,
            cast(null as timestamp) AS core_first_timestamp,
            cast(null as timestamp) AS core_last_timestamp,
            cast(null as string) AS core_source_code,
            vi_shipname,
            vi_n_shipname,
            vi_callsign,
            vi_norm_callsign,
            vi_imo,
            vi_flag,
            vi_first_timestamp,
            vi_last_timestamp,
            vi_msg_count,
            vi_pos_count,
            false AS shipname_match,
            false AS are_names_close_03_fixed,
            false AS norm_callsign_match,
            false AS imo_match,
            'NO_MATCH' AS match_fields,
            'NO_CONFIDENCE' AS confidence
        FROM clean_vid_segs
        WHERE vessel_id NOT IN (
            SELECT DISTINCT vessel_id
            FROM core_and_registry
        )
    ),

    #############################################################################################################################
    # Step 3 - ONLY REGISTRY DATA (IDENTITY CORE TABLE) - records from the identity core table that do not match to AIS record  #
    #############################################################################################################################
    only_registry AS (
        SELECT
            'ONLY_REGISTRY' as source,
            vessel_record_id,
            cast(null as string) AS vessel_id,
            ssvid,
            shipname AS core_shipname,
            n_shipname AS core_n_shipname,
            n_callsign AS core_n_callsign,
            imo AS core_imo,
            flag AS core_flag,
            geartype AS core_geartype,
            is_fishing AS core_is_fishing,
            is_carrier AS core_is_carrier,
            is_bunker AS core_is_bunker,
            length_m AS core_length_m,
            tonnage_gt AS core_tonnage_gt,
            first_timestamp AS core_first_timestamp,
            last_timestamp AS core_last_timestamp,
            source_code AS core_source_code,
            cast(null as string) AS vi_shipname,
            cast(null as string) AS vi_n_shipname,
            cast(null as string) AS vi_callsign,
            cast(null as string) AS vi_norm_callsign,
            cast(null as string) AS vi_imo,
            cast(null as string) AS vi_flag,
            cast(null as timestamp) AS vi_first_timestamp,
            cast(null as timestamp) AS vi_last_timestamp,
            cast(null as integer) AS vi_msg_count,
            cast(null as integer) AS vi_pos_count,
            false AS ishipname_match,
            false AS iare_names_close_03_fixed,
            false AS inorm_callsign_match,
            false AS imo_match,
            'NO_MATCH' AS match_fields,
            'NO_CONFIDENCE' AS confidence
        FROM source_vessel_identity_core
        WHERE vessel_record_id NOT IN (
            SELECT DISTINCT vessel_record_id
            FROM core_and_registry
        )
    ),


    ###########################################################################################
    # Step 4 - JOIN MATCHED, AIS ONLY (only_core), AND REGISTRY ONLY (only_registry) TOGETHER #
    ###########################################################################################
    union_all AS (
        SELECT DISTINCT * FROM core_and_registry
        UNION ALL
        SELECT DISTINCT * FROM only_core
        UNION ALL
        SELECT DISTINCT * FROM only_registry
    )

#### RESULTS
SELECT
    source,
    vessel_record_id,
    vessel_id,
    ssvid,
    core_shipname,
    core_n_shipname,
    core_n_callsign,
    core_imo,
    core_flag,
    core_geartype,
    core_is_fishing,
    core_is_carrier,
    core_is_bunker,
    core_length_m,
    core_tonnage_gt,
    core_first_timestamp,
    core_last_timestamp,
    core_source_code,
    vi_shipname,
    vi_n_shipname,
    vi_callsign,
    vi_norm_callsign,
    vi_imo,
    vi_flag,
    vi_first_timestamp,
    vi_last_timestamp,
    vi_msg_count,
    vi_pos_count,
    ishipname_match,
    iare_names_close_03_fixed,
    inorm_callsign_match,
    imo_match,
    match_fields,
    confidence
FROM union_all;