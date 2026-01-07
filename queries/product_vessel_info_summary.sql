/*

product_vessel_info_summary -> Old all_vessels_byyear_v2 (pipe 2.5)

Variables to replace
====================
date_YYYYMMDD => It is the date of the version that we want to generate

source_base_vessel_identity
    pipe2_5 -> world-fishing-827.pipe_production_v20201001.base_vessel_identity_info_merge
    pipe3 -> world-fishing-827.pipe_ais_v3_published.product_vessel_info_match

source_ssvid_byyear
    pipe2_5 -> world-fishing-827.gfw_research.vi_ssvid_byyear
    pipe3 -> world-fishing-827.pipe_ais_v3_published.vi_ssvid_byyear_v

source_purse_seine_support_vessels_byyear
    pipe2_5 -> world-fishing-827.vessel_database.purse_seine_support_vessels_byyear
    pipe3 -> world-fishing-827.pipe_ais_v3_internal.purse_seine_support_vessels_byyear_v

*/

## comment this out to simplify the code

-- CREATE TEMP FUNCTION get_month()
--     RETURNS INTEGER
-- AS (
--     EXTRACT(MONTH from PARSE_DATE('%Y%m%d', '{{date_YYYYMMDD}}'))
--     );
-- 
-- CREATE TEMP FUNCTION get_current_year()
--     RETURNS INTEGER
-- AS (
--     EXTRACT(YEAR from PARSE_DATE('%Y%m%d', '{{date_YYYYMMDD}}'))
--     );
-- 
-- CREATE TEMP FUNCTION get_next_year()
--     RETURNS INTEGER
-- AS (
--     get_current_year() + 1
--     );
-- 
-- CREATE TEMP FUNCTION get_previous_year()
--     RETURNS INTEGER
-- AS (
--     get_current_year() -1
--     );

WITH

    ####
    #### SOURCE TABLES
    ####
    
    ### ADD manual feedback vessel information
    source_vessel_feedback AS (
    	SELECT DISTINCT
    		Vessel_ID as vessel_id,
    		Updated_vessel_class as feedback_vessel_class,
    		Most_specific_known_vessel_type as raw_feedback_vessel_class
    	FROM `world-fishing-827.scratch_willa_ttl100.poc_gfw_map_vessel_corrections`
    	WHERE Feedback_Action = 'Approved'
    	),

    ### vessel API initial table (step 1)
    ### TO UPDATE SOURCE TABLE ONCE WE GET BASE VESSEL INFO TABLE WORKING AS WE INTEND
    source_vessel_api AS (
        SELECT
            vessel_id,
            ssvid,
            confidence,
            year,
            vi_msg_count AS vessel_id_msg_count,
            vi_pos_count AS vessel_id_pos_count,
            vi_shipname AS shipname,
            vi_callsign AS callsign,
            vi_imo AS imo,
            vi_flag AS mmsi_flag,
            core_flag,
            core_geartype,
            core_is_carrier,
            core_is_bunker,
        FROM `global-fishing-watch.pipe_ais_v3_published.product_vessel_info_match` vi,
             UNNEST(GENERATE_ARRAY(EXTRACT(YEAR FROM vi.vi_first_timestamp), EXTRACT(YEAR FROM vi.vi_last_timestamp))) as year
        -- only keep AIS/core results
        WHERE source != 'ONLY_REGISTRY'
    ),

    ### vi ssvid by year
    ### where most summary values about AIS activity come from
    source_vi_ssvid_by_year AS (
        SELECT
            ssvid,
            year,
            activity,
            ais_identity,
            inferred,
            registry_info,
            best,
            on_fishing_list_known,
            on_fishing_list_nn,
            on_fishing_list_sr,
            on_fishing_list_best
        FROM `global-fishing-watch.pipe_ais_v3_published.vi_ssvid_byyear_v`
    ),

    ### support vessels
    ### table where support vessels info comes from
    source_purse_seine_support_vessel_byyear_table AS (
        SELECT
            mmsi,
            year
        FROM `gfw-int-vessel-identity-v1.pipe_vessel_identity_ais_v3_internal.purse_seine_support_vessels_byyear`
    ),

    ####
    #### POPULATED ATTRIBUTES
    ####


    ### list of fishing vessel types
    ###
    fishing_classes AS (
        SELECT DISTINCT
            best.best_vessel_class AS fv
        FROM source_vi_ssvid_by_year
        WHERE on_fishing_list_best
    ),

    ### unique vessel names per mmsi
    ### from tyler fv query
    shipname_count AS (
        SELECT
            * EXCEPT(value, count),
            COUNT(*) as shipname_count
        FROM (
                 SELECT
                     ssvid,
                     year,
                     value,
                     SUM(count) AS count
                 FROM source_vi_ssvid_by_year,
                     UNNEST(ais_identity.n_shipname)
                 WHERE value IS NOT NULL
                 GROUP BY ssvid, year, value
             )
        WHERE
            count >= 10
        GROUP BY ssvid, year
    ),
    
    ### populate manual feedback shiptype and geartype
    ###
	vessel_feedback AS (
  		SELECT
    		vessel_id,
    		feedback_vessel_class,
    		raw_feedback_vessel_class,
    	-- shiptype
    		CASE
      		WHEN feedback_vessel_class IN (SELECT fv FROM fishing_classes) THEN 'fishing'
      		WHEN feedback_vessel_class IN (
        	'support','carrier','bunker','discrepancy','gear',
        	'cargo','passenger','seismic_vessel'
      		) THEN feedback_vessel_class
      		ELSE 'other'
    		END AS feedback_shiptype,
    	-- geartype
    		CASE
      		WHEN feedback_vessel_class IN (SELECT fv FROM fishing_classes)
       		 OR feedback_vessel_class IN (
        	'support','carrier','bunker','discrepancy','gear',
        	'cargo','passenger','seismic_vessel') THEN feedback_vessel_class
      		ELSE 'other'
    		END AS feedback_geartype
  		FROM source_vessel_feedback
	),
    
    #### MOVED REGISTRY ABOVE VI SSVID TO CONNECT MANUAL FEEDBACK
    ### select fields of interest in vessel api step 1 (base vessel info)
    ### make registry values for LOW or NO CONFIDENCES NULL
    ### make sure only one core_geartype, core_is_carrier, core_is_bunker result per vessel_id and year combination
    api AS (
        SELECT
            * EXCEPT(hc_flag, hc_geartype, hc_is_carrier, hc_is_bunker),
            STRING_AGG(DISTINCT hc_flag, '|' ORDER BY hc_flag) AS core_flag,
            STRING_AGG(DISTINCT hc_geartype, '|' ORDER BY hc_geartype) AS core_geartype,
            MAX(hc_is_carrier) AS core_is_carrier,
            MAX(hc_is_bunker) AS core_is_bunker,
        FROM (
                 SELECT
                     * EXCEPT(confidence, core_flag, core_geartype, core_is_carrier, core_is_bunker),
                     CASE WHEN confidence = 'HIGH' THEN IFNULL(core_flag, '')
                          ELSE NULL END AS hc_flag,
                     CASE WHEN confidence = 'HIGH' THEN IFNULL(core_geartype, '')
                          ELSE NULL END AS hc_geartype,
                     CASE WHEN confidence = 'HIGH' THEN core_is_carrier ELSE FALSE END AS hc_is_carrier,
                     CASE WHEN confidence = 'HIGH' THEN core_is_bunker ELSE FALSE END AS hc_is_bunker
                 FROM source_vessel_api
             )
        GROUP BY
            vessel_id, ssvid, shipname, callsign, imo, mmsi_flag,
            year, vessel_id_msg_count, vessel_id_pos_count
    ),
    
    ### connect API and feedback on vessel_id 
    ###
    api_and_feedback AS (
    SELECT
        api.*,
        feedback.* EXCEPT (vessel_id),
        feedback.feedback_vessel_class IS NOT NULL AS has_feedback_override
    FROM api
    LEFT JOIN vessel_feedback as feedback
        USING (vessel_id)
    ),

    vi_ssvid_by_year AS (
        SELECT
            api_and_feedback.* EXCEPT (ssvid, year),
            ship.shipname_count AS shipname_count,
            vi.*,
            best.best_flag AS gfw_best_flag,
            best.best_vessel_class AS best_vessel_class,
            -- add best registry vessel class
            (
                SELECT STRING_AGG(IFNULL(value, ''), '|' ORDER BY value)
                FROM UNNEST(registry_info.best_known_vessel_class) AS value
            ) AS registry_vessel_class,
            -- add neural net vessel class ag
            inferred.inferred_vessel_class_ag AS inferred_vessel_class_ag,
            -- count rows to remove duplicates, but Jenn suggest way query is re-written, duplicates are removed already and dont need this line of code
            row_number() over (PARTITION BY ssvid, year) AS row,
            -- fields used to idenitify good fishing vessels
            activity.offsetting as offsetting,
            activity.overlap_hours_multinames as overlap_hours_multinames,
            activity.active_hours as active_hours,
            activity.fishing_hours as fishing_hours,
            activity.positions as positions,
            activity.active_positions as active_positions,
            -- best best vessel class is fishing gear
            best.best_vessel_class IN (
                select fv
                FROM fishing_classes
            ) as best_best_fishing,
            -- new boolean to see if vessel on any our fishing lists, with exception of self reported list
            CASE
                WHEN on_fishing_list_known OR on_fishing_list_nn OR on_fishing_list_best OR best.best_vessel_class IN (select fv FROM fishing_classes) OR api_and_feedback.feedback_shiptype = 'fishing' THEN TRUE
                ELSE FALSE
                END AS potential_fishing,
            -- add potential_fishing vessel class SOURCE to use
            COALESCE(
                    CASE
                        WHEN api_and_feedback.feedback_shiptype = 'fishing' THEN 'verified_feedback'
                        WHEN on_fishing_list_best THEN 'on_fishing_list_best'
                        WHEN best.best_vessel_class IN (select fv FROM fishing_classes) THEN 'best_vessel_class'
                        WHEN on_fishing_list_known THEN 'registry'
                        WHEN on_fishing_list_nn THEN 'inferred'
                        ELSE NULL END) AS potential_fishing_source,
            -- mmsi and year in support by year table
            ssvid IN (
                SELECT mmsi
                FROM source_purse_seine_support_vessel_byyear_table as ps
                WHERE ps.year = vi.year
            ) as in_support_list,
        FROM api_and_feedback 
                 LEFT JOIN source_vi_ssvid_by_year as vi 
        		USING (ssvid, year)
                 LEFT JOIN shipname_count as ship USING (ssvid, year)
    ),

    ### add fields for products
    ### prod_shiptype, prod_geartype, fv_confidence, noise
    av_populate_fields AS (
        SELECT
            *,
            -- shiptype
            CASE
                WHEN has_feedback_override THEN feedback_shiptype
                WHEN in_support_list THEN 'support'
                WHEN core_is_carrier THEN 'carrier'
                WHEN core_is_bunker THEN 'bunker'
                WHEN (NOT on_fishing_list_best AND NOT best_best_fishing) AND ( ((on_fishing_list_known
                    AND NOT on_fishing_list_nn) OR (NOT on_fishing_list_known AND on_fishing_list_nn)) ) THEN 'discrepancy'
                WHEN best_vessel_class = 'gear' THEN 'gear'
                WHEN potential_fishing THEN 'fishing'
                WHEN best_vessel_class = 'cargo' THEN 'cargo'
                WHEN best_vessel_class = 'passenger' THEN 'passenger'
                WHEN best_vessel_class = 'seismic_vessel' THEN 'seismic_vessel'
                ELSE 'other'
                END AS prod_shiptype,
            -- geartype
            CASE
                WHEN has_feedback_override THEN feedback_geartype
                WHEN in_support_list THEN 'purse_seine_support'
                WHEN core_is_carrier THEN 'carrier'
                WHEN core_is_bunker THEN 'bunker'
                WHEN (NOT on_fishing_list_best AND NOT best_best_fishing) AND ( ((on_fishing_list_known
                    AND NOT on_fishing_list_nn) OR (NOT on_fishing_list_known AND on_fishing_list_nn)) ) THEN 'inconclusive'
                WHEN best_vessel_class = 'gear' THEN 'gear'
                WHEN potential_fishing THEN IF (best_vessel_class IS null, 'inconclusive', best_vessel_class)
                WHEN best_vessel_class = 'cargo' THEN 'cargo'
                WHEN best_vessel_class = 'passenger' THEN 'passenger'
                WHEN best_vessel_class = 'seismic_vessel' THEN 'seismic_vessel'
                ELSE 'other'
                END AS prod_geartype,
            -- geartype source
            CASE
                WHEN has_feedback_override THEN 'verified_feedback'
                WHEN in_support_list THEN 'support_vessel_list'
                WHEN core_is_carrier THEN 'core_is_carrier'
                WHEN core_is_bunker THEN 'core_is_bunker'
                WHEN (NOT on_fishing_list_best AND NOT best_best_fishing) AND ( ((on_fishing_list_known
                    AND NOT on_fishing_list_nn) OR (NOT on_fishing_list_known AND on_fishing_list_nn)) ) THEN 'gfw_research_vi_ssvid_fishing_list_nn_and_known'
                WHEN best_vessel_class = 'gear' THEN 'gfw_research_vi_ssvid_best_vessel_class'
                WHEN potential_fishing THEN 'gfw_research_vi_ssvid_best_vessel_class'
                WHEN best_vessel_class = 'cargo' THEN 'gfw_research_vi_ssvid_best_vessel_class'
                WHEN best_vessel_class = 'passenger' THEN 'gfw_research_vi_ssvid_best_vessel_class'
                WHEN best_vessel_class = 'seismic_vessel' THEN 'gfw_research_vi_ssvid_best_vessel_class'
                ELSE 'not_applicable'
                END AS prod_geartype_source,
            -- noisy
            CASE
                WHEN offsetting OR overlap_hours_multinames >= 24 THEN TRUE
                ELSE FALSE
                END AS noisy_vessel
        FROM vi_ssvid_by_year --vi_api_join
    ),

    ## add chunk of select fields that are easier to look at
    all_vessels AS (
        SELECT
            * EXCEPT(
            activity,
            ais_identity, inferred, registry_info, best, on_fishing_list_known, on_fishing_list_nn,
            row, best_best_fishing
            )
        FROM av_populate_fields
    )

    -- ## For the versions between December to June, we copy the vessels for the last year to the new year
    -- all_vessels_extended AS (
    --     SELECT
    --         * EXCEPT(year),
    --         year
    --     FROM all_vessels
    --     WHERE year <= IF(get_month() > 11, get_current_year(), IF (get_month() < 6,  get_previous_year(), get_next_year()) )
    --     UNION ALL
    --     SELECT
    --         * EXCEPT(year),
    --         IF(get_month() > 11, get_next_year(), IF (get_month() < 6,  get_current_year(), year) ) as year
    --     FROM all_vessels
    --     WHERE year = IF(get_month() > 11,  get_current_year(), IF (get_month() < 6,  get_previous_year(), null) )
    -- )

SELECT DISTINCT
    vessel_id,
    ssvid,
    year,
    shipname,
    callsign,
    imo,
    mmsi_flag,
    gfw_best_flag,
    core_flag,
    best_vessel_class,
    registry_vessel_class,
    inferred_vessel_class_ag,
    core_geartype,
    feedback_vessel_class,
    raw_feedback_vessel_class,
    has_feedback_override,
    prod_shiptype,
    prod_geartype,
    prod_geartype_source,
    core_is_carrier,
    core_is_bunker,
    in_support_list,
    on_fishing_list_best,
    on_fishing_list_sr,
    potential_fishing,
    potential_fishing_source,
    noisy_vessel,
    shipname_count,
    offsetting,
    overlap_hours_multinames,
    active_hours,
    fishing_hours,
    positions,
    active_positions,
    vessel_id_msg_count,
    vessel_id_pos_count
FROM all_vessels