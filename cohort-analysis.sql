 
WITH base AS (
     SELECT
        -- DATE_TRUNC(DATE(_pt),WEEK(MONDAY)) + 6  AS week_end,
        DATE(_pt) as date,
        test,
        variant,
        COUNT(DISTINCT pageview_id) AS pageviews ,
        -- COUNT(DISTINCT COALESCE(cast(combined_regi_id AS STRING),agent_id)) as agent_id
        COALESCE(cast(combined_regi_id AS STRING),agent_id) as agent_id
   
    FROM
        `nyt-eventtracker-prd.et.page`,
        UNNEST(ab_exposes)
    WHERE
     
        test = 'Wirecutter_Regi_Wall_III'
        AND source_app LIKE '%wirecutter%'
        AND DATE(_pt) between '2023-02-01' AND '2023-02-28'
        -- AND combined_regi_id IS NOT NULL
        AND COALESCE(cast(combined_regi_id AS STRING),agent_id) NOT IN 
                (
                    SELECT
                        agent_id
                    FROM `nyt-eventtracker-prd.et.page`, unnest(ab_exposes)
                    WHERE DATE_TRUNC(DATE(_pt), WEEK(monday)) between '2023-02-01' AND '2023-02-28'
                    AND source_app LIKE '%wirecutter%'
                    AND agent_id IS NOT NULL
                    AND test = 'Wirecutter_Regi_Wall_III'
                    GROUP BY 1
                    HAVING COUNT(DISTINCT variant) > 1
                )
    GROUP BY 1,2,3,5
    -- ORDER BY 1,2,3
)
-- ,
-- clicks AS
-- (
--     SELECT
--         -- DATE_TRUNC(DATE(_pt), WEEK(monday)) + 6 AS week_end,
--         DATE(_pt) as date,
--         base.variant,
--         COALESCE(cast(pg.combined_regi_id AS STRING),pg.agent_id) as agent_id,
--         int.module.element.name AS pclicks,
--         pg.pageview_id

--     FROM
--         nyt-eventtracker-prd.et.page AS pg,
--         unnest(interactions) AS int
--     JOIN base
--     ON
--         base.agent_id = pg.agent_id
--         AND base.pageview_id = pg.pageview_id
--         AND base.date = DATE(pg._pt)
--     WHERE
--         DATE_TRUNC(DATE(_pt), WEEK(monday))  between '2023-02-01' AND '2023-02-28'
--         AND source_app LIKE '%wirecutter%'
--         AND int.module.element.name LIKE '%outbound_product%'
--     -- GROUP BY 1
-- )

SELECT 
    --  DATE_TRUNC(b.date, WEEK(MONDAY)) + 6 AS week_end,
    DATE_TRUNC(b.date, MONTH) AS date,
     b.variant,
     COUNT(DISTINCT b.agent_id) as users,
     CASE
        WHEN pageviews > 10 THEN 11
        ELSE pageviews
     END AS pv_bucket
    -- b.agent_id as users,
    -- b.pageview_id as pvs,
    -- c.pclicks

    --  COUNT(c.pclicks) AS pclicks

FROM base b
WHERE variant = "0_control"
GROUP BY 1,2,4
ORDER BY 1,4
-- LEFT JOIN clicks c ON b.pageview_id = c.pageview_id
-- GROUP BY 1,2
   