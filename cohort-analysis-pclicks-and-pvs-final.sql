--  test 1 --

WITH base AS (
     SELECT
        DATE(_pt) as date,
        test,
        variant,
        -- COUNT(DISTINCT pageview_id) AS pageviews ,
        pageview_id,
        -- COUNT(DISTINCT COALESCE(cast(combined_regi_id AS STRING),agent_id)) as agent_id
        COALESCE(cast(combined_regi_id AS STRING),agent_id) as agent_id
   
    FROM
        `nyt-eventtracker-prd.et.page`,
        UNNEST(ab_exposes)
    WHERE
     
        test = 'Wirecutter_Regi_Wall_III'
        AND source_app LIKE '%wirecutter%'
        AND DATE(_pt) BETWEEN '2023-02-01' AND '2023-02-28'
        -- AND combined_regi_id IS NOT NULL
        AND COALESCE(cast(combined_regi_id AS STRING),agent_id) NOT IN -- Dedup agents 
                (
                    SELECT
                        agent_id
                    FROM `nyt-eventtracker-prd.et.page`, unnest(ab_exposes)
                    WHERE DATE_TRUNC(DATE(_pt), WEEK(monday)) BETWEEN '2023-02-01' AND '2023-02-28'
                    AND source_app LIKE '%wirecutter%'
                    AND agent_id IS NOT NULL
                    AND test = 'Wirecutter_Regi_Wall_III'
                    GROUP BY 1
                    HAVING COUNT(DISTINCT variant) > 1
                )
    -- GROUP BY 1,2,3,5
)
,
clicks AS
(
    SELECT
        DATE(_pt) as date,
        -- base.variant,
        COALESCE(cast(pg.combined_regi_id AS STRING),pg.agent_id) as agent_id,
        -- base.agent_id,
        int.module.element.name AS pclicks,
        pg.pageview_id,
        -- base.pageview_id
    FROM
        nyt-eventtracker-prd.et.page AS pg,
        unnest(interactions) AS int
    
    -- RIGHT JOIN base
    -- ON
    --     base.agent_id = pg.agent_id
    --     AND base.pageview_id = pg.pageview_id
    --     AND base.date = DATE(pg._pt)
    WHERE
        DATE(_pt)  BETWEEN '2023-02-01' AND '2023-02-28'
        AND source_app LIKE '%wirecutter%'
        AND int.module.element.name LIKE '%outbound_product%'
    -- GROUP BY 1
),
final AS (

 SELECT 
    b.date,
    variant,
    -- COUNT(DISTINCT agent_id) AS users,
    b.agent_id,
    COUNT(DISTINCT b.pageview_id) AS pageviews,
    COUNT(pclicks) AS pclicks

 FROM base b
 LEFT JOIN clicks c ON c.pageview_id = b.pageview_id AND c.date = c.date

    WHERE variant = "0_control" AND b.date BETWEEN '2023-02-01' AND '2023-02-28'
    GROUP BY 1,2,3


)
SELECT 

    DATE_TRUNC(date, WEEK(MONDAY)) + 6 AS date,
    variant,
    COUNT(DISTINCT agent_id) AS users,
    SUM(pclicks) as pclicks,
    SUM(pageviews) AS pageviews,
    CASE 
        WHEN pageviews > 10 THEN 11
        ELSE pageviews
    END AS pv_bucket

FROM final
GROUP BY 1,2,6
ORDER BY 1,5

   