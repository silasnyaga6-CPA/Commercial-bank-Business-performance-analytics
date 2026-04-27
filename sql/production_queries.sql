--QUERY 1 — Monthly Product Performance with MoM, YoY & SR Trend

-- **Business use:** First Monday of every month — the full product P&L view.
-- Covers all products × all subsidiaries, MoM growth, year-on-year where
-- available, and success rate change in percentage points.

-- ## QUERY 1 — Monthly Product Performance (PostgreSQL Production Version)

WITH monthly AS (
    SELECT
        TO_CHAR(t."TRANSACTION_DATE"::DATE, 'YYYY-MM') AS txn_month,

        t."PRODUCT",
        t."SUBSIDIARY",
        t."PRODUCT_CATEGORY",

        COUNT(*) AS total_txns,

        SUM(CASE WHEN t."STATUS" = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_txns,
        SUM(CASE WHEN t."STATUS" = 'FAILED'  THEN 1 ELSE 0 END) AS failed_txns,

        SUM(t."TXN_AMOUNT_USD")::NUMERIC AS value_usd,
        AVG(t."TXN_AMOUNT_LOCAL")::NUMERIC AS avg_txn_local,
        AVG(t."PROCESSING_TIME_SECS")::NUMERIC AS avg_proc_secs,
        SUM(t."FEE_AMOUNT_LOCAL")::NUMERIC AS total_fees_local,

        COUNT(DISTINCT t."CUSTOMER_ID") AS unique_customers

    FROM transactions t
    GROUP BY 1,2,3,4
),
with_growth AS (
    SELECT
        *,
	LAG(total_txns, 1) OVER (PARTITION BY "PRODUCT", "SUBSIDIARY" ORDER BY txn_month) AS prev_mo_txns,
	LAG(total_txns, 12) OVER (PARTITION BY "PRODUCT", "SUBSIDIARY" ORDER BY txn_month) AS prev_yr_txns,
    LAG(value_usd, 1) OVER (PARTITION BY "PRODUCT", "SUBSIDIARY" ORDER BY txn_month) AS prev_mo_val,
	LAG(unique_customers, 1) OVER (PARTITION BY "PRODUCT", "SUBSIDIARY" ORDER BY txn_month) AS prev_customers,   
	LAG((100.0 * successful_txns / NULLIF(total_txns, 0))::NUMERIC,1 ) OVER (PARTITION BY "PRODUCT", "SUBSIDIARY" ORDER BY txn_month) AS prev_mo_sr
FROM monthly)

SELECT
    txn_month,
    "PRODUCT",
    "SUBSIDIARY",
    "PRODUCT_CATEGORY",
	total_txns,
    successful_txns,
    failed_txns,

    ROUND((100.0 * successful_txns / NULLIF(total_txns, 0))::NUMERIC,1) AS success_rate_pct,
    ROUND((100.0 * failed_txns / NULLIF(total_txns, 0))::NUMERIC,1) AS failure_rate_pct,

    ROUND(value_usd, 2) AS value_usd,
    ROUND(avg_txn_local, 2) AS avg_txn_local,
    ROUND(avg_proc_secs, 2) AS avg_proc_secs,

    unique_customers,

    ROUND((100.0 * (total_txns - prev_mo_txns) / NULLIF(prev_mo_txns, 0))::NUMERIC, 1 ) AS mom_vol_pct,
    ROUND((100.0 * (total_txns - prev_yr_txns) / NULLIF(prev_yr_txns, 0))::NUMERIC, 1) AS yoy_vol_pct,
    ROUND((100.0 * (value_usd - prev_mo_val) / NULLIF(prev_mo_val, 0))::NUMERIC,1) AS mom_val_pct,
    ROUND((100.0 * (unique_customers - prev_customers) / NULLIF(prev_customers, 0))::NUMERIC, 1 ) AS mom_customer_growth,
    ROUND(((100.0 * successful_txns / NULLIF(total_txns, 0)) - prev_mo_sr)::NUMERIC, 2 ) AS sr_change_pp,
    ROUND((total_fees_local / NULLIF(value_usd, 0))::NUMERIC,4 ) AS fee_to_value_ratio,
CASE
     WHEN (100.0 * successful_txns / NULLIF(total_txns, 0)) < 95 THEN  'BREACH'ELSE 'OK'END AS sla_status
FROM with_growth
WHERE TO_DATE(txn_month, 'YYYY-MM')
      >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3 months'AND "SUBSIDIARY" = 'Equity Bank Kenya'
ORDER BY txn_month DESC, total_txns DESC;

-- ## QUERY 2 — KPI Actual vs Target | Full RAG Dashboard

-- **Business use:** Product House monthly review. Three RAG columns — volume,
-- value, success rate — per product/subsidiary combination.


WITH actuals AS (
    SELECT
        TRIM(UPPER(t."PRODUCT"))                                  AS "PRODUCT",
        TRIM(UPPER(t."SUBSIDIARY"))                               AS "SUBSIDIARY",
		date_trunc('month', t."TRANSACTION_DATE"::date)::date     AS report_month,
		COUNT(*)                                                  AS actual_vol,
		ROUND(SUM(COALESCE(t."TXN_AMOUNT_USD",0))::numeric, 2)    AS actual_val_usd,
		ROUND( (100.0 * SUM(CASE WHEN TRIM(UPPER(t."STATUS")) = 'SUCCESS' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0))::numeric, 2)AS actual_sr,
		COUNT(DISTINCT t."CUSTOMER_ID")                           AS actual_custs,
		COUNT(DISTINCT CASE WHEN t."AGENT_ID" IS NOT NULL AND TRIM(t."AGENT_ID") <> ''THEN TRIM(UPPER(t."AGENT_ID"))END)AS active_agents
	 FROM transactions t
     WHERE t."TRANSACTION_DATE"::date >= date_trunc('month', CURRENT_DATE - INTERVAL '1 month')
      AND t."TRANSACTION_DATE"::date <  date_trunc('month', CURRENT_DATE)
GROUP BY 1,2,3)	
SELECT
    a."PRODUCT",
    a."SUBSIDIARY",
	
 -- VOLUME KPI
    a.actual_vol,
    k."TARGET_VOLUME",
	ROUND((100.0 * a.actual_vol / NULLIF(k."TARGET_VOLUME",0))::numeric, 1) AS vol_attainment_pct,
	CASE 
	WHEN a.actual_vol >= k."TARGET_VOLUME" * 0.95 THEN 'GREEN'     
   	WHEN a.actual_vol >= k."TARGET_VOLUME" * 0.80 THEN 'AMBER'       
    ELSE 'RED' END AS vol_rag,

    -- VALUE KPI
    ROUND(a.actual_val_usd, 2)                                     AS actual_val_usd,
    ROUND(k."TARGET_VALUE_USD"::numeric, 2)                         AS target_val_usd,
	ROUND((100.0 * a.actual_val_usd / NULLIF(k."TARGET_VALUE_USD",0))::numeric, 1) AS val_attainment_pct,
	CASE 
        WHEN a.actual_val_usd >= k."TARGET_VALUE_USD" * 0.95 THEN 'GREEN'
        WHEN a.actual_val_usd >= k."TARGET_VALUE_USD" * 0.80 THEN 'AMBER'
        ELSE 'RED'
    END AS val_rag,

    -- SUCCESS RATE KPI
    a.actual_sr,
    k."TARGET_SUCCESS_RATE",
	CASE 
        WHEN a.actual_sr >= k."TARGET_SUCCESS_RATE"     THEN 'GREEN'
        WHEN a.actual_sr >= k."TARGET_SUCCESS_RATE" - 5 THEN 'AMBER'
        ELSE 'RED'
    END AS sr_rag,

    -- CUSTOMER KPI
    a.actual_custs,
    k."TARGET_ACTIVE_CUSTOMERS",
	ROUND((100.0 * a.actual_custs / NULLIF(k."TARGET_ACTIVE_CUSTOMERS",0))::numeric, 1) AS cust_attainment_pct,

    -- AGENT KPI
    a.active_agents

FROM actuals a
JOIN kpi_targets k  
    ON TRIM(UPPER(a."PRODUCT"))    = TRIM(UPPER(k."PRODUCT"))
   AND TRIM(UPPER(a."SUBSIDIARY")) = TRIM(UPPER(k."SUBSIDIARY_NAME"))
   AND a.report_month              = k."TARGET_MONTH"::date

ORDER BY vol_attainment_pct ASC;


-- ## QUERY 3 — Failure Spike Detection with WoW % Change

-- **Business use:** Runs hourly in production. Flags any failure code × channel × product
-- that exceeds 1.5× its three-week rolling average — triggers page to IT Ops / Fraud

WITH failures AS (
    SELECT
        t."CHANNEL"            AS channel,
        t."CHANNEL_TYPE"      AS channel_type,
        t."PRODUCT"           AS product,
        t."FAILURE_DESCRIPTION" AS failure_description,
        t."FAILURE_CATEGORY"  AS failure_category,

        CASE 
            WHEN t."TRANSACTION_DATE"::date >= CURRENT_DATE - INTERVAL '7 days'
            THEN 'this_week'
            ELSE 'prior_3wk'
        END AS period,

        COUNT(*) AS cnt,

        ROUND(SUM(COALESCE(t."TXN_AMOUNT_LOCAL",0))::numeric, 2) AS failed_value,
        ROUND(SUM(COALESCE(t."TXN_AMOUNT_USD",0))::numeric, 2)   AS failed_value_usd

    FROM transactions t
    WHERE t."STATUS" = 'FAILED'
      AND t."FAILURE_DESCRIPTION" IS NOT NULL
      AND TRIM(t."FAILURE_DESCRIPTION") <> ''
      AND t."TRANSACTION_DATE"::date >= CURRENT_DATE - INTERVAL '28 days'
    GROUP BY 1,2,3,4,5,6
),

pivoted AS (
    SELECT
        channel,
        channel_type,
        product,
        failure_description,
        failure_category,

        SUM(CASE WHEN period = 'this_week' THEN cnt ELSE 0 END) AS this_week,
        SUM(CASE WHEN period = 'prior_3wk' THEN cnt ELSE 0 END) AS prior_3wk,

        SUM(CASE WHEN period = 'this_week' THEN failed_value ELSE 0 END) AS tw_val_local,
        SUM(CASE WHEN period = 'this_week' THEN failed_value_usd ELSE 0 END) AS tw_val_usd

    FROM failures
    GROUP BY 1,2,3,4,5
)

SELECT
    channel,
    product,
    failure_description,
    failure_category,

    this_week,

    ROUND(prior_3wk / 3.0, 1) AS avg_prior_wk,

    ROUND(tw_val_local::numeric, 2) AS failed_val_local,
    ROUND(tw_val_usd::numeric, 2)   AS failed_val_usd,

    CASE 
        WHEN prior_3wk = 0 THEN 'NEW'
        WHEN this_week > (prior_3wk / 3.0) * 1.5 THEN 'SPIKING'
        WHEN this_week < (prior_3wk / 3.0) * 0.7 THEN 'IMPROVING'
        ELSE 'STABLE'
    END AS trend_flag,

    ROUND(
        100.0 * (this_week - (prior_3wk / 3.0)) 
        / NULLIF((prior_3wk / 3.0), 0),
        0
    ) AS wow_pct

FROM pivoted
WHERE this_week > 0
ORDER BY this_week DESC, tw_val_usd DESC;


-- ## QUERY 4 — Agent Scorecard | NTILE Quartile + Float Risk + Dormancy

-- **Business use:** Monthly field team briefing. Identifies top/bottom agents,
-- float top-up candidates, and dormant agents for reactivation.

WITH agent_stats AS (
    SELECT
        t."AGENT_ID",
        a."AGENT_NAME",
        a."AGENT_TIER",
        a."REGION",
        a."COUNTY",
        a."FLOAT_LIMIT_KES",

        COUNT(*) AS total_txns,

        SUM(CASE WHEN t."STATUS" = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_txns,
        SUM(CASE WHEN t."STATUS" = 'FAILED'  THEN 1 ELSE 0 END) AS failed_txns,

        ROUND(SUM(COALESCE(t."TXN_AMOUNT_LOCAL",0))::numeric, 2) AS total_value_kes,
        ROUND(SUM(COALESCE(t."TXN_AMOUNT_USD",0))::numeric, 2)   AS total_value_usd,
        ROUND(SUM(COALESCE(t."FEE_AMOUNT_LOCAL",0))::numeric, 2) AS commission_kes,

        COUNT(DISTINCT t."CUSTOMER_ID") AS unique_customers,

        COUNT(DISTINCT t."TRANSACTION_DATE"::date) AS active_days,

        MAX(t."TRANSACTION_DATE"::date) AS last_txn_date,

        SUM(CASE WHEN t."FAILURE_CODE" = 'INSUFF_FLOAT' THEN 1 ELSE 0 END) AS float_failures,

        ROUND(AVG(t."PROCESSING_TIME_SECS")::numeric, 0) AS avg_proc_secs

    FROM transactions t
    JOIN agents a 
        ON t."AGENT_ID" = a."AGENT_ID"

    WHERE t."PRODUCT" = 'Agency Banking'
      AND t."TRANSACTION_DATE"::date >= date_trunc('month', CURRENT_DATE - INTERVAL '1 month')
      AND t."TRANSACTION_DATE"::date <  date_trunc('month', CURRENT_DATE)
      AND t."AGENT_ID" IS NOT NULL
      AND TRIM(t."AGENT_ID") <> ''

    GROUP BY 1,2,3,4,5,6
),

ranked AS (
    SELECT *,
        ROUND(
            100.0 * successful_txns / NULLIF(total_txns,0),
            1
        ) AS success_rate,

        ROUND(
            total_value_kes / NULLIF("FLOAT_LIMIT_KES",0),
            2
        ) AS float_util_ratio,

        ROUND(
            100.0 * commission_kes / NULLIF(total_value_kes,0),
            3
        ) AS commission_rate_pct,

        NTILE(4) OVER (
            PARTITION BY "REGION"
            ORDER BY total_txns DESC
        ) AS perf_quartile

    FROM agent_stats
    WHERE total_txns > 0
)

SELECT
    "AGENT_ID",
    "AGENT_TIER",
    "REGION",
    "COUNTY",

    total_txns,
    successful_txns,
    failed_txns,
    success_rate,

    ROUND(total_value_kes::numeric, 0) AS total_value_kes,
    ROUND(total_value_usd::numeric, 2) AS total_value_usd,

    commission_kes,
    unique_customers,
    active_days,

    float_failures,
    float_util_ratio,
    commission_rate_pct,

    avg_proc_secs,

    CASE perf_quartile
        WHEN 1 THEN 'TOP'
        WHEN 2 THEN 'UPPER'
        WHEN 3 THEN 'LOWER'
        ELSE 'BOTTOM'
    END AS band,

    CASE 
        WHEN float_failures > 5 THEN 'HIGH RISK'
        WHEN float_failures BETWEEN 1 AND 5 THEN 'MEDIUM'
        ELSE 'OK'
    END AS float_risk,

    CASE 
        WHEN last_txn_date < CURRENT_DATE - INTERVAL '14 days' THEN 'DORMANT'
        ELSE 'ACTIVE'
    END AS dormancy

FROM ranked
ORDER BY total_txns DESC;

-- ## QUERY 5 — Cross-Border Remittance Corridor Analysis

-- **Business use:** Treasury and Correspondent Banking monthly review.
-- Tracks which corridors are growing, SLA compliance, and fee competitiveness.

WITH corridors AS (
    SELECT
        t."SOURCE_COUNTRY" || '->' || t."DEST_COUNTRY" AS corridor,

        date_trunc('month', t."TRANSACTION_DATE"::date)::date AS txn_month,

        COUNT(*) AS total_txns,

        SUM(CASE WHEN t."STATUS" = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_txns,
        SUM(CASE WHEN t."STATUS" = 'FAILED'  THEN 1 ELSE 0 END) AS failed_txns,

        ROUND(SUM(COALESCE(t."TXN_AMOUNT_USD",0))::numeric, 2) AS value_usd,

        ROUND(AVG(COALESCE(t."TXN_AMOUNT_LOCAL",0))::numeric, 0) AS avg_txn_local,

        ROUND(
            (AVG(COALESCE(t."PROCESSING_TIME_SECS",0)) / 60.0)::numeric,
            1
        ) AS avg_proc_mins,

        ROUND(
            (
                AVG(
                    COALESCE(t."FEE_AMOUNT_LOCAL",0)
                    / NULLIF(t."TXN_AMOUNT_LOCAL",0)
                ) * 100
            )::numeric,
            3
        ) AS avg_fee_rate_pct,

        COUNT(DISTINCT t."CUSTOMER_ID") AS unique_senders

    FROM transactions t
    WHERE t."PRODUCT" = 'Equity Remit'
      AND t."SOURCE_COUNTRY" IS DISTINCT FROM t."DEST_COUNTRY"
      AND t."TRANSACTION_DATE"::date >= date_trunc('month', CURRENT_DATE - INTERVAL '3 months')
      AND t."TRANSACTION_DATE"::date <  date_trunc('month', CURRENT_DATE)

    GROUP BY 1,2
),

enriched AS (
    SELECT
        *,
        LAG(total_txns) OVER (
            PARTITION BY corridor
            ORDER BY txn_month
        ) AS prev_mo_txns
    FROM corridors
)

SELECT
    corridor,
    txn_month,

    total_txns,
    successful_txns,
    failed_txns,

    ROUND(
        (100.0 * successful_txns / NULLIF(total_txns,0))::numeric,
        1
    ) AS success_rate_pct,

    value_usd,
    avg_txn_local,
    avg_proc_mins,
    avg_fee_rate_pct,
    unique_senders,

    prev_mo_txns,

    ROUND(
        (
            100.0 * (total_txns - prev_mo_txns)
            / NULLIF(prev_mo_txns,0)
        )::numeric,
        1
    ) AS mom_growth_pct,

    RANK() OVER (
        PARTITION BY txn_month
        ORDER BY value_usd DESC
    ) AS rank_by_value

FROM enriched
ORDER BY txn_month DESC, value_usd DESC;

-- ## QUERY 6 — Rolling 3M Success Rate with Volatility Score
WITH monthly_sr AS (
    SELECT
        date_trunc('month', t."TRANSACTION_DATE"::date)::date AS txn_month,

        t."PRODUCT",
        t."SUBSIDIARY",

        COUNT(*) AS total,

        SUM(CASE WHEN t."STATUS" = 'SUCCESS' THEN 1 ELSE 0 END) AS success,

        ROUND(
            (100.0 * SUM(CASE WHEN t."STATUS" = 'SUCCESS' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*),0))::numeric,
            2
        ) AS sr,

        ROUND(SUM(COALESCE(t."TXN_AMOUNT_USD",0))::numeric, 2) AS value_usd

    FROM transactions t
    GROUP BY 1,2,3
),

enriched AS (
    SELECT
        *,
        AVG(sr) OVER (
            PARTITION BY "PRODUCT", "SUBSIDIARY"
            ORDER BY txn_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3m_sr,

        sr - LAG(sr,1) OVER (
            PARTITION BY "PRODUCT", "SUBSIDIARY"
            ORDER BY txn_month
        ) AS sr_change_pp,

        (
            MAX(sr) OVER (
                PARTITION BY "PRODUCT", "SUBSIDIARY"
                ORDER BY txn_month
                ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            )
            -
            MIN(sr) OVER (
                PARTITION BY "PRODUCT", "SUBSIDIARY"
                ORDER BY txn_month
                ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            )
        ) AS sr_volatility_6m

    FROM monthly_sr
)

SELECT
    txn_month,
    "PRODUCT",
    "SUBSIDIARY",
    total,
    sr AS monthly_sr,
    value_usd,

    ROUND(rolling_3m_sr::numeric, 2) AS rolling_3m_sr,

    ROUND(COALESCE(sr_change_pp,0)::numeric, 2) AS sr_change_pp,

    ROUND(COALESCE(sr_volatility_6m,0)::numeric, 2) AS sr_volatility_6m

FROM enriched
WHERE txn_month >= date_trunc('month', CURRENT_DATE - INTERVAL '5 months')
ORDER BY "PRODUCT", "SUBSIDIARY", txn_month;

-- ## QUERY 7 — Customer Revenue by Segment with Lifetime Value
-- **Business use:** Marketing and CRM prioritisation. Compares revenue per
-- customer across RETAIL / SME / CORPORATE / PREMIUM segments.
SELECT
    c."SEGMENT",

    COUNT(DISTINCT t."CUSTOMER_ID") AS customer_count,

    COUNT(*) AS total_txns,

    ROUND(
        (COUNT(*)::numeric / NULLIF(COUNT(DISTINCT t."CUSTOMER_ID"),0))::numeric,
        1
    ) AS txns_per_customer,

    ROUND(
        SUM(COALESCE(t."TXN_AMOUNT_USD",0))::numeric,
        2
    ) AS total_rev_usd,

    ROUND(
        (SUM(COALESCE(t."TXN_AMOUNT_USD",0))
        / NULLIF(COUNT(DISTINCT t."CUSTOMER_ID"),0))::numeric,
        2
    ) AS rev_per_customer_usd,

    ROUND(
        AVG(COALESCE(t."TXN_AMOUNT_USD",0))::numeric,
        2
    ) AS avg_txn_usd,

    ROUND(
        (
            100.0 * SUM(CASE WHEN t."STATUS" = 'SUCCESS' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*),0)
        )::numeric,
        2
    ) AS success_rate_pct,

    COUNT(DISTINCT t."PRODUCT") AS products_used,

    ROUND(
        (
            SUM(COALESCE(t."FEE_AMOUNT_LOCAL",0))
            / NULLIF(COUNT(DISTINCT t."CUSTOMER_ID"),0)
        )::numeric,
        2
    ) AS avg_fee_per_cust

FROM transactions t
JOIN customers c
    ON t."CUSTOMER_ID" = c."CUSTOMER_ID"

WHERE t."STATUS" = 'SUCCESS'
  AND t."TRANSACTION_DATE"::date >= date_trunc('month', CURRENT_DATE - INTERVAL '3 months')

GROUP BY c."SEGMENT"
ORDER BY rev_per_customer_usd DESC;

-- ## QUERY 8 — Channel Performance Matrix | All Products × All Channels

-- **Business use:** Technology team quarterly review. Which channel × product
-- combinations have the worst success rates and highest processing times?

WITH channel_product AS (
    SELECT
        t."CHANNEL"        AS channel,
        t."CHANNEL_TYPE"   AS channel_type,
        t."PRODUCT"        AS product,

        COUNT(*) AS total_txns,

        SUM(CASE WHEN t."STATUS" = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_txns,

        ROUND(
            (100.0 * SUM(CASE WHEN t."STATUS" = 'SUCCESS' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*),0))::numeric,
            2
        ) AS success_rate,

        ROUND(AVG(COALESCE(t."PROCESSING_TIME_SECS",0))::numeric, 1) AS avg_proc_secs,

        ROUND(
            AVG(
                CASE WHEN t."STATUS" = 'FAILED'
                     THEN t."PROCESSING_TIME_SECS"
                END
            )::numeric,
            1
        ) AS avg_fail_proc_secs,

        ROUND(SUM(COALESCE(t."TXN_AMOUNT_USD",0))::numeric, 2) AS value_usd,

        ROUND(AVG(COALESCE(t."FEE_AMOUNT_LOCAL",0))::numeric, 2) AS avg_fee

    FROM transactions t
    WHERE t."TRANSACTION_DATE"::date >= date_trunc('month', CURRENT_DATE - INTERVAL '3 months')
    GROUP BY t."CHANNEL", t."CHANNEL_TYPE", t."PRODUCT"
),

ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY channel
            ORDER BY success_rate ASC
        ) AS worst_sr_rank,

        RANK() OVER (
            PARTITION BY product
            ORDER BY total_txns DESC
        ) AS vol_rank_in_product

    FROM channel_product
)

SELECT
    channel,
    channel_type,
    product,

    total_txns,
    successful_txns,
    success_rate,
    avg_proc_secs,
    avg_fail_proc_secs,

    ROUND(value_usd::numeric, 0) AS value_usd,
    avg_fee,

    worst_sr_rank,
    vol_rank_in_product

FROM ranked
ORDER BY success_rate ASC, total_txns DESC;

-- ## QUERY 9 — Cohort Retention Analysis (12-Month Grid)

-- **Business use:** Customer Lifecycle team. Measures what % of customers
-- acquired in each cohort month are still transacting N months later.

WITH first_txn AS (
    SELECT
        t."CUSTOMER_ID",

        date_trunc('month', MIN(t."TRANSACTION_DATE"::date))::date AS cohort_month

    FROM transactions t
    WHERE t."STATUS" = 'SUCCESS'
    GROUP BY t."CUSTOMER_ID"
),

activity AS (
    SELECT
        t."CUSTOMER_ID",
        f.cohort_month,

        date_trunc('month', t."TRANSACTION_DATE"::date)::date AS activity_month,

        (
            (EXTRACT(YEAR FROM t."TRANSACTION_DATE"::date) * 12 +
             EXTRACT(MONTH FROM t."TRANSACTION_DATE"::date))
          -
            (EXTRACT(YEAR FROM f.cohort_month) * 12 +
             EXTRACT(MONTH FROM f.cohort_month))
        ) AS month_offset

    FROM transactions t
    JOIN first_txn f
        ON t."CUSTOMER_ID" = f."CUSTOMER_ID"
    WHERE t."STATUS" = 'SUCCESS'
),

cohort_size AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT "CUSTOMER_ID") AS cohort_n
    FROM first_txn
    GROUP BY 1
),

retention AS (
    SELECT
        cohort_month,
        month_offset,
        COUNT(DISTINCT "CUSTOMER_ID") AS retained
    FROM activity
    GROUP BY 1,2
)

SELECT
    r.cohort_month,
    cs.cohort_n,
    r.month_offset,
    r.retained,

    ROUND(
        (100.0 * r.retained / NULLIF(cs.cohort_n,0))::numeric,
        1
    ) AS retention_rate_pct

FROM retention r
JOIN cohort_size cs
    ON r.cohort_month = cs.cohort_month

WHERE r.month_offset BETWEEN 0 AND 6
  AND r.cohort_month >= date_trunc('month', CURRENT_DATE - INTERVAL '8 months')

ORDER BY r.cohort_month, r.month_offset;

-- ## QUERY 9 — Cohort Retention Analysis (12-Month Grid)
-- **Business use:** Customer Lifecycle team. Measures what % of customers
-- acquired in each cohort month are still transacting N months later.
WITH cleaned_txns AS (
    SELECT
        t.*,
        t."TRANSACTION_DATE"::date AS txn_date
    FROM transactions t
    WHERE t."STATUS" = 'SUCCESS'
),

first_txn AS (
    SELECT
        "CUSTOMER_ID",
        date_trunc('month', MIN(txn_date)) AS cohort_month
    FROM cleaned_txns
    GROUP BY "CUSTOMER_ID"
),

activity AS (
    SELECT
        t."CUSTOMER_ID",
        f.cohort_month,
        date_trunc('month', t.txn_date) AS activity_month,

        (
            (EXTRACT(YEAR FROM t.txn_date) * 12 + EXTRACT(MONTH FROM t.txn_date))
          -
            (EXTRACT(YEAR FROM f.cohort_month) * 12 + EXTRACT(MONTH FROM f.cohort_month))
        ) AS month_offset

    FROM cleaned_txns t
    JOIN first_txn f
        ON t."CUSTOMER_ID" = f."CUSTOMER_ID"
),

cohort_size AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT "CUSTOMER_ID") AS cohort_n
    FROM first_txn
    GROUP BY cohort_month
),

retention AS (
    SELECT
        cohort_month,
        month_offset,
        COUNT(DISTINCT "CUSTOMER_ID") AS retained
    FROM activity
    GROUP BY 1,2
)

SELECT
    r.cohort_month,
    cs.cohort_n,
    r.month_offset,
    r.retained,

    ROUND(
        (100.0 * r.retained / NULLIF(cs.cohort_n,0))::numeric,
        1
    ) AS retention_rate_pct

FROM retention r
JOIN cohort_size cs
    ON r.cohort_month = cs.cohort_month

WHERE r.month_offset BETWEEN 0 AND 6
ORDER BY r.cohort_month, r.month_offset;

-- ## QUERY 10 — Subsidiary Benchmarking | Cross-Entity Comparison

-- **Business use:** Group CFO report. Compares all 5 subsidiaries on the same
-- KPIs to identify which entity is leading and which needs support.

WITH sub_summary AS (
    SELECT
        "SUBSIDIARY",
        "COUNTRY",

        COUNT(*) AS total_txns,

        SUM(CASE WHEN "STATUS" = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_txns,

        ROUND(SUM("TXN_AMOUNT_USD")::numeric, 2) AS total_value_usd,

        ROUND(AVG("TXN_AMOUNT_USD")::numeric, 2) AS avg_txn_usd,

        COUNT(DISTINCT "CUSTOMER_ID") AS active_customers,

        COUNT(DISTINCT "PRODUCT") AS products_offered,

        COUNT(DISTINCT "CHANNEL") AS channels_used,

        ROUND(
            100.0 * SUM(CASE WHEN "STATUS" = 'SUCCESS' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0)::numeric,
            2
        ) AS success_rate,

        ROUND(AVG("PROCESSING_TIME_SECS")::numeric, 1) AS avg_proc_secs,

        SUM(
            CASE
                WHEN "PRODUCT" = 'Equity Remit'
                 AND "SOURCE_COUNTRY" <> "DEST_COUNTRY"
                THEN 1 ELSE 0
            END
        ) AS remit_txns

    FROM transactions
    WHERE "TRANSACTION_DATE"::date >= CURRENT_DATE - INTERVAL '3 months'
    GROUP BY "SUBSIDIARY", "COUNTRY"
)

SELECT *
FROM sub_summary;

-- ## QUERY 11 — Fee Revenue Analysis | Product Profitability

-- **Business use:** Finance team — revenue attribution. Which products and
-- subsidiaries generate the most fee income per transaction?

SELECT
    t."PRODUCT",
    t."SUBSIDIARY",
    t."CURRENCY",

    COUNT(*) AS total_txns,

    SUM(CASE WHEN t."STATUS" = 'SUCCESS' THEN 1 ELSE 0 END) AS paid_txns,

    ROUND(SUM(t."FEE_AMOUNT_LOCAL")::numeric, 2) AS total_fees_local,

    ROUND(SUM(t."FEE_AMOUNT_LOCAL" * f."USD_RATE")::numeric, 2) AS total_fees_usd,

    ROUND(AVG(t."FEE_AMOUNT_LOCAL")::numeric, 2) AS avg_fee_local,

    ROUND(
        AVG(
            CASE
                WHEN t."TXN_AMOUNT_LOCAL" > 0
                THEN (t."FEE_AMOUNT_LOCAL" / t."TXN_AMOUNT_LOCAL") * 100
            END
        )::numeric,
        3
    ) AS avg_fee_rate_pct,

    RANK() OVER (
        PARTITION BY t."SUBSIDIARY"
        ORDER BY SUM(t."FEE_AMOUNT_LOCAL") DESC
    ) AS fee_rank

FROM "transactions" t

JOIN "forex_rates" f
    ON t."SUBSIDIARY" = f."SUBSIDIARY_NAME"
   AND DATE_TRUNC('month', t."TRANSACTION_DATE"::date)
       = DATE_TRUNC('month', f."RATE_MONTH"::date)

WHERE t."STATUS" = 'SUCCESS'
  AND DATE_TRUNC('month', t."TRANSACTION_DATE"::date)
      = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')

GROUP BY
    t."PRODUCT",
    t."SUBSIDIARY",
    t."CURRENCY"

ORDER BY total_fees_usd DESC;

-- ## QUERY 12 — Daily Transaction Heatmap | Hourly & Day-of-Week Patterns

-- **Business use:** Capacity planning and system maintenance scheduling.
-- Shows when volume peaks and when it is safe to run batch jobs.

SELECT
    CASE EXTRACT(DOW FROM t."TRANSACTION_DATE"::timestamp)
        WHEN 0 THEN '7-Sun'
        WHEN 1 THEN '1-Mon'
        WHEN 2 THEN '2-Tue'
        WHEN 3 THEN '3-Wed'
        WHEN 4 THEN '4-Thu'
        WHEN 5 THEN '5-Fri'
        WHEN 6 THEN '6-Sat'
    END AS day_of_week,

    EXTRACT(HOUR FROM t."TRANSACTION_DATE"::timestamp) AS hour_of_day,

    COUNT(*) AS total_txns,

    SUM(CASE WHEN t."STATUS" = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_txns,

    ROUND(
        100.0 * SUM(CASE WHEN t."STATUS" = 'SUCCESS' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        1
    ) AS success_rate_pct,

    ROUND(AVG(t."TXN_AMOUNT_USD")::numeric, 2) AS avg_value_usd,

    ROUND(AVG(t."PROCESSING_TIME_SECS")::numeric, 1) AS avg_proc_secs

FROM "transactions" t

WHERE DATE_TRUNC('month', t."TRANSACTION_DATE"::timestamp)
      >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')

GROUP BY 1, 2
ORDER BY day_of_week, hour_of_day;
