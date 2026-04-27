
> **Load:** `transactions.csv` (main fact) + `kpi_targets.csv` + `agents.csv`
> + `customers.csv` + `forex_rates.csv` + `failure_codes.csv`
>
> **Exact column names from CSV (use these in DAX):**
> `Transactions[STATUS]` · `Transactions[TXN_AMOUNT_USD]` · `Transactions[PRODUCT]`
> `Transactions[SUBSIDIARY]` · `Transactions[CHANNEL]` · `Transactions[CUSTOMER_ID]`
> `Transactions[AGENT_ID]` · `Transactions[FAILURE_CODE]` · `Transactions[FAILURE_DESCRIPTION]`
> `Transactions[FAILURE_CATEGORY]` · `Transactions[PROCESSING_TIME_SECS]`
> `Transactions[SOURCE_COUNTRY]` · `Transactions[DEST_COUNTRY]`
> `KPI_Targets[PRODUCT]` · `KPI_Targets[SUBSIDIARY_NAME]` · `KPI_Targets[TARGET_MONTH]`
> `KPI_Targets[TARGET_VOLUME]` · `KPI_Targets[TARGET_VALUE_USD]` · `KPI_Targets[TARGET_SUCCESS_RATE]`
> `Agents[AGENT_ID]` · `Agents[AGENT_TIER]` · `Agents[REGION]` · `Agents[FLOAT_LIMIT_KES]`
> `Customers[CUSTOMER_ID]` · `Customers[SEGMENT]` · `Customers[SUBSIDIARY_NAME]`

---

### Base Measures

```dax
Total Transactions =
COUNTROWS(Transactions)

Successful Transactions =
CALCULATE(COUNTROWS(Transactions), Transactions[STATUS] = "SUCCESS")

Failed Transactions =
CALCULATE(COUNTROWS(Transactions), Transactions[STATUS] = "FAILED")

Total Value USD =
SUM(Transactions[TXN_AMOUNT_USD])

Total Fee Revenue Local =
SUM(Transactions[FEE_AMOUNT_LOCAL])

Success Rate % =
DIVIDE([Successful Transactions], [Total Transactions], 0) * 100

Failure Rate % =
DIVIDE([Failed Transactions], [Total Transactions], 0) * 100

Avg Processing Time Mins =
AVERAGEX(Transactions, Transactions[PROCESSING_TIME_SECS] / 60)

Unique Customers =
DISTINCTCOUNT(Transactions[CUSTOMER_ID])
```

---

### Prior Period Comparisons

```dax
Transactions LM =
CALCULATE([Total Transactions], DATEADD(DateTable[Date], -1, MONTH))

Transactions LY =
CALCULATE([Total Transactions], SAMEPERIODLASTYEAR(DateTable[Date]))

Value USD LM =
CALCULATE([Total Value USD], DATEADD(DateTable[Date], -1, MONTH))

Value USD LY =
CALCULATE([Total Value USD], SAMEPERIODLASTYEAR(DateTable[Date]))

Success Rate LM =
CALCULATE([Success Rate %], DATEADD(DateTable[Date], -1, MONTH))

Customers LM =
CALCULATE([Unique Customers], DATEADD(DateTable[Date], -1, MONTH))
```

---

### Growth Measures

```dax
MoM Volume Growth % =
DIVIDE([Total Transactions] - [Transactions LM], [Transactions LM], BLANK()) * 100

YoY Volume Growth % =
DIVIDE([Total Transactions] - [Transactions LY], [Transactions LY], BLANK()) * 100

MoM Value Growth % =
DIVIDE([Total Value USD] - [Value USD LM], [Value USD LM], BLANK()) * 100

MoM Success Rate Change pp =
[Success Rate %] - [Success Rate LM]

MoM Customer Growth % =
DIVIDE([Unique Customers] - [Customers LM], [Customers LM], BLANK()) * 100

MoM Arrow =
VAR g = [MoM Volume Growth %]
RETURN SWITCH(TRUE(),
    ISBLANK(g), "—",
    g >= 10,    "▲▲ " & FORMAT(g,"0.0") & "%",
    g >  0,     "▲ "  & FORMAT(g,"0.0") & "%",
    g > -10,    "▼ "  & FORMAT(ABS(g),"0.0") & "%",
                "▼▼ " & FORMAT(ABS(g),"0.0") & "%"
)
```

---

### KPI Attainment & RAG

```dax
Volume Attainment % =
VAR Target =
    CALCULATE(
        SUM(KPI_Targets[TARGET_VOLUME]),
        ALLEXCEPT(KPI_Targets,
            KPI_Targets[PRODUCT],
            KPI_Targets[SUBSIDIARY_NAME],
            KPI_Targets[TARGET_MONTH])
    )
RETURN DIVIDE([Total Transactions], Target, 0) * 100

Value Attainment % =
VAR Target =
    CALCULATE(
        SUM(KPI_Targets[TARGET_VALUE_USD]),
        ALLEXCEPT(KPI_Targets,
            KPI_Targets[PRODUCT],
            KPI_Targets[SUBSIDIARY_NAME],
            KPI_Targets[TARGET_MONTH])
    )
RETURN DIVIDE([Total Value USD], Target, 0) * 100

Volume RAG =
SWITCH(TRUE(),
    [Volume Attainment %] >= 95, "GREEN",
    [Volume Attainment %] >= 80, "AMBER",
    "RED"
)

Volume RAG Numeric =
SWITCH([Volume RAG], "GREEN", 1, "AMBER", 2, 3)

Success Rate RAG =
VAR TargetSR =
    CALCULATE(
        MAX(KPI_Targets[TARGET_SUCCESS_RATE]),
        ALLEXCEPT(KPI_Targets,
            KPI_Targets[PRODUCT],
            KPI_Targets[SUBSIDIARY_NAME])
    )
RETURN SWITCH(TRUE(),
    [Success Rate %] >= TargetSR,      "GREEN",
    [Success Rate %] >= TargetSR - 5,  "AMBER",
    "RED"
)
```

---

### Rolling Averages & YTD

```dax
Rolling 3M Avg Success Rate =
CALCULATE(
    AVERAGEX(VALUES(DateTable[MonthYear]), [Success Rate %]),
    DATESINPERIOD(DateTable[Date], LASTDATE(DateTable[Date]), -3, MONTH)
)

Rolling 3M Avg Transactions =
CALCULATE(
    AVERAGEX(VALUES(DateTable[MonthYear]), [Total Transactions]),
    DATESINPERIOD(DateTable[Date], LASTDATE(DateTable[Date]), -3, MONTH)
)

SR vs Rolling 3M =
[Success Rate %] - [Rolling 3M Avg Success Rate]

YTD Transactions =
TOTALYTD([Total Transactions], DateTable[Date])

YTD Value USD =
TOTALYTD([Total Value USD], DateTable[Date])

YTD vs Prior Year % =
VAR CY = [YTD Transactions]
VAR PY = CALCULATE([YTD Transactions], SAMEPERIODLASTYEAR(DateTable[Date]))
RETURN DIVIDE(CY - PY, PY, BLANK()) * 100
```

---

### Agent Measures

```dax
Active Agents =
CALCULATE(
    DISTINCTCOUNT(Transactions[AGENT_ID]),
    Transactions[STATUS]  = "SUCCESS",
    NOT ISBLANK(Transactions[AGENT_ID]),
    Transactions[AGENT_ID] <> "",
    Transactions[PRODUCT]  = "Agency Banking"
)

Txns per Active Agent =
DIVIDE([Successful Transactions], [Active Agents], 0)

Float Utilisation Ratio =
DIVIDE(
    SUM(Transactions[TXN_AMOUNT_LOCAL]),
    CALCULATE(SUM(Agents[FLOAT_LIMIT_KES]),
              ALLEXCEPT(Agents, Agents[AGENT_ID])),
    0
)
```

---

### Remittance & Customer Measures

```dax
Cross-Border Volume USD =
CALCULATE(
    [Total Value USD],
    Transactions[PRODUCT] = "Equity Remit",
    Transactions[SOURCE_COUNTRY] <> Transactions[DEST_COUNTRY]
)

Corridor =
Transactions[SOURCE_COUNTRY] & "->" & Transactions[DEST_COUNTRY]

New Customers This Month =
VAR ThisMonthCusts =
    CALCULATETABLE(VALUES(Transactions[CUSTOMER_ID]), DATESMTD(DateTable[Date]))
VAR AllPriorCusts =
    CALCULATETABLE(VALUES(Transactions[CUSTOMER_ID]),
        FILTER(ALL(DateTable), DateTable[Date] < MIN(DateTable[Date])))
RETURN COUNTROWS(EXCEPT(ThisMonthCusts, AllPriorCusts))

Revenue per Customer USD =
DIVIDE([Total Value USD], [Unique Customers], 0)

Product Rank by Volume =
RANKX(ALLSELECTED(Transactions[PRODUCT]),
      [Total Transactions],, DESC, DENSE)

Subsidiary Rank by Value =
RANKX(ALLSELECTED(Transactions[SUBSIDIARY]),
      [Total Value USD],, DESC, DENSE)

Report Title =
"Equity Bank Payments — " & FORMAT(EOMONTH(TODAY(),-1),"MMMM YYYY")
```

---

## Power BI Dashboard Build Order (5 Pages)

```
Page 1 — Executive Summary
  KPI Cards:     Total Transactions | Success Rate % | Total Value USD
                 Unique Customers | MoM Arrow | YoY Growth %
  Line chart:    DateTable[MonthYear] × Total Transactions, Legend=PRODUCT
  Clustered bar: SUBSIDIARY × Total Value USD (coloured by Subsidiary Rank)
  Table:         PRODUCT | Total Txns | Success Rate % | MoM Arrow | Vol RAG
  Slicers:       SUBSIDIARY · PRODUCT · DateTable[Date] between

Page 2 — Product & Channel Performance
  Matrix:        Rows=PRODUCT, Cols=MonthYear, Values=Total Txns + Success Rate %
  Combo chart:   Columns=Total Txns, Line=Success Rate %, X=MonthYear
  Donut:         CHANNEL × Total Transactions (showing channel mix %)
  Stacked bar:   PRODUCT × STATUS (success vs failed split)
  Scatter:       X=Total Txns, Y=Success Rate %, Size=Value USD, Detail=PRODUCT

Page 3 — Failure Analysis & Alerts
  Cards:         Failure Rate % | Total Failed Value USD
  Bar chart:     FAILURE_DESCRIPTION × Failed Transactions (filter STATUS=FAILED)
  Treemap:       FAILURE_CATEGORY × Failed Value USD
  Line chart:    MonthYear × Failure Rate %, Legend=CHANNEL
  Table:         CHANNEL | PRODUCT | FAILURE_DESCRIPTION | this_week_count | trend
  Slicer:        FAILURE_CATEGORY (multi-select)

Page 4 — Remittance Corridors
  Map visual:    DEST_COUNTRY location, Size=Cross-Border Volume USD
  Bar chart:     Corridor × Total Value USD (filter cross-border Equity Remit)
  Line chart:    MonthYear × Total Transactions, Legend=Corridor
  Table:         Corridor | Txns | SR% | Value USD | Avg Fee Rate | Avg Proc Mins
  Card:          Cross-Border Volume USD | Unique Senders

Page 5 — Agent Scorecard
  Cards:         Active Agents | Txns per Active Agent | Total Commission KES
  Table:         AGENT_ID | AGENT_TIER | REGION | Total Txns | SR% | Float Util | Band | Risk
  Bar chart:     REGION × Total Transactions (Agency Banking filter)
  Scatter:       X=Total Txns, Y=Success Rate %, Size=Float Util, Detail=AGENT_ID
  Map:           COUNTY location, Size=Total Value KES
  Slicer:        REGION · AGENT_TIER · float_risk band
```

---

## CSV → DAX Field Reference (complete mapping)

| CSV Column               | DAX Reference                           | Used in measure                    |
|--------------------------|-----------------------------------------|------------------------------------|
| `STATUS`                 | `Transactions[STATUS]`                  | Success/Failure/SR%                |
| `TXN_AMOUNT_USD`         | `Transactions[TXN_AMOUNT_USD]`          | Total Value USD, Revenue per Cust  |
| `TXN_AMOUNT_LOCAL`       | `Transactions[TXN_AMOUNT_LOCAL]`        | Float Utilisation                  |
| `FEE_AMOUNT_LOCAL`       | `Transactions[FEE_AMOUNT_LOCAL]`        | Fee Revenue                        |
| `PRODUCT`                | `Transactions[PRODUCT]`                 | All product filters & slicers      |
| `PRODUCT_CATEGORY`       | `Transactions[PRODUCT_CATEGORY]`        | Category grouping                  |
| `SUBSIDIARY`             | `Transactions[SUBSIDIARY]`              | Subsidiary slicers & RAG           |
| `CHANNEL`                | `Transactions[CHANNEL]`                 | Channel breakdown                  |
| `CHANNEL_TYPE`           | `Transactions[CHANNEL_TYPE]`            | Channel type grouping              |
| `CUSTOMER_ID`            | `Transactions[CUSTOMER_ID]`             | Unique Customers, New Customers    |
| `AGENT_ID`               | `Transactions[AGENT_ID]`                | Active Agents, Float Util          |
| `FAILURE_CODE`           | `Transactions[FAILURE_CODE]`            | Failure analysis                   |
| `FAILURE_DESCRIPTION`    | `Transactions[FAILURE_DESCRIPTION]`     | Failure detail                     |
| `FAILURE_CATEGORY`       | `Transactions[FAILURE_CATEGORY]`        | Failure treemap                    |
| `PROCESSING_TIME_SECS`   | `Transactions[PROCESSING_TIME_SECS]`    | Avg Processing Time Mins           |
| `SOURCE_COUNTRY`         | `Transactions[SOURCE_COUNTRY]`          | Remittance corridors               |
| `DEST_COUNTRY`           | `Transactions[DEST_COUNTRY]`            | Remittance corridors, Map          |
| `KPI_Targets[PRODUCT]`           | matches `Transactions[PRODUCT]`  | Volume/Value Attainment            |
| `KPI_Targets[SUBSIDIARY_NAME]`   | matches `Transactions[SUBSIDIARY]`| RAG Status                        |
| `KPI_Targets[TARGET_VOLUME]`     | —                                | Volume Attainment %                |
| `KPI_Targets[TARGET_VALUE_USD]`  | —                                | Value Attainment %                 |
| `KPI_Targets[TARGET_SUCCESS_RATE]`| —                               | Success Rate RAG                   |
| `Agents[AGENT_ID]`               | matches `Transactions[AGENT_ID]` | Agent measures                     |
| `Agents[FLOAT_LIMIT_KES]`        | —                                | Float Utilisation Ratio            |
| `Agents[REGION]`                 | —                                | Agent map, bar chart               |
| `Customers[CUSTOMER_ID]`         | matches `Transactions[CUSTOMER_ID]`| Segment analysis                 |
| `Customers[SEGMENT]`             | —                                | Revenue per Segment                |

---
