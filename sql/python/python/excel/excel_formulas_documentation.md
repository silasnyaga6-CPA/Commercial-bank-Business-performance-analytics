Load `transactions.csv` → Ctrl+T → name table **RawData**.
> Add helper column: `=TEXT([@TRANSACTION_DATE],"YYYY-MM")` → name **TXN_MONTH**
> Exact column names from CSV: `PRODUCT · SUBSIDIARY · STATUS · TXN_AMOUNT_USD
> CHANNEL · CUSTOMER_ID · AGENT_ID · FAILURE_CODE · FAILURE_DESCRIPTION
> FAILURE_CATEGORY · PROCESSING_TIME_SECS · FEE_AMOUNT_LOCAL · SOURCE_COUNTRY · DEST_COUNTRY`

---

### Formula 1: Total Transactions Last Full Month (per product)
```excel
=COUNTIFS(
    RawData[PRODUCT],    [@PRODUCT],
    RawData[SUBSIDIARY], "Equity Bank Kenya",
    RawData[TXN_MONTH],  TEXT(EOMONTH(TODAY(),-1),"YYYY-MM")
)
```

### Formula 2: Success Rate with Zero-Guard
```excel
=IFERROR(
    SUMPRODUCT(
        (RawData[PRODUCT]=[@PRODUCT]) *
        (RawData[SUBSIDIARY]=[@SUBSIDIARY]) *
        (RawData[STATUS]="SUCCESS") *
        (RawData[TXN_MONTH]=TEXT(EOMONTH(TODAY(),-1),"YYYY-MM"))
    )
    /
    COUNTIFS(
        RawData[PRODUCT],    [@PRODUCT],
        RawData[SUBSIDIARY], [@SUBSIDIARY],
        RawData[TXN_MONTH],  TEXT(EOMONTH(TODAY(),-1),"YYYY-MM")
    ),
    0
)
```
Format as **Percentage, 1 decimal**.

### Formula 3: RAG Status (LET + IFS)
```excel
=LET(
    actual,  COUNTIFS(
                 RawData[PRODUCT],    [@PRODUCT],
                 RawData[SUBSIDIARY], [@SUBSIDIARY],
                 RawData[TXN_MONTH],  TEXT(EOMONTH(TODAY(),-1),"YYYY-MM")
             ),
    target,  XLOOKUP(
                 [@PRODUCT]&[@SUBSIDIARY],
                 KPI_Targets[PRODUCT]&KPI_Targets[SUBSIDIARY_NAME],
                 KPI_Targets[TARGET_VOLUME], 0
             ),
    att,     IFERROR(actual/target, 0),
    IFS(att >= 0.95, "🟢 GREEN",
        att >= 0.80, "🟡 AMBER",
        TRUE,        "🔴 RED")
)
```

### Formula 4: Top 5 Failure Codes this Month (spills — Excel 365)
```excel
=LET(
    fails,  FILTER(
                RawData[FAILURE_DESCRIPTION],
                (RawData[STATUS]="FAILED") *
                (RawData[FAILURE_DESCRIPTION]<>"") *
                (RawData[TXN_MONTH]=TEXT(EOMONTH(TODAY(),-1),"YYYY-MM"))
            ),
    codes,  UNIQUE(fails),
    counts, BYROW(codes, LAMBDA(c, COUNTIF(fails,c))),
    sorted, SORT(HSTACK(codes,counts), 2, -1),
    TAKE(sorted, 5)
)
```
Spills a 5×2 table automatically — failure description + count.

### Formula 5: Success Rate Matrix — All Channels × Products (spills)
```excel
=LET(
    channels, UNIQUE(RawData[CHANNEL]),
    products, UNIQUE(RawData[PRODUCT]),
    matrix,   MAKEARRAY(
                  ROWS(channels), ROWS(products),
                  LAMBDA(r,c,
                      IFERROR(
                          SUMPRODUCT(
                              (RawData[CHANNEL]=INDEX(channels,r))*
                              (RawData[PRODUCT]=INDEX(products,c))*
                              (RawData[STATUS]="SUCCESS")
                          )
                          /
                          SUMPRODUCT(
                              (RawData[CHANNEL]=INDEX(channels,r))*
                              (RawData[PRODUCT]=INDEX(products,c))
                          ), 0
                      )
                  )
              ),
    VSTACK(
        HSTACK("Channel\Product", TRANSPOSE(products)),
        HSTACK(channels, TEXT(matrix,"0.0%"))
    )
)
```

### Formula 6: MoM Volume Change with Arrow
```excel
=LET(
    curr, COUNTIFS(RawData[PRODUCT],[@PRODUCT],
                   RawData[TXN_MONTH],TEXT(EOMONTH(TODAY(),-1),"YYYY-MM")),
    prev, COUNTIFS(RawData[PRODUCT],[@PRODUCT],
                   RawData[TXN_MONTH],TEXT(EOMONTH(TODAY(),-2),"YYYY-MM")),
    pct,  IFERROR((curr-prev)/prev,0),
    IFS(pct>=0.1,  "▲▲ "&TEXT(pct,"0.0%"),
        pct>0,     "▲ " &TEXT(pct,"0.0%"),
        pct>=-0.1, "▼ " &TEXT(ABS(pct),"0.0%"),
        TRUE,      "▼▼ "&TEXT(ABS(pct),"0.0%"))
)
```

### Formula 7: Agent Float Utilisation Ratio
```excel
-- Assumes Agents table loaded as AgentData named table
=LET(
    agent,  [@AGENT_ID],
    value,  SUMIFS(
                RawData[TXN_AMOUNT_LOCAL],
                RawData[AGENT_ID],   agent,
                RawData[TXN_MONTH],  TEXT(EOMONTH(TODAY(),-1),"YYYY-MM")
            ),
    limit,  XLOOKUP(agent, AgentData[AGENT_ID], AgentData[FLOAT_LIMIT_KES], 1),
    IFERROR(value/limit, "N/A")
)
```
Format as **Number, 2 decimal places**. Values >1 mean agent turned float
over more than once — flag for float limit increase.

### Formula 8: Corridor Revenue Matrix (dynamic)
```excel
=LET(
    remit,     FILTER(RawData, (RawData[PRODUCT]="Equity Remit") *
                               (RawData[SOURCE_COUNTRY]<>RawData[DEST_COUNTRY])),
    corridors, UNIQUE(CHOOSECOLS(remit,1)&"->"&CHOOSECOLS(remit,2)),
    months,    UNIQUE(CHOOSECOLS(remit,3)),
    matrix,    MAKEARRAY(ROWS(months),ROWS(corridors),
                   LAMBDA(r,c,
                       SUMPRODUCT(
                           (RawData[SOURCE_COUNTRY]&"->"&RawData[DEST_COUNTRY]
                            =INDEX(corridors,c)) *
                           (RawData[TXN_MONTH]=INDEX(months,r)) *
                           RawData[TXN_AMOUNT_USD]
                       )
                   )
               ),
    VSTACK(HSTACK("Month",TRANSPOSE(corridors)), HSTACK(months,matrix))
)
```

### Formula 9: YTD Running Total (SCAN)
```excel
=SCAN(
    0,
    FILTER(RawData[TXN_AMOUNT_USD],
           (RawData[SUBSIDIARY]="Equity Bank Kenya") *
           (LEFT(RawData[TXN_MONTH],4)=TEXT(YEAR(TODAY()),"0000")) *
           (RawData[STATUS]="SUCCESS")),
    LAMBDA(acc,x, acc+x)
)
```

### Formula 10: Rolling 3-Month Average Success Rate
```excel
=AVERAGE(
    OFFSET(
        [@SuccessRate],
        -2, 0,
        MIN(3, ROW()-ROW(KPI_Table[#Headers]))
    )
)
```
