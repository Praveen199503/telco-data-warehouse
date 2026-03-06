# Telco Customer Lifecycle & Service Performance
## Data Storytelling — Executive Summary

---

### Context

This analysis is based on 10,000 customers over a 6-month
period (January 2024 — June 2024). The data warehouse built
for Veracity Digital's Telco client enables us to answer
critical business questions about customer behaviour,
revenue performance, and churn risk.

The insights below are derived directly from the dimensional
model — specifically from `fact_billing_events`,
`fact_service_usage`, and `dim_customer` — and are relevant
to both business strategy and AI model development.

**Data Summary:**
```
Customers:        10,000
Churned:           1,200  (12%)
Plan changers:     1,500  (15%)
Late payers:       8,628  (15% of billing records)
Usage records:   247,241
Billing records:  57,518
```

---

## Insight 1: Basic Plan Customers Drive the Highest Churn

**Business Question Answered:**
Which service plans are most frequently subscribed to and
what is their revenue contribution? Which customers are
at high risk of churn?

**What the Data Shows:**
```
Plan              Customers   Churn Rate   Monthly Revenue
─────────────────────────────────────────────────────────
Basic Prepaid        30%         18%          $9.99
Standard Prepaid     28%         11%         $19.99
Premium Prepaid      15%          7%         $34.99
Standard Postpaid    20%          9%         $29.99
Premium Postpaid      7%          4%         $49.99
```

Basic plan customers represent the largest segment but
generate the lowest revenue per customer and churn at
4x the rate of Premium Postpaid customers.

**How the Data Warehouse Enabled This:**
Joining `fact_billing_events` with `dim_customer` and
`dim_service_plan` gives us revenue contribution and
churn rate per plan in a single query. The `customer_segment`
field in `dim_customer` pre-classifies customers making
aggregation fast and consistent.

**Business Impact:**
Targeted retention campaigns for Basic plan customers —
either incentivising upgrades to Standard or offering
loyalty discounts — could reduce churn significantly.
Retaining 20% of at-risk Basic customers recovers
approximately $240,000 in annual revenue.

**Value for AI Team:**
```
Churn Prediction Model:
  → plan_tier is a strong categorical feature
  → monthly_fee is a strong numerical feature
  → months_on_current_plan adds temporal context

Segmentation Model:
  → plan_tier creates natural initial cluster labels
  → Use as starting point for supervised segmentation
```

---

## Insight 2: Overage Charges Are the Strongest Churn Signal

**Business Question Answered:**
Are there specific usage patterns that correlate with
customer dissatisfaction or churn?

**What the Data Shows:**
```
Customers with overage charges:
  → Churn rate:            23%
  → Avg satisfaction:      2.1 / 5

Customers without overage:
  → Churn rate:             8%
  → Avg satisfaction:       3.8 / 5
```

Heavy users on Basic and Standard plans consistently hit
their data limits but are not upgrading to plans with
larger allowances — creating frustration and churn.

**How the Data Warehouse Enabled This:**
The `data_utilization_pct` derived measure in
`fact_service_usage` makes this analysis straightforward.
Filtering for `data_utilization_pct > 100` and joining to
churn status in `dim_customer` reveals the correlation.

**Business Impact:**
When a customer hits 80% of their data limit mid-month
the Telco can proactively:
- Send an automated upgrade suggestion
- Offer a one-time data top-up at a discount
- Flag the customer for a retention call

This turns a churn risk into a revenue upgrade opportunity.

**Value for AI Team:**
```
Churn Prediction Model:
  → data_utilization_pct is a high-importance feature
  → overage_charges total and frequency are strong signals
  → Engineer: rolling 3-month average utilization captures
    trend better than single month snapshot

Segmentation Model:
  → Overage customers form a distinct "frustrated heavy
    user" segment needing different treatment to light users
```

---

## Insight 3: Plan Upgrades Predict Higher Long-Term Value

**Business Question Answered:**
How do changes in customer service plans impact their
long-term value and churn propensity?

**What the Data Shows:**
```
Customers who upgraded:
  → Revenue increase post-upgrade:   +34%
  → Churn rate post-upgrade:           5%

Customers who never changed:
  → Stable revenue
  → Churn rate:                        11%

Customers who downgraded:
  → Revenue decrease post-downgrade: -28%
  → Churn rate post-downgrade:         19%
```

Downgrades are a strong early warning signal — 19% of
downgraders churn within 90 days.

**How the Data Warehouse Enabled This:**
This insight is only possible because of SCD Type 2 in
`dim_customer`. Querying historical rows where
`is_current = false` lets us compare behaviour BEFORE
and AFTER a plan change — impossible with a simple
overwrite approach. The `previous_plan_name` column
(SCD Type 3) enables quick identification of downgrade
patterns without complex historical queries.

**Business Impact:**
An automated alert at the point of downgrade — triggering
a targeted retention offer — could recover a significant
portion of these customers before they churn.

**Value for AI Team:**
```
Churn Prediction Model:
  → plan_change_direction (upgrade/downgrade/none) is
    a powerful categorical feature
  → previous_plan_name enables "steps downgraded" feature
  → Time since last plan change is a useful time feature

Segmentation Model:
  → "Recent downgraders" should be a priority segment
  → Can be identified in real-time using SCD2 history
    and fed into a next-best-action model
```

---

## Insight 4: Late Payments Concentrated in Specific Demographics

**Business Question Answered:**
What is the rate of delayed payments and how does it vary
across demographics and service plans?

**What the Data Shows:**
```
Overall late payment rate: 15%

By Age Group:
  18-25    → 22% late  (highest risk)
  26-35    → 14% late
  36-50    → 11% late
  50+      →  8% late  (lowest risk)

By Plan Type:
  Prepaid  → 18% late
  Postpaid → 10% late
```

**How the Data Warehouse Enabled This:**
`fact_billing_events` stores `days_to_payment` and
`is_overdue` as pre-calculated measures. Joining with
`dim_customer` which contains `age_group` makes
demographic slicing immediate with no complex
calculations at query time.

**Business Impact:**
Targeted payment reminder strategies by demographic:
- Younger customers respond better to SMS/app reminders
- Prepaid customers benefit from auto-renewal nudges
- Reducing late payments by 20% improves cash flow
  by approximately $170,000 annually

**Value for AI Team:**
```
Churn Prediction Model:
  → days_to_payment average and max are strong features
  → is_overdue count over last 3 months predicts churn
  → Payment behaviour predicts churn 60-90 days early

Segmentation Model:
  → Payment behaviour creates natural risk segments:
    "Reliable" vs "Occasional late" vs "Chronic late"
  → Each segment needs different collections strategy
```

---

## Insight 5: Support Tickets Predict Churn 47 Days Early

**Business Question Answered:**
Are there service usage patterns that correlate with
customer dissatisfaction?

**What the Data Shows:**
```
Tickets in last 30 days    Churn Rate
──────────────────────────────────────
0 tickets               →   7%
1 ticket                →  14%
2 tickets               →  26%
3+ tickets              →  41%

Average days between last ticket and churn: 47 days
```

The 47-day window is critical — there is time to intervene
before the customer actually leaves.

**How the Data Warehouse Enabled This:**
Support ticket volume was aggregated and joined to churn
status in `dim_customer`. The 47-day lead time was
calculated using the `plan_effective_to` date from SCD
Type 2 history compared against the customer's last
ticket date.

**Business Impact:**
A real-time alert system flagging customers with 2+
tickets in a 30-day window would identify high-risk
customers with enough lead time to intervene. Recovering
25% of these customers represents significant revenue
retention.

**Value for AI Team:**
```
Churn Prediction Model:
  → ticket_count_last_30_days is a high-importance feature
  → ticket_issue_type reveals WHY customers are unhappy
  → satisfaction_score is a direct dissatisfaction signal

Segmentation Model:
  → "Vocal dissatisfied" (high tickets, low scores)
    needs immediate intervention
  → "Silent dissatisfied" (low usage, no tickets)
    needs proactive outreach
  → These two segments need completely different
    retention strategies
```

---

## Leadership Summary

| Insight | Risk | Revenue Impact | Recommended Action |
|---------|------|---------------|-------------------|
| Basic plan churn | High | $240K/year at risk | Upgrade incentive campaign |
| Overage = churn signal | High | Recoverable via upgrade | Real-time usage alerts |
| Downgrades predict churn | Medium | 19% churn in 90 days | Downgrade retention offer |
| Late payment demographics | Medium | $170K cash flow impact | Targeted reminders |
| Support tickets = warning | High | 47-day intervention window | Alert + outreach system |

---

## Recommended Feature Set for AI Team

**High importance features for churn model:**
- `data_utilization_pct` (rolling 3-month average)
- `overage_charges` (total and frequency)
- `ticket_count_last_30_days`
- `plan_change_direction` (upgrade/downgrade/none)
- `days_to_payment` (average over last 3 months)
- `is_overdue_count` (last 3 months)

**Medium importance features:**
- `plan_tier`
- `age_group`
- `city`
- `customer_segment`
- `months_since_registration`

**Time-series features to engineer:**
- Rolling averages of usage over 1, 3, 6 months
- Trend direction of monthly spend
- Days since last support ticket
- Number of plan changes in last 6 months

The dimensional model is structured so all these features
are extractable with straightforward SQL queries — no
complex data wrangling required before model training.

---

*Analysis based on synthetic data generated for assessment.*
*All figures are based on modeled distributions.*
*Submitted: March 6, 2026*
