# Enterprise Data Architecture & Financial Reconciliation

## Executive Summary
This repository contains the complete production-grade data pipeline and Jupyter Notebook audit for the **Enterprise Data Architecture & Reconciliation** capstone project. 

The pipeline reconciles three disconnected, non-standard systems across a corporate merger scenario:
1. **Human Resources & User Accounts (`enterprise_database.db`):** Historical state change log (47,464 records for 25,000 users) requiring latest active status isolation.
2. **Server Activity Logs (`server_logs.txt`):** 100,000 raw server transaction logs containing semi-structured strings and system errors.
3. **Foreign Exchange Rates (`daily_exchange_rates.csv`):** Daily EUR-to-USD conversion rates with weekend market closure gaps (104 dates missing rates).

---

## The Four Pedagogical Traps & Architectural Solutions

### 1. The Scale & Optimization Trap
- **The Problem:** 100,000 raw log entries. Using naive Python `for` loops or `df.iterrows()` to parse strings and calculate conversions causes timeouts and takes several minutes to hours.
- **The Solution:** Fully vectorized operations using Pandas and C-optimized regex routines.
- **Result:** Complete processing of 100,000 logs in **0.32 seconds**.

### 2. The Unstructured Data Trap
- **The Problem:** Transaction details (Date, User ID, Product ID, Euro Value) are trapped inside JSON-like log strings, interspersed with corrupted `ERROR` messages (`failed_val`).
- **The Solution:** Vectorized boolean mask (`~df.iloc[:, 0].str.contains("ERROR")`) filters out 5,098 error logs. A single vectorized regular expression with named capture groups extracts all four fields simultaneously:
  ```python
  pattern = r'\[(?P<Date>\d{4}-\d{2}-\d{2})[^\]]*\]\s+INFO:.*?payload=\{"u":"(?P<User_ID>[^"]+)",\s*"item_code":"(?P<Product_ID>[^"]+)",\s*"eur_val":(?P<Euro_Value>[\d.]+)\}'
  extracted_data = df_clean.iloc[:, 0].str.extract(pattern)
  ```
- **Result:** 94,902 clean transaction records extracted with **0 null values**.

### 3. The Temporal Reconciliation Trap (Time-Series)
- **The Problem:** Forex exchange rates are unavailable on weekends when financial markets close. An `INNER JOIN` against raw exchange rates would silently drop ~28% of transactions (weekend purchases).
- **The Solution:** Chronological forward-filling (`.ffill()`) rolls Friday's closing market exchange rate across Saturday and Sunday.
- **Result:** Reconciled 100% of weekend transaction volume (104 weekend dates filled) without dropping valid records.

### 4. The Version Control Trap (Advanced SQL)
- **The Problem:** The user database contains historical logs where a single customer may have multiple entries with statuses like `Pending`, `Active`, and `Suspended`. Simple queries return duplicate users or stale states.
- **The Solution:** Advanced SQL **Window Functions** to partition by `user_id` and order chronologically:
  ```sql
  WITH ranked_users AS (
      SELECT 
          user_id, 
          name, 
          status, 
          updated_at,
          ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) as rn
      FROM users
  )
  SELECT user_id, name, status, updated_at
  FROM ranked_users
  WHERE rn = 1 AND status = 'Active';
  ```
- **Result:** Exactly 14,183 currently active accounts isolated out of 25,000 total unique users.

---

## Financial Audit Results

- **Total Audited USD Revenue:** **$148,274,622.91**
- **Total Reconciled Transactions:** **53,899**
- **Unique Active Customers with Purchases:** **13,888**
- **Unique Products Sold:** **900**
- **Average Transaction Value:** **$2,750.97**
- **End-to-End Pipeline Runtime:** **~0.60 seconds** (CLI pipeline), **~3.0 seconds** (including 300 DPI visualization rendering).

### Monthly Revenue Breakdown
| Month | Reconciled USD Revenue |
| :--- | :--- |
| **January 2023** | $12,841,761.38 |
| **February 2023** | $11,436,130.90 |
| **March 2023** | $12,348,931.84 |
| **April 2023** | $12,015,376.19 |
| **May 2023** | $12,333,644.45 |
| **June 2023** | $12,084,355.82 |
| **July 2023** | $12,444,163.58 |
| **August 2023** | $12,812,730.66 |
| **September 2023** | $12,228,952.05 |
| **October 2023** | $12,501,330.36 |
| **November 2023** | $12,588,223.09 |
| **December 2023** | $12,639,022.60 |

### Top 5 Most Valuable Customers
| Rank | User ID | Name | Total USD Spent |
| :---: | :--- | :--- | :---: |
| **1** | `U-06582` | `User_U-06582` | **$40,050.99** |
| **2** | `U-23313` | `User_U-23313` | **$39,555.81** |
| **3** | `U-03805` | `User_U-03805` | **$39,529.95** |
| **4** | `U-20018` | `User_U-20018` | **$38,649.97** |
| **5** | `U-06489` | `User_U-06489` | **$38,629.14** |

---

## Project Structure

```
enterprise-data-reconciliation/
├── data/
│   ├── daily_exchange_rates.csv         # Daily EUR-USD forex exchange rates
│   ├── enterprise_database.db          # SQLite customer database
│   └── server_logs.txt                 # 100,000 raw server transaction logs
├── outputs/
│   ├── cfo_dashboard.png               # High-resolution dual-panel visualization
│   ├── executive_summary.json          # Key financial metrics & top customers
│   └── reconciled_data.csv             # Final reconciled active dataset (53,899 rows)
├── .venv/                              # Isolated Python virtual environment
├── Enterprise_Data_Reconciliation.ipynb # Executed notebook with rich outputs
├── reconciliation_pipeline.py          # Standalone reproducible CLI pipeline
├── requirements.txt                    # Pinned package dependencies
└── README.md                           # This documentation
```

---

## How to Run

### 1. Run the Python Pipeline Script
```bash
# Activate virtual environment
.\.venv\Scripts\activate

# Run pipeline
python reconciliation_pipeline.py
```

### 2. View or Run the Jupyter Notebook
```bash
# Start Jupyter
jupyter notebook Enterprise_Data_Reconciliation.ipynb
```
Or open directly in VS Code / Cursor / Google Colab.
