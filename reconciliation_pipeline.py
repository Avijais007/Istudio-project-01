"""
Enterprise Data Architecture & Reconciliation Pipeline
======================================================
Automated end-to-end data pipeline resolving the 4 architectural traps:
1. Scale & Vectorization: High-throughput parsing of 100k rows in < 1 second.
2. Unstructured Log Parsing: Vectorized regex extraction with error isolation.
3. Temporal Reconciliation: Forward-filling missing weekend forex exchange rates.
4. Version Control: SQLite Window Functions to isolate latest Active user records.
"""

import os
import sys
import time
import json
import sqlite3
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

def extract_active_users(db_path: str) -> pd.DataFrame:
    """
    Phase 1: Advanced SQL Extraction
    Uses SQL Window Functions to isolate the most recent status for each user,
    filtering for only 'Active' accounts.
    """
    print("[Phase 1] Extracting active users from database...", flush=True)
    t0 = time.time()
    
    conn = sqlite3.connect(db_path)
    query = """
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
    """
    df_active_users = pd.read_sql_query(query, conn)
    conn.close()
    
    duration = time.time() - t0
    print(f"  -> Extracted {len(df_active_users):,} active users in {duration:.3f}s", flush=True)
    return df_active_users

def extract_server_logs(logs_path: str) -> pd.DataFrame:
    """
    Phase 2: Regex & Unstructured Data Wrangling
    Vectorized extraction of Date, User ID, Product ID, and Euro Value from 100k raw text logs.
    Filters out system error messages without python for loops.
    """
    print("[Phase 2] Parsing raw server logs...", flush=True)
    t0 = time.time()
    
    df_raw = pd.read_csv(logs_path)
    total_raw = len(df_raw)
    
    # 1. Filter out ERROR logs (vectorized)
    df_clean = df_raw[~df_raw.iloc[:, 0].str.contains("ERROR", regex=False)].copy()
    errors_filtered = total_raw - len(df_clean)
    
    # 2. Extract structured fields with vectorized regex
    pattern = r'\[(?P<Date>\d{4}-\d{2}-\d{2})[^\]]*\]\s+INFO:.*?payload=\{"u":"(?P<User_ID>[^"]+)",\s*"item_code":"(?P<Product_ID>[^"]+)",\s*"eur_val":(?P<Euro_Value>[\d.]+)\}'
    extracted_data = df_clean.iloc[:, 0].str.extract(pattern)
    
    # 3. Type casting
    extracted_data['Euro_Value'] = extracted_data['Euro_Value'].astype(float)
    
    duration = time.time() - t0
    print(f"  -> Processed {total_raw:,} raw logs: {errors_filtered:,} errors removed, {len(extracted_data):,} transactions extracted in {duration:.3f}s", flush=True)
    return extracted_data

def reconcile_data(df_logs: pd.DataFrame, fx_path: str, df_users: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 3: Financial Reconciliation & Merging
    Resolves temporal gaps by forward-filling weekend forex rates and performs
    relational joins against active users.
    """
    print("[Phase 3] Reconciling financial transactions...", flush=True)
    t0 = time.time()
    
    # 1. Load and clean exchange rates
    df_fx = pd.read_csv(fx_path)
    df_fx['Date'] = pd.to_datetime(df_fx['Date']).dt.strftime('%Y-%m-%d')
    df_fx = df_fx.sort_values('Date').reset_index(drop=True)
    
    # Forward-fill weekend gaps
    gaps_before = df_fx['EUR_to_USD'].isnull().sum()
    df_fx['EUR_to_USD'] = df_fx['EUR_to_USD'].ffill()
    print(f"  -> Forward-filled {gaps_before} weekend/missing exchange rate dates", flush=True)
    
    # 2. Merge logs with exchange rates on Date
    df_merged = pd.merge(df_logs, df_fx, on='Date', how='inner')
    
    # 3. Merge with active users on User ID
    df_final = pd.merge(df_merged, df_users, left_on='User_ID', right_on='user_id', how='inner')
    
    # 4. Calculate USD Revenue
    df_final['USD_Revenue'] = df_final['Euro_Value'] * df_final['EUR_to_USD']
    
    duration = time.time() - t0
    total_rev = df_final['USD_Revenue'].sum()
    print(f"  -> Reconciled {len(df_final):,} active transactions in {duration:.3f}s", flush=True)
    print(f"  -> Total Audited USD Revenue: ${total_rev:,.2f}", flush=True)
    return df_final

def generate_cfo_dashboard(df_final: pd.DataFrame, output_dir: str):
    """
    Phase 4: Aggregation & Visualization (CFO Dashboard)
    Produces publication-grade executive figures and summary metrics.
    """
    print("[Phase 4] Generating CFO Executive Dashboard & Report...", flush=True)
    t0 = time.time()
    os.makedirs(output_dir, exist_ok=True)
    
    # Prepare monthly data
    df_copy = df_final.copy()
    df_copy['Month'] = pd.to_datetime(df_copy['Date']).dt.to_period('M').astype(str)
    monthly_rev = df_copy.groupby('Month')['USD_Revenue'].sum().reset_index()
    
    # Prepare Top 5 customers
    top5_customers = (
        df_copy.groupby(['user_id', 'name'])['USD_Revenue']
        .sum()
        .reset_index()
        .sort_values('USD_Revenue', ascending=True) # Ascending for horizontal bar plot
        .tail(5)
    )
    
    # Visualization setup
    sns.set_theme(style='whitegrid')
    fig, axes = plt.subplots(1, 2, figsize=(18, 7), dpi=300)
    fig.patch.set_facecolor('#F8F9FA')
    
    # Chart 1: Monthly USD Revenue
    ax1 = axes[0]
    ax1.set_facecolor('#FFFFFF')
    bars = ax1.bar(range(len(monthly_rev)), monthly_rev['USD_Revenue'] / 1e6, color='#1F77B4', width=0.6, edgecolor='#0D47A1', alpha=0.9, label='Monthly Revenue')
    ax1.plot(range(len(monthly_rev)), monthly_rev['USD_Revenue'] / 1e6, color='#FF7F0E', marker='o', linewidth=2.5, markersize=7, label='Trend Line')
    
    ax1.set_title('2023 Total USD Revenue by Month (Active Accounts)', fontsize=15, fontweight='bold', pad=15, color='#1A202C')
    ax1.set_xlabel('Month', fontsize=12, fontweight='bold', labelpad=10)
    ax1.set_ylabel('Revenue ($ Millions USD)', fontsize=12, fontweight='bold', labelpad=10)
    ax1.set_xticks(range(len(monthly_rev)))
    ax1.set_xticklabels(monthly_rev['Month'], rotation=45, ha='right', fontsize=10)
    ax1.yaxis.set_major_formatter(ticker.FormatStrFormatter('$%.1fM'))
    ax1.legend(loc='upper right', frameon=True, facecolor='white', framealpha=0.9)
    
    # Value labels on bars
    for bar in bars:
        h = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width() / 2, h + 0.1, f'${h:.2f}M', ha='center', va='bottom', fontsize=8.5, fontweight='bold', color='#2D3748')
    ax1.set_ylim(0, monthly_rev['USD_Revenue'].max() / 1e6 * 1.18)
    
    # Chart 2: Top 5 Most Valuable Customers
    ax2 = axes[1]
    ax2.set_facecolor('#FFFFFF')
    bar_colors = ['#4A90E2', '#357ABD', '#2868A8', '#1B528F', '#103E73']
    labels = [f"{row['user_id']} ({row['name']})" for _, row in top5_customers.iterrows()]
    bars2 = ax2.barh(labels, top5_customers['USD_Revenue'], color=bar_colors, height=0.55, edgecolor='#0D47A1', alpha=0.95)
    
    ax2.set_title('Top 5 Most Valuable Customers (Total USD Spent)', fontsize=15, fontweight='bold', pad=15, color='#1A202C')
    ax2.set_xlabel('Total Spent ($ USD)', fontsize=12, fontweight='bold', labelpad=10)
    ax2.set_ylabel('Customer ID & Name', fontsize=12, fontweight='bold', labelpad=10)
    ax2.xaxis.set_major_formatter(ticker.StrMethodFormatter('${x:,.0f}'))
    
    for bar in bars2:
        w = bar.get_width()
        ax2.text(w + 300, bar.get_y() + bar.get_height() / 2, f'${w:,.2f}', ha='left', va='center', fontsize=9.5, fontweight='bold', color='#1A202C')
    ax2.set_xlim(0, top5_customers['USD_Revenue'].max() * 1.18)
    
    plt.tight_layout(pad=3.0)
    
    # Save chart
    dashboard_img = os.path.join(output_dir, 'cfo_dashboard.png')
    fig.savefig(dashboard_img, dpi=300, bbox_inches='tight')
    plt.close(fig)
    print(f"  -> Saved CFO Dashboard image to: {dashboard_img}", flush=True)
    
    # Save CSV and JSON summary
    csv_out = os.path.join(output_dir, 'reconciled_data.csv')
    df_final.to_csv(csv_out, index=False)
    print(f"  -> Saved reconciled transaction dataset ({len(df_final):,} rows) to: {csv_out}", flush=True)
    
    summary = {
        'total_audited_usd_revenue': round(float(df_final['USD_Revenue'].sum()), 2),
        'total_active_transactions': int(len(df_final)),
        'unique_active_customers': int(df_final['user_id'].nunique()),
        'unique_products_sold': int(df_final['Product_ID'].nunique()),
        'average_transaction_usd': round(float(df_final['USD_Revenue'].mean()), 2),
        'monthly_revenue_breakdown': {
            row['Month']: round(float(row['USD_Revenue']), 2) for _, row in monthly_rev.iterrows()
        },
        'top_5_customers': [
            {
                'rank': idx + 1,
                'user_id': row['user_id'],
                'name': row['name'],
                'total_usd_spent': round(float(row['USD_Revenue']), 2)
            }
            for idx, (_, row) in enumerate(top5_customers.iloc[::-1].iterrows())
        ]
    }
    
    json_out = os.path.join(output_dir, 'executive_summary.json')
    with open(json_out, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    print(f"  -> Saved executive summary metrics to: {json_out}", flush=True)
    print(f"  -> Completed Phase 4 in {time.time() - t0:.3f}s", flush=True)

def run_pipeline(base_dir: str):
    print("=" * 75, flush=True)
    print("   ENTERPRISE DATA ARCHITECTURE & RECONCILIATION PIPELINE", flush=True)
    print("=" * 75, flush=True)
    t_global_start = time.time()
    
    db_path = os.path.join(base_dir, 'data', 'enterprise_database.db')
    logs_path = os.path.join(base_dir, 'data', 'server_logs.txt')
    fx_path = os.path.join(base_dir, 'data', 'daily_exchange_rates.csv')
    output_dir = os.path.join(base_dir, 'outputs')
    
    df_users = extract_active_users(db_path)
    df_logs = extract_server_logs(logs_path)
    df_final = reconcile_data(df_logs, fx_path, df_users)
    generate_cfo_dashboard(df_final, output_dir)
    
    total_elapsed = time.time() - t_global_start
    print("=" * 75, flush=True)
    print(f" PIPELINE COMPLETED SUCCESSFULLY IN {total_elapsed:.2f} SECONDS!", flush=True)
    print("=" * 75, flush=True)

if __name__ == '__main__':
    project_root = os.path.dirname(os.path.abspath(__file__))
    run_pipeline(project_root)
