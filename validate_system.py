"""
Complete System Validation
Checks: Data integrity, Metadata coverage, Integration
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

DB_PATH = 'nse_data_production/nse_data.db'


def validate_all():
    conn = sqlite3.connect(DB_PATH)
    results = {}

    # ========== DATA VALIDATION ==========
    print("\n1️⃣ DATA INTEGRITY")
    print("=" * 50)

    # Check duplicates
    dups = pd.read_sql("""
        SELECT symbol, date, COUNT(*) as cnt
        FROM equity_data
        GROUP BY symbol, date
        HAVING cnt > 1
    """, conn)

    results['duplicates'] = len(dups)
    print(f"   Duplicates: {len(dups)} {'✅' if len(dups) == 0 else '❌'}")

    # Check NULL values
    nulls = pd.read_sql("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) as null_close,
            SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as null_volume
        FROM equity_data
    """, conn).iloc[0]

    results['null_close'] = nulls['null_close']
    print(f"   NULL close: {nulls['null_close']} {'✅' if nulls['null_close'] == 0 else '❌'}")

    # Check negative prices
    negatives = pd.read_sql("""
        SELECT COUNT(*) as cnt
        FROM equity_data
        WHERE close < 0
    """, conn).iloc[0]['cnt']

    results['negative_prices'] = negatives
    print(f"   Negative prices: {negatives} {'✅' if negatives == 0 else '❌'}")

    # ========== METADATA VALIDATION ==========
    print("\n2️⃣ METADATA COVERAGE")
    print("=" * 50)

    total_symbols = pd.read_sql("""
        SELECT COUNT(DISTINCT symbol) as cnt 
        FROM equity_data
    """, conn).iloc[0]['cnt']

    metadata_symbols = pd.read_sql("""
        SELECT COUNT(*) as cnt 
        FROM symbol_metadata
    """, conn).iloc[0]['cnt']

    coverage = (metadata_symbols / total_symbols * 100) if total_symbols > 0 else 0
    results['metadata_coverage'] = coverage

    print(f"   Total symbols: {total_symbols:,}")
    print(f"   With metadata: {metadata_symbols:,}")
    print(f"   Coverage: {coverage:.1f}% {'✅' if coverage > 80 else '⚠️' if coverage > 50 else '❌'}")

    # Check for "Unknown" sectors
    unknown_sectors = pd.read_sql("""
        SELECT COUNT(*) as cnt
        FROM symbol_metadata
        WHERE sector = 'Unknown' OR sector IS NULL
    """, conn).iloc[0]['cnt']

    print(f"   Unknown sectors: {unknown_sectors} {'✅' if unknown_sectors < metadata_symbols * 0.1 else '⚠️'}")

    # ========== INTEGRATION VALIDATION ==========
    print("\n3️⃣ DATA-METADATA INTEGRATION")
    print("=" * 50)

    # Check if symbols in data have metadata
    orphan_symbols = pd.read_sql("""
        SELECT COUNT(DISTINCT e.symbol) as cnt
        FROM equity_data e
        LEFT JOIN symbol_metadata m ON e.symbol = m.symbol
        WHERE m.symbol IS NULL
    """, conn).iloc[0]['cnt']

    results['orphan_symbols'] = orphan_symbols
    print(f"   Symbols without metadata: {orphan_symbols} {'✅' if orphan_symbols < total_symbols * 0.2 else '⚠️'}")

    # Test query with join
    try:
        test_query = pd.read_sql("""
            SELECT e.symbol, e.date, e.close, m.sector, m.industry
            FROM equity_data e
            LEFT JOIN symbol_metadata m USING (symbol)
            LIMIT 10
        """, conn)

        has_sector = test_query['sector'].notna().sum() > 0
        print(f"   JOIN working: {'✅' if has_sector else '❌'}")
    except Exception as e:
        print(f"   JOIN working: ❌ ({e})")

    # ========== DATE VALIDATION ==========
    print("\n4️⃣ DATE CONTINUITY")
    print("=" * 50)

    dates = pd.read_sql("""
        SELECT DISTINCT date 
        FROM equity_data 
        ORDER BY date
    """, conn)

    if len(dates) > 0:
        dates['date'] = pd.to_datetime(dates['date'])
        date_range = pd.date_range(dates['date'].min(), dates['date'].max(), freq='D')
        weekdays = date_range[date_range.weekday < 5]

        missing = len(set(weekdays.date) - set(dates['date'].dt.date))
        results['missing_dates'] = missing

        print(f"   Date range: {dates['date'].min().date()} to {dates['date'].max().date()}")
        print(f"   Missing weekdays: {missing} {'✅' if missing < 20 else '⚠️'}")

    conn.close()

    # ========== OVERALL SCORE ==========
    print("\n" + "=" * 50)
    print("OVERALL HEALTH SCORE")
    print("=" * 50)

    score = 100
    if results.get('duplicates', 0) > 0: score -= 20
    if results.get('null_close', 0) > 0: score -= 15
    if results.get('negative_prices', 0) > 0: score -= 15
    if results.get('metadata_coverage', 0) < 80: score -= 20
    if results.get('orphan_symbols', 0) > total_symbols * 0.2: score -= 15
    if results.get('missing_dates', 0) > 50: score -= 15

    print(f"\n   Score: {score}/100 ", end="")
    if score >= 90:
        print("✅ EXCELLENT")
    elif score >= 70:
        print("⚠️  GOOD")
    elif score >= 50:
        print("⚠️  NEEDS IMPROVEMENT")
    else:
        print("❌ CRITICAL ISSUES")

    print("\n" + "=" * 50 + "\n")

    return results


if __name__ == '__main__':
    validate_all()