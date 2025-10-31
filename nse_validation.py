"""
NSE Data Validation Framework
Comprehensive validation for equity, derivatives, and metadata.
"""

import pandas as pd
import logging
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime, timedelta

logger = logging.getLogger('nse_validation')


@dataclass
class ValidationResult:
    """Result of validation check"""
    valid: bool
    message: str
    warnings: List[str] = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class DataValidator:
    """Centralized validation for all NSE data types"""

    # Required columns for each data type
    REQUIRED_COLUMNS = {
        'equity': ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume'],
        'derivatives': ['symbol', 'instrument', 'date', 'expiry_date',
                        'strike_price', 'settle_price', 'open_interest'],
        'metadata': ['symbol', 'company_name', 'sector', 'industry']
    }

    def __init__(self, config=None):
        """Initialize validator with config thresholds"""
        from nse_data_downloader2 import Config
        self.config = config or Config

    def validate_schema(self, df: pd.DataFrame, data_type: str) -> ValidationResult:
        """Validate dataframe has required columns"""

        if data_type not in self.REQUIRED_COLUMNS:
            return ValidationResult(False, f"Unknown data type: {data_type}")

        required = set(self.REQUIRED_COLUMNS[data_type])
        actual = set(df.columns)
        missing = required - actual

        if missing:
            return ValidationResult(
                False,
                f"Missing required columns: {sorted(missing)}"
            )

        return ValidationResult(True, "Schema valid")

    def validate_equity_data(self, df: pd.DataFrame, date: datetime) -> ValidationResult:
        """Validate equity data with business rules"""
        warnings = []

        # 1. Schema check
        schema_result = self.validate_schema(df, 'equity')
        if not schema_result.valid:
            return schema_result

        # 2. Check for negative prices
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if (df[col] < 0).any():
                return ValidationResult(False, f"Negative values in {col}")

        # 3. OHLC relationship validation
        invalid_ohlc = (
                (df['high'] < df['low']) |
                (df['high'] < df['open']) |
                (df['high'] < df['close']) |
                (df['low'] > df['open']) |
                (df['low'] > df['close'])
        )

        if invalid_ohlc.any():
            count = invalid_ohlc.sum()
            return ValidationResult(False, f"{count} records with invalid OHLC relationships")

        # 4. Price range validation
        for col in price_cols:
            out_of_range = (
                    (df[col] < self.config.PRICE_MIN) |
                    (df[col] > self.config.PRICE_MAX)
            )
            if out_of_range.any():
                warnings.append(f"{out_of_range.sum()} {col} values outside expected range")

        # 5. Volume validation
        if (df['volume'] < 0).any():
            return ValidationResult(False, "Negative volume values")

        if (df['volume'] > self.config.VOLUME_MAX).any():
            warnings.append(f"Some volume values exceed max ({self.config.VOLUME_MAX})")

        # 6. Date validation
        if 'date' in df.columns:
            unique_dates = df['date'].nunique()
            if unique_dates > 1:
                warnings.append(f"Multiple dates in data ({unique_dates})")

        # 7. Null percentage check
        null_pct = (df[price_cols].isnull().sum().sum() / (len(df) * len(price_cols))) * 100
        if null_pct > self.config.MAX_NULL_PERCENT:
            return ValidationResult(False, f"Null percentage too high: {null_pct:.1f}%")

        # 8. Record count check
        if len(df) < self.config.MIN_RECORDS_PER_DAY:
            warnings.append(f"Low record count: {len(df)} < {self.config.MIN_RECORDS_PER_DAY}")

        return ValidationResult(True, "Equity data valid", warnings)

    def validate_derivatives_data(self, df: pd.DataFrame, date: datetime) -> ValidationResult:
        """Validate derivatives data"""
        warnings = []

        # Schema check
        schema_result = self.validate_schema(df, 'derivatives')
        if not schema_result.valid:
            return schema_result

        # Negative values check
        numeric_cols = ['strike_price', 'settle_price', 'open_interest']
        for col in numeric_cols:
            if (df[col] < 0).any():
                return ValidationResult(False, f"Negative values in {col}")

        # Option type validation
        if 'option_type' in df.columns:
            valid_types = {'CE', 'PE', 'XX', None, ''}
            invalid = ~df['option_type'].isin(valid_types)
            if invalid.any():
                unique_invalid = df.loc[invalid, 'option_type'].unique()
                warnings.append(f"Invalid option types: {list(unique_invalid)}")

        # Expiry date validation
        if 'expiry_date' in df.columns:
            try:
                expiry_dates = pd.to_datetime(df['expiry_date'])
                if (expiry_dates < pd.Timestamp(date)).any():
                    warnings.append("Some contracts with expiry date before trade date")
            except:
                warnings.append("Could not parse expiry dates")

        return ValidationResult(True, "Derivatives data valid", warnings)

    def validate_metadata(self, df: pd.DataFrame) -> ValidationResult:
        """Validate symbol metadata"""

        warnings = []

        # Schema check
        schema_result = self.validate_schema(df, 'metadata')
        if not schema_result.valid:
            return schema_result

        # Check for "Unknown" values
        if 'sector' in df.columns:
            unknown_sector = (df['sector'] == 'Unknown') | (df['sector'].isnull())
            if unknown_sector.any():
                warnings.append(f"{unknown_sector.sum()} symbols with Unknown sector")

        if 'industry' in df.columns:
            unknown_industry = (df['industry'] == 'Unknown') | (df['industry'].isnull())
            if unknown_industry.any():
                warnings.append(f"{unknown_industry.sum()} symbols with Unknown industry")

        # Market cap validation
        if 'market_cap' in df.columns:
            if (df['market_cap'] < 0).any():
                return ValidationResult(False, "Negative market cap values")

            null_market_cap = df['market_cap'].isnull().sum()
            if null_market_cap > 0:
                warnings.append(f"{null_market_cap} symbols without market cap")

        return ValidationResult(True, "Metadata valid", warnings)

    def validate_integration(self, db_connection) -> ValidationResult:
        """Validate integration between tables"""
        warnings = []

        try:
            cursor = db_connection.cursor()

            # Check orphan symbols (in equity but not in metadata)
            cursor.execute("""
                SELECT COUNT(DISTINCT e.symbol)
                FROM equity_data e
                LEFT JOIN symbol_metadata m USING (symbol)
                WHERE m.symbol IS NULL
            """)
            orphan_count = cursor.fetchone()[0]

            if orphan_count > 0:
                warnings.append(f"{orphan_count} symbols without metadata")

            # Check symbols with no recent data
            # Calculate cutoff in Python
            cutoff_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            cursor.execute("""
                SELECT COUNT(DISTINCT symbol)
                FROM symbol_metadata 
                WHERE symbol NOT IN (
                    SELECT DISTINCT symbol 
                    FROM equity_data 
                    WHERE date >= ?
                )
            """, (cutoff_date,))
            stale_count = cursor.fetchone()[0]

            if stale_count > 0:
                warnings.append(f"{stale_count} symbols with no data in last 30 days")

            return ValidationResult(True, "Integration valid", warnings)
        except Exception as e:
            return ValidationResult(False, f"Integration validation failed: {e}")


def validate_full_system(db_path: str):
    """Run complete system validation"""
    import sqlite3

    print("\n" + "=" * 80)
    print("NSE DATA SYSTEM VALIDATION")
    print("=" * 80)

    validator = DataValidator()
    conn = sqlite3.connect(db_path)

    # 1. Sample equity data validation
    print("\n1□ EQUITY DATA VALIDATION")
    try:
        # Get max date first
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date) FROM equity_data")
        max_date = cursor.fetchone()[0]

        if max_date:
            df_equity = pd.read_sql("""
                    SELECT * FROM equity_data
                    WHERE date = ?
                    LIMIT 1000
                """, conn, params=(max_date,))
        else:
            df_equity = pd.DataFrame()

        if not df_equity.empty:
            result = validator.validate_equity_data(df_equity, datetime.now())
            print(f"\tStatus: {'✅' if result.valid else '❌'} {result.message}")
            for warning in result.warnings:
                print(f"\t⚠\t{warning}")
        else:
            print("\t⚠\tNo equity data found")
    except Exception as e:
        print(f"\t❌ Error: {e}")

    # 2. Sample derivatives validation
    print("\n2□ DERIVATIVES DATA VALIDATION")
    try:
        df_deriv = pd.read_sql("""
            SELECT * FROM derivatives_data
            WHERE date = (SELECT MAX(date) FROM derivatives_data)
            LIMIT 1000
        """, conn)

        if not df_deriv.empty:
            result = validator.validate_derivatives_data(df_deriv, datetime.now())
            print(f"\tStatus: {'✅' if result.valid else '❌'} {result.message}")
            for warning in result.warnings:
                print(f"\t⚠\t{warning}")
        else:
            print("\t⚠\tNo derivatives data found")
    except Exception as e:
        print(f"\t❌ Error: {e}")

    # 3. Metadata validation
    print("\n3□ METADATA VALIDATION")
    try:
        df_metadata = pd.read_sql("SELECT * FROM symbol_metadata", conn)

        if not df_metadata.empty:
            result = validator.validate_metadata(df_metadata)
            print(f"\tStatus: {'✅' if result.valid else '❌'} {result.message}")
            for warning in result.warnings:
                print(f"\t⚠\t{warning}")
        else:
            print("\t⚠\tNo metadata found")
    except Exception as e:
        print(f"\t❌ Error: {e}")

    # 4. Integration validation
    print("\n4□ INTEGRATION VALIDATION")
    result = validator.validate_integration(conn)
    print(f"\tStatus: {'✅' if result.valid else '❌'} {result.message}")
    for warning in result.warnings:
        print(f"\t⚠\t{warning}")

    conn.close()
    print("\n" + "=" * 80 + "\n")


if __name__ == '__main__':
    validate_full_system('nse_data_production/nse_data.db')
