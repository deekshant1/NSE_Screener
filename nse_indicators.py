"""
Technical Indicators Module for NSE Data
=========================================
Production-grade technical analysis using TA-Lib (C library) or pandas-ta (pure Python)

Install (Choose ONE):
  pip install TA-Lib          # Fastest, requires C compiler
  pip install pandas-ta        # Pure Python, easier install

Integration Steps:
1. Copy this file as: nse_indicators.py
2. Import in your analysis scripts:
   from nse_indicators import TechnicalAnalyzer
3. Use with your data:
   analyzer = TechnicalAnalyzer()
   df = analyzer.calculate_all(df)
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional, List, Dict

# Try TA-Lib first (fastest), fall back to pandas-ta
try:
    import talib

    HAS_TALIB = True
    HAS_PANDAS_TA = False
    logger = logging.getLogger('nse_downloader')
    logger.info("Using TA-Lib for indicators (C-based, fastest)")
except ImportError:
    HAS_TALIB = False
    try:
        import pandas_ta as ta

        HAS_PANDAS_TA = True
        logger = logging.getLogger('nse_downloader')
        logger.info("Using pandas-ta for indicators (pure Python)")
    except ImportError:
        HAS_PANDAS_TA = False
        print("⚠️  No technical analysis library found!")
        print("   Install one: pip install TA-Lib  OR  pip install pandas-ta")

logger = logging.getLogger('nse_downloader')


class TechnicalAnalyzer:
    """
    Calculate technical indicators using best available library

    Usage:
        analyzer = TechnicalAnalyzer()
        df_with_indicators = analyzer.calculate_all(df)
    """

    def __init__(self):
        self.has_library = HAS_TALIB or HAS_PANDAS_TA
        self.using_talib = HAS_TALIB

        if not self.has_library:
            logger.warning(
                "No technical analysis library available. "
                "Indicators will not be calculated."
            )

    def calculate_all(self, df: pd.DataFrame,
                      indicators: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Calculate all or selected technical indicators

        Args:
            df: DataFrame with OHLCV data
            indicators: List of indicators to calculate, or None for all

        Returns:
            DataFrame with indicators added
        """
        if not self.has_library:
            logger.warning("Cannot calculate indicators - no library available")
            return df

        # Validate required columns
        required = ['open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required):
            logger.error(f"Missing required columns. Need: {required}")
            return df

        df = df.copy()

        # Default: calculate all indicators
        if indicators is None:
            indicators = [
                'sma', 'ema', 'rsi', 'macd', 'bbands',
                'stoch', 'atr', 'adx', 'obv', 'vwap'
            ]

        # Calculate each indicator
        for indicator in indicators:
            try:
                if indicator == 'sma':
                    df = self.add_sma(df)
                elif indicator == 'ema':
                    df = self.add_ema(df)
                elif indicator == 'rsi':
                    df = self.add_rsi(df)
                elif indicator == 'macd':
                    df = self.add_macd(df)
                elif indicator == 'bbands':
                    df = self.add_bollinger_bands(df)
                elif indicator == 'stoch':
                    df = self.add_stochastic(df)
                elif indicator == 'atr':
                    df = self.add_atr(df)
                elif indicator == 'adx':
                    df = self.add_adx(df)
                elif indicator == 'obv':
                    df = self.add_obv(df)
                elif indicator == 'vwap':
                    df = self.add_vwap(df)
                else:
                    logger.warning(f"Unknown indicator: {indicator}")
            except Exception as e:
                logger.error(f"Error calculating {indicator}: {e}")

        return df

    # ==================== TREND INDICATORS ====================

    def add_sma(self, df: pd.DataFrame,
                periods: List[int] = [20, 50, 200]) -> pd.DataFrame:
        """Simple Moving Average"""
        for period in periods:
            if HAS_TALIB:
                df[f'sma_{period}'] = talib.SMA(df['close'], timeperiod=period)
            elif HAS_PANDAS_TA:
                df[f'sma_{period}'] = ta.sma(df['close'], length=period)
            else:
                df[f'sma_{period}'] = df['close'].rolling(window=period).mean()

        return df

    def add_ema(self, df: pd.DataFrame,
                periods: List[int] = [12, 26, 50]) -> pd.DataFrame:
        """Exponential Moving Average"""
        for period in periods:
            if HAS_TALIB:
                df[f'ema_{period}'] = talib.EMA(df['close'], timeperiod=period)
            elif HAS_PANDAS_TA:
                df[f'ema_{period}'] = ta.ema(df['close'], length=period)
            else:
                df[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean()

        return df

    def add_macd(self, df: pd.DataFrame,
                 fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        """MACD (Moving Average Convergence Divergence)"""
        if HAS_TALIB:
            macd, signal_line, hist = talib.MACD(
                df['close'],
                fastperiod=fast,
                slowperiod=slow,
                signalperiod=signal
            )
            df['macd'] = macd
            df['macd_signal'] = signal_line
            df['macd_hist'] = hist
        elif HAS_PANDAS_TA:
            macd_df = ta.macd(df['close'], fast=fast, slow=slow, signal=signal)
            df = pd.concat([df, macd_df], axis=1)
        else:
            # Manual calculation
            ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
            ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
            df['macd'] = ema_fast - ema_slow
            df['macd_signal'] = df['macd'].ewm(span=signal, adjust=False).mean()
            df['macd_hist'] = df['macd'] - df['macd_signal']

        return df

    # ==================== MOMENTUM INDICATORS ====================

    def add_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Relative Strength Index"""
        if HAS_TALIB:
            df['rsi'] = talib.RSI(df['close'], timeperiod=period)
        elif HAS_PANDAS_TA:
            df['rsi'] = ta.rsi(df['close'], length=period)
        else:
            # Manual calculation
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))

        return df

    def add_stochastic(self, df: pd.DataFrame,
                       k_period: int = 14, d_period: int = 3) -> pd.DataFrame:
        """Stochastic Oscillator"""
        if HAS_TALIB:
            slowk, slowd = talib.STOCH(
                df['high'], df['low'], df['close'],
                fastk_period=k_period,
                slowk_period=d_period,
                slowd_period=d_period
            )
            df['stoch_k'] = slowk
            df['stoch_d'] = slowd
        elif HAS_PANDAS_TA:
            stoch_df = ta.stoch(df['high'], df['low'], df['close'],
                                k=k_period, d=d_period)
            df = pd.concat([df, stoch_df], axis=1)
        else:
            # Manual calculation
            low_min = df['low'].rolling(window=k_period).min()
            high_max = df['high'].rolling(window=k_period).max()
            df['stoch_k'] = 100 * (df['close'] - low_min) / (high_max - low_min)
            df['stoch_d'] = df['stoch_k'].rolling(window=d_period).mean()

        return df

    # ==================== VOLATILITY INDICATORS ====================

    def add_bollinger_bands(self, df: pd.DataFrame,
                            period: int = 20, std: float = 2.0) -> pd.DataFrame:
        """Bollinger Bands"""
        if HAS_TALIB:
            upper, middle, lower = talib.BBANDS(
                df['close'],
                timeperiod=period,
                nbdevup=std,
                nbdevdn=std
            )
            df['bb_upper'] = upper
            df['bb_middle'] = middle
            df['bb_lower'] = lower
        elif HAS_PANDAS_TA:
            bb_df = ta.bbands(df['close'], length=period, std=std)
            df = pd.concat([df, bb_df], axis=1)
        else:
            # Manual calculation
            sma = df['close'].rolling(window=period).mean()
            std_dev = df['close'].rolling(window=period).std()
            df['bb_middle'] = sma
            df['bb_upper'] = sma + (std_dev * std)
            df['bb_lower'] = sma - (std_dev * std)

        # Add bandwidth and %B
        df['bb_bandwidth'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle']
        df['bb_percent'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])

        return df

    def add_atr(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Average True Range"""
        if HAS_TALIB:
            df['atr'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=period)
        elif HAS_PANDAS_TA:
            df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=period)
        else:
            # Manual calculation
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = np.max(ranges, axis=1)
            df['atr'] = true_range.rolling(window=period).mean()

        return df

    # ==================== TREND STRENGTH ====================

    def add_adx(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Average Directional Index"""
        if HAS_TALIB:
            df['adx'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=period)
            df['plus_di'] = talib.PLUS_DI(df['high'], df['low'], df['close'], timeperiod=period)
            df['minus_di'] = talib.MINUS_DI(df['high'], df['low'], df['close'], timeperiod=period)
        elif HAS_PANDAS_TA:
            adx_df = ta.adx(df['high'], df['low'], df['close'], length=period)
            df = pd.concat([df, adx_df], axis=1)
        else:
            logger.warning("ADX requires TA-Lib or pandas-ta (complex calculation)")

        return df

    # ==================== VOLUME INDICATORS ====================

    def add_obv(self, df: pd.DataFrame) -> pd.DataFrame:
        """On-Balance Volume"""
        if HAS_TALIB:
            df['obv'] = talib.OBV(df['close'], df['volume'])
        elif HAS_PANDAS_TA:
            df['obv'] = ta.obv(df['close'], df['volume'])
        else:
            # Manual calculation
            df['obv'] = (np.sign(df['close'].diff()) * df['volume']).fillna(0).cumsum()

        return df

    def add_vwap(self, df: pd.DataFrame) -> pd.DataFrame:
        """Volume Weighted Average Price (intraday indicator)"""
        # Note: VWAP is typically reset daily, but we calculate cumulative here
        if HAS_PANDAS_TA:
            df['vwap'] = ta.vwap(df['high'], df['low'], df['close'], df['volume'])
        else:
            # Manual calculation
            typical_price = (df['high'] + df['low'] + df['close']) / 3
            df['vwap'] = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()

        return df

    # ==================== CUSTOM SIGNALS ====================

    def add_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add trading signals based on indicators

        Signals:
        - trend_signal: 1 (bullish), -1 (bearish), 0 (neutral)
        - momentum_signal: 1 (bullish), -1 (bearish), 0 (neutral)
        - overall_signal: Combined signal
        """
        df = df.copy()

        # Trend signal (based on moving averages)
        if 'sma_20' in df.columns and 'sma_50' in df.columns:
            df['trend_signal'] = np.where(
                df['sma_20'] > df['sma_50'], 1,
                np.where(df['sma_20'] < df['sma_50'], -1, 0)
            )

        # Momentum signal (based on RSI)
        if 'rsi' in df.columns:
            df['momentum_signal'] = np.where(
                df['rsi'] < 30, 1,  # Oversold - bullish
                np.where(df['rsi'] > 70, -1, 0)  # Overbought - bearish
            )

        # Combine signals
        if 'trend_signal' in df.columns and 'momentum_signal' in df.columns:
            df['overall_signal'] = df['trend_signal'] + df['momentum_signal']
            # Normalize to -1, 0, 1
            df['overall_signal'] = np.sign(df['overall_signal'])

        return df

    # ==================== PATTERN RECOGNITION ====================

    def add_candlestick_patterns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add candlestick pattern recognition (requires TA-Lib)"""
        if not HAS_TALIB:
            logger.warning("Candlestick patterns require TA-Lib")
            return df

        df = df.copy()

        # Bullish patterns
        df['hammer'] = talib.CDLHAMMER(df['open'], df['high'], df['low'], df['close'])
        df['morning_star'] = talib.CDLMORNINGSTAR(df['open'], df['high'], df['low'], df['close'])
        df['engulfing_bull'] = talib.CDLENGULFING(df['open'], df['high'], df['low'], df['close'])
        df['piercing'] = talib.CDLPIERCING(df['open'], df['high'], df['low'], df['close'])

        # Bearish patterns
        df['hanging_man'] = talib.CDLHANGINGMAN(df['open'], df['high'], df['low'], df['close'])
        df['evening_star'] = talib.CDLEVENINGSTAR(df['open'], df['high'], df['low'], df['close'])
        df['dark_cloud'] = talib.CDLDARKCLOUDCOVER(df['open'], df['high'], df['low'], df['close'])
        df['shooting_star'] = talib.CDLSHOOTINGSTAR(df['open'], df['high'], df['low'], df['close'])

        # Doji patterns
        df['doji'] = talib.CDLDOJI(df['open'], df['high'], df['low'], df['close'])
        df['dragonfly_doji'] = talib.CDLDRAGONFLYDOJI(df['open'], df['high'], df['low'], df['close'])
        df['gravestone_doji'] = talib.CDLGRAVESTONEDOJI(df['open'], df['high'], df['low'], df['close'])

        return df


# ==================== BATCH CALCULATION ====================

def calculate_indicators_for_symbol(df: pd.DataFrame, symbol: str,
                                    indicators: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Calculate indicators for a single symbol (use in loops)

    Args:
        df: DataFrame with OHLCV data for ONE symbol
        symbol: Symbol name (for logging)
        indicators: List of indicators or None for all

    Returns:
        DataFrame with indicators added
    """
    analyzer = TechnicalAnalyzer()

    try:
        df_with_indicators = analyzer.calculate_all(df, indicators)
        logger.debug(f"Calculated indicators for {symbol}")
        return df_with_indicators
    except Exception as e:
        logger.error(f"Failed to calculate indicators for {symbol}: {e}")
        return df


def calculate_indicators_batch(df: pd.DataFrame,
                               indicators: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Calculate indicators for multiple symbols efficiently

    Args:
        df: DataFrame with OHLCV data (can have multiple symbols)
        indicators: List of indicators or None for all

    Returns:
        DataFrame with indicators added for all symbols
    """
    if 'symbol' not in df.columns:
        # Single symbol data
        return calculate_indicators_for_symbol(df, 'UNKNOWN', indicators)

    # Multiple symbols - process each separately
    results = []
    for symbol, group in df.groupby('symbol'):
        df_with_indicators = calculate_indicators_for_symbol(
            group.copy(), symbol, indicators
        )
        results.append(df_with_indicators)

    return pd.concat(results, ignore_index=True)


# ==================== DATABASE INTEGRATION ====================

def add_indicators_to_database(db_path: str, table: str = 'equity_data',
                               symbols: Optional[List[str]] = None,
                               indicators: Optional[List[str]] = None):
    """
    Add indicators directly to database (creates new columns)

    Args:
        db_path: Path to SQLite database
        table: Table name ('equity_data' or 'derivatives_data')
        symbols: List of symbols to process (None = all)
        indicators: List of indicators to calculate (None = all)

    Example:
        add_indicators_to_database(
            'nse_data_production/nse_data.db',
            table='equity_data',
            symbols=['RELIANCE', 'TCS'],
            indicators=['sma', 'rsi', 'macd']
        )
    """
    import sqlite3

    conn = sqlite3.connect(db_path)

    try:
        # Get symbols to process
        if symbols:
            symbol_filter = "'" + "','".join(symbols) + "'"
            query = f"SELECT * FROM {table} WHERE symbol IN ({symbol_filter}) ORDER BY symbol, date"
        else:
            query = f"SELECT * FROM {table} ORDER BY symbol, date"

        logger.info(f"Loading data from {table}...")
        df = pd.read_sql_query(query, conn)

        if df.empty:
            logger.warning("No data found")
            return

        logger.info(f"Calculating indicators for {df['symbol'].nunique()} symbols...")
        df_with_indicators = calculate_indicators_batch(df, indicators)

        # Get new columns (indicators)
        original_cols = set(df.columns)
        new_cols = [col for col in df_with_indicators.columns if col not in original_cols]

        if not new_cols:
            logger.warning("No new indicators calculated")
            return

        logger.info(f"Adding {len(new_cols)} indicator columns to database...")

        # Add new columns to table
        cursor = conn.cursor()
        for col in new_cols:
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} REAL")
                logger.debug(f"Added column: {col}")
            except sqlite3.OperationalError:
                pass  # Column already exists

        conn.commit()

        # Update values (batch by symbol for efficiency)
        for symbol in df_with_indicators['symbol'].unique():
            symbol_data = df_with_indicators[df_with_indicators['symbol'] == symbol]

            for _, row in symbol_data.iterrows():
                update_parts = []
                values = []

                for col in new_cols:
                    if pd.notna(row[col]):
                        update_parts.append(f"{col} = ?")
                        values.append(row[col])

                if update_parts:
                    values.extend([row['symbol'], row['date']])
                    update_sql = f"""
                        UPDATE {table} 
                        SET {', '.join(update_parts)}
                        WHERE symbol = ? AND date = ?
                    """
                    cursor.execute(update_sql, values)

        conn.commit()
        logger.info(f"✅ Successfully added indicators to {table}")

    except Exception as e:
        logger.error(f"Failed to add indicators to database: {e}")
        conn.rollback()
    finally:
        conn.close()


# ==================== SCREENING & FILTERING ====================

class TechnicalScreener:
    """Screen stocks based on technical indicators"""

    @staticmethod
    def screen_golden_cross(df: pd.DataFrame) -> pd.DataFrame:
        """Find stocks with golden cross (SMA 50 crosses above SMA 200)"""
        if 'sma_50' not in df.columns or 'sma_200' not in df.columns:
            logger.error("Need SMA 50 and 200 for golden cross")
            return pd.DataFrame()

        # Current: SMA50 > SMA200, Previous: SMA50 < SMA200
        df['prev_sma_50'] = df.groupby('symbol')['sma_50'].shift(1)
        df['prev_sma_200'] = df.groupby('symbol')['sma_200'].shift(1)

        golden_cross = df[
            (df['sma_50'] > df['sma_200']) &
            (df['prev_sma_50'] < df['prev_sma_200'])
            ]

        return golden_cross[['symbol', 'date', 'close', 'sma_50', 'sma_200']]

    @staticmethod
    def screen_rsi_oversold(df: pd.DataFrame, threshold: float = 30) -> pd.DataFrame:
        """Find oversold stocks (RSI < threshold)"""
        if 'rsi' not in df.columns:
            logger.error("Need RSI for oversold screening")
            return pd.DataFrame()

        oversold = df[df['rsi'] < threshold].copy()
        oversold = oversold.sort_values('rsi')

        return oversold[['symbol', 'date', 'close', 'rsi']]

    @staticmethod
    def screen_rsi_overbought(df: pd.DataFrame, threshold: float = 70) -> pd.DataFrame:
        """Find overbought stocks (RSI > threshold)"""
        if 'rsi' not in df.columns:
            logger.error("Need RSI for overbought screening")
            return pd.DataFrame()

        overbought = df[df['rsi'] > threshold].copy()
        overbought = overbought.sort_values('rsi', ascending=False)

        return overbought[['symbol', 'date', 'close', 'rsi']]

    @staticmethod
    def screen_breakout(df: pd.DataFrame, lookback: int = 20) -> pd.DataFrame:
        """Find stocks breaking out of 20-day high"""
        df['high_20'] = df.groupby('symbol')['high'].rolling(lookback).max().reset_index(0, drop=True)

        breakout = df[df['close'] >= df['high_20']].copy()

        return breakout[['symbol', 'date', 'close', 'high_20', 'volume']]

    @staticmethod
    def screen_volume_surge(df: pd.DataFrame, multiplier: float = 2.0) -> pd.DataFrame:
        """Find stocks with volume > multiplier × average volume"""
        df['avg_volume_20'] = df.groupby('symbol')['volume'].rolling(20).mean().reset_index(0, drop=True)

        surge = df[df['volume'] >= df['avg_volume_20'] * multiplier].copy()
        surge['volume_ratio'] = surge['volume'] / surge['avg_volume_20']
        surge = surge.sort_values('volume_ratio', ascending=False)

        return surge[['symbol', 'date', 'close', 'volume', 'avg_volume_20', 'volume_ratio']]

    @staticmethod
    def screen_trend_strength(df: pd.DataFrame, adx_threshold: float = 25) -> pd.DataFrame:
        """Find stocks with strong trends (ADX > threshold)"""
        if 'adx' not in df.columns:
            logger.error("Need ADX for trend strength screening")
            return pd.DataFrame()

        strong_trend = df[df['adx'] > adx_threshold].copy()
        strong_trend = strong_trend.sort_values('adx', ascending=False)

        return strong_trend[['symbol', 'date', 'close', 'adx']]


# ==================== EXAMPLE USAGE ====================

if __name__ == "__main__":
    """
    Example: Calculate indicators for NSE data
    """
    import sqlite3

    print("=" * 60)
    print("TECHNICAL INDICATORS MODULE - EXAMPLE USAGE")
    print("=" * 60)

    # Example 1: Single stock analysis
    print("\n1. Single Stock Analysis (RELIANCE):")
    print("-" * 60)

    # Simulated data (replace with actual database query)
    dates = pd.date_range('2024-01-01', '2024-10-01', freq='D')
    np.random.seed(42)
    sample_df = pd.DataFrame({
        'date': dates,
        'open': 2500 + np.random.randn(len(dates)).cumsum() * 10,
        'high': 2510 + np.random.randn(len(dates)).cumsum() * 10,
        'low': 2490 + np.random.randn(len(dates)).cumsum() * 10,
        'close': 2500 + np.random.randn(len(dates)).cumsum() * 10,
        'volume': 1000000 + np.random.randint(-100000, 100000, len(dates))
    })

    analyzer = TechnicalAnalyzer()

    if analyzer.has_library:
        result = analyzer.calculate_all(sample_df, indicators=['sma', 'rsi', 'macd'])
        print(result[['date', 'close', 'sma_20', 'sma_50', 'rsi']].tail())

        # Add signals
        result = analyzer.add_signals(result)
        print("\nLatest signals:")
        print(result[['date', 'close', 'trend_signal', 'momentum_signal', 'overall_signal']].tail())
    else:
        print("⚠️  No technical analysis library installed")
        print("   Install: pip install TA-Lib  OR  pip install pandas-ta")

    # Example 2: Screening
    print("\n2. Technical Screening:")
    print("-" * 60)

    if analyzer.has_library:
        screener = TechnicalScreener()

        # Add symbol for screening
        sample_df['symbol'] = 'RELIANCE'
        result_with_indicators = analyzer.calculate_all(sample_df)

        # Screen for oversold
        oversold = screener.screen_rsi_oversold(result_with_indicators, threshold=30)
        if not oversold.empty:
            print(f"\nOversold stocks (RSI < 30): {len(oversold)}")
            print(oversold.head())
        else:
            print("\nNo oversold stocks found")

    # Example 3: Database integration
    print("\n3. Database Integration Example:")
    print("-" * 60)
    print("""
    # Add indicators to your NSE database:
    from nse_indicators import add_indicators_to_database

    add_indicators_to_database(
        'nse_data_production/nse_data.db',
        table='equity_data',
        symbols=['RELIANCE', 'TCS', 'INFY'],
        indicators=['sma', 'rsi', 'macd']
    )
    """)

    print("\n" + "=" * 60)
    print("✅ Examples complete. Integrate into your workflow!")
    print("=" * 60)