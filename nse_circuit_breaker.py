"""
Circuit Breaker Module for NSE Downloader
==========================================
Production-grade circuit breaker to prevent cascading failures.

Install: pip install pybreaker

Integration Steps:
1. Copy this file as: nse_circuit_breaker.py
2. In main nse_downloader.py, add at top:
   from nse_circuit_breaker import NSECircuitBreakers
3. In NSEDownloader.__init__(), add:
   self.circuit_breakers = NSECircuitBreakers()
4. Replace _request() method with circuit_breaker_request()
5. That's it! Circuit breakers are now active.
"""

import time
import logging
from typing import Optional, Callable
from datetime import datetime

try:
    import pybreaker
    HAS_PYBREAKER = True
except ImportError:
    HAS_PYBREAKER = False
    print("⚠️  pybreaker not installed. Circuit breakers disabled.")
    print("   Install: pip install pybreaker")

logger = logging.getLogger('nse_downloader')


class CircuitBreakerConfig:
    """Circuit breaker configuration for different services"""

    # NSE website circuit breaker
    NSE_MAIN = {
        'fail_max': 5,              # Open after 5 failures
        'reset_timeout': 60,        # Try again after 60 seconds
        'name': 'nse_main_site'
    }

    # NSE Archives (old format) circuit breaker
    NSE_ARCHIVES = {
        'fail_max': 10,             # More tolerant (old system)
        'reset_timeout': 120,       # Longer recovery
        'name': 'nse_archives'
    }

    # NSE New Archives circuit breaker
    NSE_NEW_ARCHIVES = {
        'fail_max': 5,
        'reset_timeout': 60,
        'name': 'nse_new_archives'
    }

    # Database circuit breaker
    DATABASE = {
        'fail_max': 3,              # Stricter for DB
        'reset_timeout': 30,
        'name': 'database'
    }


class CircuitBreakerMonitor:
    """
    Monitor circuit breaker state and log transitions

    IMPORTANT: pybreaker expects listener objects with these exact methods:
    - before_call(cb, func, *args, **kwargs)
    - success(cb)
    - failure(cb, exc)
    - state_change(cb, old_state, new_state)
    """

    def __init__(self):
        self.state_changes = []
        self.metrics = {
            'total_calls': 0,
            'successful_calls': 0,
            'failed_calls': 0,
            'circuit_opens': 0,
            'circuit_closes': 0
        }

    def before_call(self, cb, func, *args, **kwargs):
        """Called before function execution"""
        pass

    def success(self, cb):
        """Called on successful call"""
        self.metrics['total_calls'] += 1
        self.metrics['successful_calls'] += 1

    def failure(self, cb, exc):
        """Called on failed call"""
        self.metrics['total_calls'] += 1
        self.metrics['failed_calls'] += 1
        logger.debug(
            f"Circuit breaker {cb.name} failure "
            f"({cb.fail_counter}/{cb.fail_max}): {exc}"
        )

    def state_change(self, cb, old_state, new_state):
        """Called when circuit breaker state changes"""
        timestamp = datetime.now()
        change = {
            'breaker': cb.name,
            'old_state': str(old_state),
            'new_state': str(new_state),
            'timestamp': timestamp.isoformat()
        }
        self.state_changes.append(change)

        if new_state == pybreaker.STATE_OPEN:
            self.metrics['circuit_opens'] += 1
            logger.error(
                f"🔴 Circuit breaker OPENED: {cb.name} "
                f"(failures: {cb.fail_counter}/{cb.fail_max})"
            )
        elif new_state == pybreaker.STATE_CLOSED:
            self.metrics['circuit_closes'] += 1
            logger.info(
                f"🟢 Circuit breaker CLOSED: {cb.name} "
                f"(recovered)"
            )
        elif new_state == pybreaker.STATE_HALF_OPEN:
            logger.warning(
                f"🟡 Circuit breaker HALF-OPEN: {cb.name} "
                f"(testing recovery...)"
            )

    def get_report(self) -> dict:
        """Get circuit breaker metrics report"""
        return {
            'metrics': self.metrics.copy(),
            'recent_state_changes': self.state_changes[-10:],  # Last 10
            'success_rate': (
                (self.metrics['successful_calls'] / self.metrics['total_calls'] * 100)
                if self.metrics['total_calls'] > 0 else 0
            )
        }


class NSECircuitBreakers:
    """
    Centralized circuit breaker management for NSE Downloader

    Usage:
        breakers = NSECircuitBreakers()
        response = breakers.call_with_breaker('nse_main', download_func, url)
    """

    def __init__(self):
        self.monitor = CircuitBreakerMonitor()
        self.breakers = {}

        if HAS_PYBREAKER:
            self._init_breakers()
            logger.info("Circuit breakers initialized")
        else:
            logger.warning("Circuit breakers disabled (pybreaker not installed)")

    def _init_breakers(self):
        """Initialize all circuit breakers with correct pybreaker API"""
        configs = {
            'nse_main': CircuitBreakerConfig.NSE_MAIN,
            'nse_archives': CircuitBreakerConfig.NSE_ARCHIVES,
            'nse_new_archives': CircuitBreakerConfig.NSE_NEW_ARCHIVES,
            'database': CircuitBreakerConfig.DATABASE
        }

        for name, config in configs.items():
            # Create breaker with correct parameters
            breaker = pybreaker.CircuitBreaker(**config)

            # Add listener object (not individual methods)
            breaker.add_listeners(self.monitor)

            self.breakers[name] = breaker

    def call_with_breaker(self, breaker_name: str, func: Callable, *args, **kwargs):
        """
        Execute function with circuit breaker protection

        Args:
            breaker_name: Name of breaker ('nse_main', 'nse_archives', etc)
            func: Function to execute
            *args, **kwargs: Arguments to pass to function

        Returns:
            Function result or None if circuit is open

        Raises:
            pybreaker.CircuitBreakerError: If circuit is open
        """
        if not HAS_PYBREAKER:
            # Fallback: execute without circuit breaker
            return func(*args, **kwargs)

        breaker = self.breakers.get(breaker_name)
        if not breaker:
            logger.warning(f"Unknown circuit breaker: {breaker_name}")
            return func(*args, **kwargs)

        try:
            return breaker.call(func, *args, **kwargs)
        except pybreaker.CircuitBreakerError as e:
            logger.warning(
                f"Circuit breaker {breaker_name} is OPEN, skipping call. "
                f"Will retry in {breaker._reset_timeout}s"
            )
            return None

    def get_breaker_state(self, breaker_name: str) -> Optional[str]:
        """Get current state of a circuit breaker"""
        if not HAS_PYBREAKER:
            return None

        breaker = self.breakers.get(breaker_name)
        if breaker:
            return str(breaker.current_state)
        return None

    def get_all_states(self) -> dict:
        """Get states of all circuit breakers"""
        if not HAS_PYBREAKER:
            return {}

        states = {}
        for name, breaker in self.breakers.items():
            states[name] = {
                'state': str(breaker.current_state),
                'fail_counter': breaker.fail_counter,
                'fail_max': breaker.fail_max,
                'reset_timeout': breaker._reset_timeout,
                'last_failure': (
                    breaker._last_failure.isoformat()
                    if hasattr(breaker, '_last_failure') and breaker._last_failure
                    else None
                )
            }

        return states

    def reset_breaker(self, breaker_name: str):
        """Manually reset a circuit breaker"""
        if not HAS_PYBREAKER:
            return

        breaker = self.breakers.get(breaker_name)
        if breaker:
            breaker.close()
            logger.info(f"Circuit breaker {breaker_name} manually reset")

    def reset_all(self):
        """Reset all circuit breakers"""
        if not HAS_PYBREAKER:
            return

        for name, breaker in self.breakers.items():
            breaker.close()
        logger.info("All circuit breakers reset")

    def get_metrics(self) -> dict:
        """Get monitoring metrics"""
        return self.monitor.get_report()


# ==================== INTEGRATION HELPERS ====================

def determine_breaker_for_url(url: str) -> str:
    """
    Determine which circuit breaker to use based on URL

    Args:
        url: The URL being accessed

    Returns:
        Circuit breaker name
    """
    if 'nsearchives.nseindia.com' in url:
        return 'nse_new_archives'
    elif 'archives.nseindia.com' in url:
        return 'nse_archives'
    elif 'nseindia.com' in url:
        return 'nse_main'
    else:
        return 'nse_main'  # Default


def circuit_breaker_request(circuit_breakers, session, url, timeout=30):
    """
    Drop-in replacement for session.get() with circuit breaker

    Usage in NSEDownloader._request():
        # Replace this:
        response = self.session.get(url, timeout=30)

        # With this:
        response = circuit_breaker_request(
            self.circuit_breakers,
            self.session,
            url,
            timeout=30
        )

    Args:
        circuit_breakers: NSECircuitBreakers instance
        session: requests.Session instance
        url: URL to fetch
        timeout: Request timeout

    Returns:
        Response object or None if circuit is open
    """
    breaker_name = determine_breaker_for_url(url)

    def make_request():
        return session.get(url, timeout=timeout)

    return circuit_breakers.call_with_breaker(breaker_name, make_request)


# ==================== CLI COMMANDS ====================

def print_circuit_breaker_status(circuit_breakers: NSECircuitBreakers):
    """Print circuit breaker status (for --status command)"""
    if not HAS_PYBREAKER:
        print("\n⚠️  Circuit breakers not available (pybreaker not installed)")
        return

    print("\n" + "=" * 60)
    print("CIRCUIT BREAKER STATUS")
    print("=" * 60)

    states = circuit_breakers.get_all_states()
    for name, info in states.items():
        state = info['state']
        emoji = {
            'STATE_CLOSED': '🟢',
            'STATE_OPEN': '🔴',
            'STATE_HALF_OPEN': '🟡'
        }.get(state, '⚪')

        print(f"\n{emoji} {name.upper()}:")
        print(f"   State: {state.replace('STATE_', '')}")
        print(f"   Failures: {info['fail_counter']}/{info['fail_max']}")

        if info['last_failure']:
            print(f"   Last Failure: {info['last_failure']}")

    # Metrics
    metrics = circuit_breakers.get_metrics()
    print(f"\n📊 METRICS:")
    print(f"   Total Calls: {metrics['metrics']['total_calls']}")
    print(f"   Success Rate: {metrics['success_rate']:.1f}%")
    print(f"   Circuit Opens: {metrics['metrics']['circuit_opens']}")
    print(f"   Circuit Closes: {metrics['metrics']['circuit_closes']}")

    print("=" * 60 + "\n")


# ==================== EXAMPLE INTEGRATION ====================

if __name__ == "__main__":
    """
    Example: Test circuit breakers
    """
    import requests

    print("Testing Circuit Breakers...")

    breakers = NSECircuitBreakers()
    session = requests.Session()

    # Simulate failures
    def failing_request():
        raise ConnectionError("Network error")

    print("\n1. Testing with failures (should open circuit):")
    for i in range(7):
        try:
            result = breakers.call_with_breaker('nse_main', failing_request)
            print(f"   Attempt {i + 1}: Success")
        except Exception as e:
            print(f"   Attempt {i + 1}: Failed ({type(e).__name__})")

    print("\n2. Current states:")
    print_circuit_breaker_status(breakers)

    print("\n3. Waiting for recovery (5 seconds)...")
    time.sleep(5)

    print("\n4. States after wait:")
    print_circuit_breaker_status(breakers)

    print("\n✅ Circuit breaker test complete")