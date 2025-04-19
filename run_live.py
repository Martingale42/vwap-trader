#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  VWAP Multi-Timeframe Strategy Live Trading Node for Binance Futures (Async Version)
# -------------------------------------------------------------------------------------------------

import asyncio
import os

from dotenv import load_dotenv
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.config import (
    BinanceDataClientConfig,
    BinanceExecClientConfig,
)
from nautilus_trader.adapters.binance.factories import (
    BinanceLiveDataClientFactory,
    BinanceLiveExecClientFactory,
)
from nautilus_trader.config import (
    CacheConfig,
    InstrumentProviderConfig,
    LiveExecEngineConfig,
    LoggingConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import TraderId

# Import your VWAP strategy
from src.vwap_strategy import VWAPMultiTimeframeStrategy, VWAPStrategyConfig


async def main():
    """
    Run a VWAP multi-timeframe strategy on Binance Futures asynchronously.
    """
    # ----------------------------------------------------------------------------------
    # 1. Configure the trading node
    # ----------------------------------------------------------------------------------
    instrument_id = "ADAUSDT-PERP.BINANCE"
    # You can change this to your desired instrument
    # "ADAUSDT-PERP.BINANCE",
    # "LTCUSDT-PERP.BINANCE",
    # "XRPUSDT-PERP.BINANCE",

    # Get API credentials from environment variables
    load_dotenv()
    api_key = os.getenv("BINANCE_FUTURES_API_KEY")
    api_secret = os.getenv("BINANCE_FUTURES_API_SECRET")

    config_node = TradingNodeConfig(
        trader_id=TraderId("VWAP-TRADER-001"),
        logging=LoggingConfig(
            log_level="INFO",
            log_level_file="DEBUG",
            log_file_name="vwap-strategy.json",
            log_file_format="json",
            log_directory="./logs",
            log_colors=True,
            use_pyo3=True,
        ),
        exec_engine=LiveExecEngineConfig(
            reconciliation=True,
            reconciliation_lookback_mins=1440,
            filter_position_reports=True,
        ),
        cache=CacheConfig(
            timestamps_as_iso8601=True,
            flush_on_start=False,
        ),
        data_clients={
            "BINANCE": BinanceDataClientConfig(
                api_key=api_key,
                api_secret=api_secret,
                account_type=BinanceAccountType.USDT_FUTURE,
                testnet=False,  # Set to False for live trading
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
        },
        exec_clients={
            "BINANCE": BinanceExecClientConfig(
                api_key=api_key,
                api_secret=api_secret,
                account_type=BinanceAccountType.USDT_FUTURE,
                testnet=False,  # Set to False for live trading
                instrument_provider=InstrumentProviderConfig(load_all=True),
                max_retries=3,
                retry_delay=1.0,
            ),
        },
        timeout_connection=30.0,
        timeout_reconciliation=10.0,
        timeout_portfolio=10.0,
        timeout_disconnection=10.0,
        timeout_post_stop=5.0,
    )

    # ----------------------------------------------------------------------------------
    # 2. Instantiate the node with the configuration
    # ----------------------------------------------------------------------------------
    node = TradingNode(config=config_node)

    # ----------------------------------------------------------------------------------
    # 3. Configure your VWAP strategy
    # ----------------------------------------------------------------------------------

    # Create 1-min and 4-hour bar types
    bar_type_1min = f"{instrument_id}-1-MINUTE-LAST-EXTERNAL"

    strat_config = VWAPStrategyConfig(
        instrument_id=instrument_id,
        bar_type_1min=bar_type_1min,
        vwap_period_15min=100,  # Approximately one trading day (for 15min bars)
        vwap_period_4h=30,  # Approximately 5 trading days (for 4h bars)
        std_dev_multiplier=2.0,  # Standard deviation multiplier for VWAP bands
        entry_volume_threshold=1.5,  # Volume threshold compared to average
        risk_per_trade=0.2,  # 10% risk per trade
        time_exit_hours=24,  # Exit trade after 24 hours if not stopped out/taken profit
    )

    # ----------------------------------------------------------------------------------
    # 4. Instantiate your strategy with the configuration
    # ----------------------------------------------------------------------------------
    strategy = VWAPMultiTimeframeStrategy(config=strat_config)

    # ----------------------------------------------------------------------------------
    # 5. Add your strategy to the node's trader
    # ----------------------------------------------------------------------------------
    node.trader.add_strategy(strategy)

    # ----------------------------------------------------------------------------------
    # 6. Register client factories with the node
    # ----------------------------------------------------------------------------------
    node.add_data_client_factory("BINANCE", BinanceLiveDataClientFactory)
    node.add_exec_client_factory("BINANCE", BinanceLiveExecClientFactory)

    # ----------------------------------------------------------------------------------
    # 7. Build the node
    # ----------------------------------------------------------------------------------
    node.build()

    # ----------------------------------------------------------------------------------
    # 8. Run the node asynchronously
    # ----------------------------------------------------------------------------------
    print(f"Starting VWAP Multi-Timeframe Strategy on {instrument_id}...")
    try:
        # Run the node asynchronously
        await node.run_async()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Always properly clean up resources
        await node.stop_async()
        await asyncio.sleep(1)  # Give time for cleanup
        node.dispose()
        print("Strategy stopped and resources released.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopping strategy (CTRL+C detected)...")
