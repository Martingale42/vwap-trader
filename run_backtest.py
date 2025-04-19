from decimal import Decimal

import polars as pl
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.node import (
    BacktestDataConfig,
    BacktestEngineConfig,
    BacktestNode,
    BacktestRunConfig,
    BacktestVenueConfig,
)
from nautilus_trader.backtest.results import BacktestResult
from nautilus_trader.config import ImportableStrategyConfig, LoggingConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.persistence.catalog import ParquetDataCatalog


def run_backtest():
    start = "2024-01-01"
    end = "2024-12-31"
    # You can also use a relative path such as `ParquetDataCatalog("./catalog")`,
    # for example if you're running this notebook after the data setup from the docs.
    # catalog = ParquetDataCatalog.from_env()
    catalog = ParquetDataCatalog("./data/binance/catalog")
    instruments = catalog.instruments()

    # Define the instrument for the strategy
    venue = BacktestVenueConfig(
        name="BINANCE",
        oms_type="NETTING",
        account_type="MARGIN",
        starting_balances=["100 USDT"],
        base_currency="USDT",
        default_leverage=Decimal("10.0"),
    )

    data_ADA_1 = BacktestDataConfig(
        catalog_path=str(catalog.path),
        data_cls=Bar,
        instrument_id=instruments[0].id,
        start_time=start,
        end_time=end,
        bar_types=[f"{instruments[0].id}-1-MINUTE-LAST-EXTERNAL"],
    )
    data_LTC_1 = BacktestDataConfig(
        catalog_path=str(catalog.path),
        data_cls=Bar,
        instrument_id=instruments[3].id,
        start_time=start,
        end_time=end,
        bar_types=[f"{instruments[3].id}-1-MINUTE-LAST-EXTERNAL"],
    )
    data_SUI_1 = BacktestDataConfig(
        catalog_path=str(catalog.path),
        data_cls=Bar,
        instrument_id=instruments[-1].id,
        start_time=start,
        end_time=end,
        bar_types=[f"{instruments[-1].id}-1-MINUTE-LAST-EXTERNAL"],
    )

    engine_ADA = BacktestEngineConfig(
        strategies=[
            ImportableStrategyConfig(
                strategy_path="src.vwap_strategy:VWAPMultiTimeframeStrategy",
                config_path="src.vwap_strategy:VWAPStrategyConfig",
                config={
                    "instrument_id": str(instruments[0].id),
                    "bar_type_1min": f"{instruments[0].id}-1-MINUTE-LAST-EXTERNAL",
                    "vwap_period_15min": 100,  # Approximately one trading day (for 15min bars)
                    "vwap_period_4h": 30,  # Approximately 5 trading days (for 4h bars)
                    "std_dev_multiplier": 2.0,  # Standard deviation multiplier for VWAP bands
                    "entry_volume_threshold": 1.5,  # Volume threshold compared to average
                    "risk_per_trade": 0.1,  # 10% risk per trade
                    "time_exit_hours": (
                        24
                        * 7  # Exit trade after 168 hours if not stopped out/taken profit
                    ),
                },
            )
        ],
        logging=LoggingConfig(log_level="INFO"),
    )
    engine_LTC = BacktestEngineConfig(
        strategies=[
            ImportableStrategyConfig(
                strategy_path="src.vwap_strategy:VWAPMultiTimeframeStrategy",
                config_path="src.vwap_strategy:VWAPStrategyConfig",
                config={
                    "instrument_id": str(instruments[3].id),
                    "bar_type_1min": f"{instruments[3].id}-1-MINUTE-LAST-EXTERNAL",
                    "vwap_period_15min": 100,  # Approximately one trading day (for 15min bars)
                    "vwap_period_4h": 30,  # Approximately 5 trading days (for 4h bars)
                    "std_dev_multiplier": 2.0,  # Standard deviation multiplier for VWAP bands
                    "entry_volume_threshold": 1.5,  # Volume threshold compared to average
                    "risk_per_trade": 0.1,  # 10% risk per trade
                    "time_exit_hours": (
                        24
                        * 7  # Exit trade after 168 hours if not stopped out/taken profit
                    ),
                },
            )
        ],
        logging=LoggingConfig(log_level="INFO"),
    )
    engine_SUI = BacktestEngineConfig(
        strategies=[
            ImportableStrategyConfig(
                strategy_path="src.vwap_strategy:VWAPMultiTimeframeStrategy",
                config_path="src.vwap_strategy:VWAPStrategyConfig",
                config={
                    "instrument_id": str(instruments[-1].id),
                    "bar_type_1min": f"{instruments[-1].id}-1-MINUTE-LAST-EXTERNAL",
                    "vwap_period_15min": 100,  # Approximately one trading day (for 15min bars)
                    "vwap_period_4h": 30,  # Approximately 5 trading days (for 4h bars)
                    "std_dev_multiplier": 2.0,  # Standard deviation multiplier for VWAP bands
                    "entry_volume_threshold": 1.5,  # Volume threshold compared to average
                    "risk_per_trade": 0.1,  # 10% risk per trade
                    "time_exit_hours": (
                        24
                        * 7  # Exit trade after 168 hours if not stopped out/taken profit
                    ),
                },
            )
        ],
        logging=LoggingConfig(log_level="INFO"),
    )
    config_ADA = BacktestRunConfig(
        engine=engine_ADA,
        venues=[venue],
        data=[data_ADA_1],
    )
    config_LTC = BacktestRunConfig(
        engine=engine_LTC,
        venues=[venue],
        data=[data_LTC_1],
    )
    config_SUI = BacktestRunConfig(
        engine=engine_SUI,
        venues=[venue],
        data=[data_SUI_1],
    )

    configs = [config_ADA, config_LTC, config_SUI]

    node = BacktestNode(configs=configs)

    # Runs one or many configs synchronously
    results: list[BacktestResult] = node.run()  # noqa

    for i in range(len(configs)):
        id = ["ADA", "LTC", "SUI"]
        engine: BacktestEngine = node.get_engine(configs[i].id)
        order_fills_report = pl.DataFrame(engine.trader.generate_order_fills_report())
        order_fills_report.write_json(f"./reports/order_fills_report_{id[i]}.json")
        positions_report = pl.DataFrame(engine.trader.generate_positions_report())
        positions_report.write_json(f"./reports/positions_report_{id[i]}.json")
        account_report = pl.DataFrame(
            engine.trader.generate_account_report(Venue("BINANCE"))
        )
        account_report.write_json(f"./reports/account_report_{id[i]}.json")

    node.dispose()


if __name__ == "__main__":
    run_backtest()
