# -------------------------------------------------------------------------------------------------
#  VWAP Multi-Timeframe Trading Strategy
#  使用VWAP在4小時和15分鐘時間框架上進行交易
# -------------------------------------------------------------------------------------------------

from collections import deque
from decimal import Decimal
from typing import Optional

import numpy as np
from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.indicators.vwap import VolumeWeightedAveragePrice
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.events import PositionClosed, PositionOpened
from nautilus_trader.model.identifiers import InstrumentId, Venue
from nautilus_trader.trading.strategy import Strategy


class VWAPStrategyConfig(StrategyConfig, frozen=True):
    """
    Configuration for the VWAP multi-timeframe strategy.
    """

    instrument_id: str
    bar_type_1min: str
    vwap_period_15min: int = 100  # Approximately one trading day (for 15min bars)
    vwap_period_4h: int = 30  # Approximately 5 trading days (for 4h bars)
    std_dev_multiplier: float = 2.0  # Standard deviation multiplier for VWAP bands
    entry_volume_threshold: float = 1.5  # Volume threshold compared to average
    risk_per_trade: float = 0.02  # 2% risk per trade
    time_exit_hours: int = (
        24 * 7  # Exit trade after 24 hours * 7 if not stopped out/taken profit
    )


class VWAPMultiTimeframeStrategy(Strategy):
    """
    VWAP multi-timeframe strategy that uses 4-hour and 15-minute bars.

    The strategy:
    1. Uses 4-hour VWAP to determine the overall trend
    2. Uses 15-minute VWAP for entry and exit signals
    3. Uses VWAP standard deviation bands for profit targets and stop losses
    4. Implements volume filters for signal confirmation
    5. Includes risk management with fixed percentage risk per trade
    """

    def __init__(self, config: VWAPStrategyConfig):
        """
        Initialize a new instance of the VWAPMultiTimeframeStrategy.
        """
        super().__init__(config=config)

        # Configuration
        self.bar_type_1min = BarType.from_str(config.bar_type_1min)
        self.bar_type_15min = BarType.from_str(
            f"{config.instrument_id}-15-MINUTE-LAST-INTERNAL"
        )
        self.bar_type_4h = BarType.from_str(
            f"{config.instrument_id}-4-HOUR-LAST-INTERNAL"
        )

        # VWAP indicators
        self.vwap_15min = VolumeWeightedAveragePrice()
        self.vwap_4h = VolumeWeightedAveragePrice()

        # Data storage for calculations
        self.bars_15min = []
        self.bars_4h = []
        self.volumes_15min = deque(maxlen=20)  # For volume average calculation

        # Track last VWAP values for crossover detection
        self.last_15min_price = 0.0
        self.last_15min_vwap = 0.0

        # Standard deviation bands for the 15-min timeframe
        self.upper_band_15min = 0.0
        self.lower_band_15min = 0.0

        # Tracking flags
        self.in_position = False
        self.position_side = None
        self.entry_time = None
        self.current_position_id = None

        # Statistics
        self.trades_total = 0
        self.trades_won = 0
        self.trades_lost = 0

    def on_start(self):
        """
        Actions to perform when the strategy starts.
        """
        self.log.info("VWAP Multi-Timeframe Strategy starting...")
        self.instrument = self.cache.instrument(
            InstrumentId.from_str(self.config.instrument_id)
        )
        # Subscribe to 15-minute bars
        self.subscribe_bars(self.bar_type_1min)

        # Subscribe to 4-hour bars (using bar aggregation if needed)
        try:
            # If 4-hour bars need to be created through aggregation from 15-min bars
            bar_type_15min = f"{self.bar_type_15min}@1-MINUTE-EXTERNAL"
            self.subscribe_bars(BarType.from_str(bar_type_15min))
            self.log.info(
                "15-minute bars are not available directly, aggregating from 1-minute bars."
            )
            bar_type_4h = f"{self.bar_type_4h}@1-MINUTE-EXTERNAL"
            self.subscribe_bars(BarType.from_str(bar_type_4h))
            self.log.info(
                "4-hour bars are not available directly, aggregating from 1-minute bars."
            )
        except Exception:
            # If 4-hour bars are available directly
            self.subscribe_bars(self.bar_type_15min)
            self.log.info(
                "15-minute bars are available directly, no aggregation needed."
            )
            self.subscribe_bars(self.bar_type_4h)
            self.log.info("4-hour bars are available directly, no aggregation needed.")
        self.log.info(f"Subscribed to 15-minute bars: {self.bar_type_15min}")
        self.log.info(f"Subscribed to 4-hour bars: {self.bar_type_4h}")

        # Register the VWAP indicators to receive bar data
        self.register_indicator_for_bars(self.bar_type_15min, self.vwap_15min)
        self.register_indicator_for_bars(self.bar_type_4h, self.vwap_4h)

    def on_bar(self, bar: Bar) -> None:
        """
        Actions to perform when a new bar is received.

        Parameters
        ----------
        bar : Bar
            The update bar.
        """
        # Process bar based on timeframe
        if bar.bar_type == self.bar_type_15min:
            self._process_15min_bar(bar)
        elif bar.bar_type == self.bar_type_4h:
            self._process_4h_bar(bar)

    def _process_15min_bar(self, bar: Bar) -> None:
        """
        Process a 15-minute bar update.
        """
        # Store the bar and update volume history
        self.bars_15min.append(bar)
        self.volumes_15min.append(float(bar.volume.as_double()))

        # Current price and VWAP values
        current_price = float(bar.close.as_double())
        self.last_15min_price = current_price

        # Wait until both indicators are initialized
        if not self.vwap_15min.initialized or not self.vwap_4h.initialized:
            self.log.info(
                "Waiting for VWAP indicators to initialize...", color=LogColor.BLUE
            )
            return

        # Store current VWAP values
        current_15min_vwap = self.vwap_15min.value
        current_4h_vwap = self.vwap_4h.value

        # Calculate VWAP standard deviation bands for 15-min timeframe
        if len(self.bars_15min) >= self.config.vwap_period_15min:
            # Calculate standard deviation
            recent_bars = self.bars_15min[-self.config.vwap_period_15min :]
            prices = [
                np.divide(
                    (
                        float(b.high.as_double())
                        + float(b.low.as_double())
                        + float(b.close.as_double())
                    ),
                    3.0,
                )
                for b in recent_bars
            ]
            std_dev = np.std(prices)

            # Set bands
            self.upper_band_15min = current_15min_vwap + (
                std_dev * self.config.std_dev_multiplier
            )
            self.lower_band_15min = current_15min_vwap - (
                std_dev * self.config.std_dev_multiplier
            )

            # Log VWAP and bands
            self.log.info(
                f"15min VWAP: {current_15min_vwap:.5f}, "
                f"Upper band: {self.upper_band_15min:.5f}, "
                f"Lower band: {self.lower_band_15min:.5f}",
                color=LogColor.CYAN,
            )

        # Detect 15-min VWAP crossover (if we have previous values)
        if np.not_equal(self.last_15min_vwap, 0.0):
            # Calculate average volume
            avg_volume = (
                np.divide(sum(self.volumes_15min), len(self.volumes_15min))
                if self.volumes_15min
                else 0.0
            )
            current_volume = float(bar.volume.as_double())
            volume_ratio = (
                np.divide(current_volume, avg_volume)
                if np.not_equal(avg_volume, 0.0)
                else 0.0
            )

            # Log volume analysis
            self.log.info(
                f"Volume: {current_volume:.2f}, Avg Volume: {avg_volume:.2f}, "
                f"Ratio: {volume_ratio:.2f}, Threshold: {self.config.entry_volume_threshold:.2f}",
                color=LogColor.YELLOW,
            )

            # See if we need to exit based on time
            if self.in_position and self.entry_time:
                bar_time = unix_nanos_to_dt(bar.ts_event)
                # Check if position has been open for more than time_exit_hours
                elapsed_time = bar_time - self.entry_time
                if elapsed_time.total_seconds() > (self.config.time_exit_hours * 3600):
                    self.log.info(
                        f"Time-based exit triggered after {elapsed_time.total_seconds()/3600:.1f} hours",
                        color=LogColor.MAGENTA,
                    )
                    self._exit_position()

            # Check if we're in a position for exit signals
            if self.in_position:
                # Exit long position
                if self.position_side == OrderSide.BUY:
                    # If price rises above upper band, take profit
                    if current_price >= self.upper_band_15min:
                        self.log.info(
                            f"Take profit triggered: Price {current_price:.5f} >= Upper band {self.upper_band_15min:.5f}",
                            color=LogColor.GREEN,
                        )
                        self._exit_position()
                    # If price falls below VWAP, stop loss
                    elif current_price < current_15min_vwap:
                        self.log.info(
                            f"Stop loss triggered: Price {current_price:.5f} < VWAP {current_15min_vwap:.5f}",
                            color=LogColor.RED,
                        )
                        self._exit_position()

                # Exit short position
                elif self.position_side == OrderSide.SELL:
                    # If price falls below lower band, take profit
                    if current_price <= self.lower_band_15min:
                        self.log.info(
                            f"Take profit triggered: Price {current_price:.5f} <= Lower band {self.lower_band_15min:.5f}",
                            color=LogColor.GREEN,
                        )
                        self._exit_position()
                    # If price rises above VWAP, stop loss
                    elif current_price > current_15min_vwap:
                        self.log.info(
                            f"Stop loss triggered: Price {current_price:.5f} > VWAP {current_15min_vwap:.5f}",
                            color=LogColor.RED,
                        )
                        self._exit_position()

            # Check for entry signals if we're not in a position
            elif not self.in_position:
                # Uptrend in 4-hour timeframe: current price above 4h VWAP
                uptrend_4h = current_price > current_4h_vwap
                # Downtrend in 4-hour timeframe: current price below 4h VWAP
                downtrend_4h = current_price < current_4h_vwap

                # 15-min price crossing above VWAP
                cross_above = (
                    self.last_15min_price > current_15min_vwap
                    and self.last_15min_price <= self.last_15min_vwap
                )
                # 15-min price crossing below VWAP
                cross_below = (
                    self.last_15min_price < current_15min_vwap
                    and self.last_15min_price >= self.last_15min_vwap
                )

                # Volume is above threshold
                volume_check = volume_ratio >= self.config.entry_volume_threshold

                # Long signal: 4h uptrend + 15min cross above VWAP + high volume
                if uptrend_4h and cross_above and volume_check:
                    self.log.info(
                        "LONG SIGNAL: 4h uptrend + 15min cross above VWAP + high volume",
                        color=LogColor.GREEN,
                    )
                    self._enter_position(OrderSide.BUY, bar)

                # Short signal: 4h downtrend + 15min cross below VWAP + high volume
                elif downtrend_4h and cross_below and volume_check:
                    self.log.info(
                        "SHORT SIGNAL: 4h downtrend + 15min cross below VWAP + high volume",
                        color=LogColor.RED,
                    )
                    self._enter_position(OrderSide.SELL, bar)

        # Update last VWAP value for next comparison
        self.last_15min_vwap = current_15min_vwap

    def _process_4h_bar(self, bar: Bar) -> None:
        """
        Process a 4-hour bar update.
        """
        # Store the bar
        self.bars_4h.append(bar)

        # Log 4-hour VWAP if available
        if self.vwap_4h.initialized:
            self.log.info(
                f"4h VWAP updated: {self.vwap_4h.value:.5f} at {unix_nanos_to_dt(bar.ts_event)}",
                color=LogColor.MAGENTA,
            )

    def _enter_position(self, side: OrderSide, bar: Bar) -> None:
        """
        Enter a new position.

        Parameters
        ----------
        side : OrderSide
            The order side (BUY or SELL).
        bar : Bar
            The current bar.
        """
        if self.in_position:
            self.log.warning("Already in position, cannot enter new position.")
            return

        # Calculate position size based on risk percentage
        account_balance = self.get_account_balance(self.instrument.quote_currency)
        if account_balance is None:
            self.log.error("Unable to determine account balance.")
            return

        current_price = float(bar.close.as_double())

        # Calculate stop loss price
        if side == OrderSide.BUY:
            stop_price = self.lower_band_15min
        else:  # SELL
            stop_price = self.upper_band_15min

        # Calculate risk per trade in currency
        risk_amount = float(account_balance) * self.config.risk_per_trade

        # Calculate position size based on risk
        price_distance = abs(current_price - float(stop_price))
        if price_distance <= 0:
            self.log.error(f"Invalid price distance: {price_distance}. Aborting trade.")
            return

        position_size = np.divide(risk_amount, price_distance)
        position_qty = self.instrument.make_qty(Decimal(str(position_size)))

        # Adjust position size if it's below the minimum lot size
        min_qty = self.instrument.min_quantity
        if position_qty < min_qty:
            position_qty = min_qty
            self.log.warning(
                f"Position size adjusted to minimum quantity: {position_qty}"
            )

        # Create market order for entry
        order = self.order_factory.market(
            instrument_id=self.instrument.id,
            order_side=side,
            quantity=self.instrument.calculate_base_quantity(position_qty, bar.close),
            time_in_force=TimeInForce.GTC,  # Immediate or Cancel
            reduce_only=False,
        )

        # Submit the order
        self.submit_order(order)
        self.log.info(
            f"Submitted {side} order: {order}",
            color=LogColor.GREEN if side == OrderSide.BUY else LogColor.RED,
        )

        # Update tracking variables
        self.in_position = True
        self.position_side = side
        self.entry_time = unix_nanos_to_dt(bar.ts_event)
        self.trades_total += 1

    def _exit_position(self) -> None:
        """
        Exit the current position.
        """
        if not self.in_position or self.position_side is None:
            self.log.warning("No position to exit.")
            return

        # Create opposing market order to close the position
        exit_side = (
            OrderSide.SELL if self.position_side == OrderSide.BUY else OrderSide.BUY
        )

        # Get the current position size
        position = self.portfolio.net_position(self.instrument.id)
        if position == Decimal("0"):
            self.log.warning("No position to exit.")
            # Reset tracking variables anyway
            self.in_position = False
            self.position_side = None
            self.entry_time = None
            self.current_position_id = None
            return
        if exit_side == OrderSide.BUY:
            position = -position
        # Create market order for exit
        order = self.order_factory.market(
            instrument_id=self.instrument.id,
            order_side=exit_side,
            quantity=self.instrument.make_qty(position),
            time_in_force=TimeInForce.GTC,  # Immediate or Cancel
            reduce_only=False,  # Ensure we only reduce position, not open new one
        )

        # Submit the order
        self.submit_order(order)
        self.log.info(
            f"Submitted exit {exit_side} order: {order}", color=LogColor.YELLOW
        )

        # We'll reset tracking variables when we receive the position closed event

    def on_position_opened(self, event: PositionOpened) -> None:
        """
        Callback for position opened event.

        Parameters
        ----------
        event : PositionOpened
            The position opened event.
        """
        if event.instrument_id != self.instrument.id:
            return  # Not our instrument

        self.log.info(f"Position opened: {event}")
        self.current_position_id = event.position_id

    def on_position_closed(self, event: PositionClosed) -> None:
        """
        Callback for position closed event.

        Parameters
        ----------
        event : PositionClosed
            The position closed event.
        """
        if event.instrument_id != self.instrument.id:
            return  # Not our instrument

        self.log.info(f"Position closed: {event}")

        # Check if this is our current position
        if self.current_position_id == event.position_id:
            # Update trade statistics
            if float(event.realized_pnl) >= 0:
                self.trades_won += 1
                self.log.info(
                    f"Trade won: Realized P&L = {event.realized_pnl}",
                    color=LogColor.GREEN,
                )
            else:
                self.trades_lost += 1
                self.log.info(
                    f"Trade lost: Realized P&L = {event.realized_pnl}",
                    color=LogColor.RED,
                )

            # Reset tracking variables
            self.in_position = False
            self.position_side = None
            self.entry_time = None
            self.current_position_id = None

            # Log trade statistics
            win_rate = (
                (self.trades_won / self.trades_total) * 100
                if self.trades_total > 0
                else 0
            )
            self.log.info(
                f"Trade statistics: Won={self.trades_won}, Lost={self.trades_lost}, "
                f"Total={self.trades_total}, Win rate={win_rate:.2f}%",
                color=LogColor.BLUE,
            )

    def get_account_balance(self, currency) -> Optional[Decimal]:
        """
        Get the account balance for the specified currency.

        Parameters
        ----------
        currency : Currency
            The currency to check.

        Returns
        -------
        Optional[Decimal]
            The account balance if available, None otherwise.
        """
        try:
            return self.portfolio.account(Venue("BINANCE")).balance_total()
        except Exception as e:
            self.log.error(f"Error getting account balance: {e}")
            return None

    def on_stop(self) -> None:
        """
        Actions to perform when the strategy stops.
        """
        self.log.info("VWAP Multi-Timeframe Strategy stopped.")

        # Log final statistics
        self.log.info(f"Total trades: {self.trades_total}")
        self.log.info(f"Won trades: {self.trades_won}")
        self.log.info(f"Lost trades: {self.trades_lost}")

        win_rate = (
            (self.trades_won / self.trades_total) * 100 if self.trades_total > 0 else 0
        )
        self.log.info(f"Win rate: {win_rate:.2f}%")
