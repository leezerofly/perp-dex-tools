"""
Modular Trading Bot - Supports multiple exchanges
"""

import os
import time
import asyncio
import traceback
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from exchanges import ExchangeFactory
from exchanges.base import OrderResult
from helpers import TradingLogger
from helpers.lark_bot import LarkBot
from helpers.telegram_bot import TelegramBot


@dataclass
class TradingConfig:
    """Configuration class for trading parameters."""
    ticker: str
    contract_id: str
    quantity: Decimal
    take_profit: Decimal
    tick_size: Decimal
    direction: str
    max_orders: int
    wait_time: int
    exchange: str
    grid_step: Decimal
    stop_price: Decimal
    pause_price: Decimal
    boost_mode: bool
    # extended option: only place one TP and one SL as limit close orders after fill
    tp_sl_only: bool = False
    # leverage multiplier for calculating TP/SL prices
    leverage: Decimal = Decimal('20')  # 默认杠杆 20x

    @property
    def close_order_side(self) -> str:
        """Get the close order side based on bot direction."""
        return 'buy' if self.direction == "sell" else 'sell'


@dataclass
class OrderMonitor:
    """Thread-safe order monitoring state."""
    order_id: Optional[str] = None
    filled: bool = False
    filled_price: Optional[Decimal] = None
    filled_qty: Decimal = 0.0

    def reset(self):
        """Reset the monitor state."""
        self.order_id = None
        self.filled = False
        self.filled_price = None
        self.filled_qty = 0.0


class TradingBot:
    """Modular Trading Bot - Main trading logic supporting multiple exchanges."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self.logger = TradingLogger(config.exchange, config.ticker, log_to_console=True)

        # Create exchange client
        try:
            self.exchange_client = ExchangeFactory.create_exchange(
                config.exchange,
                config
            )
        except ValueError as e:
            raise ValueError(f"Failed to create exchange client: {e}")

        # Trading state
        self.active_close_orders = []
        self.last_close_orders = 0
        self.last_open_order_time = 0
        self.last_log_time = 0
        self.current_order_status = None
        self.order_filled_event = asyncio.Event()
        self.order_canceled_event = asyncio.Event()
        self.shutdown_requested = False
        self.loop = None
        
        # TP/SL order tracking (for OCO logic)
        self.tp_order_id = None
        self.sl_order_id = None

        # Register order callback
        self._setup_websocket_handlers()

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True

        try:
            # Disconnect from exchange
            await self.exchange_client.disconnect()
            self.logger.log("Graceful shutdown completed", "INFO")

        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")

    def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for order updates."""
        def order_update_handler(message):
            """Handle order updates from WebSocket."""
            try:
                # Check if this is for our contract
                if message.get('contract_id') != self.config.contract_id:
                    return

                order_id = message.get('order_id')
                status = message.get('status')
                side = message.get('side', '')
                order_type = message.get('order_type', '')
                filled_size = Decimal(message.get('filled_size'))
                if order_type == "OPEN":
                    self.current_order_status = status

                if status == 'FILLED':
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        # Ensure thread-safe interaction with asyncio event loop
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_filled_event.set)
                        else:
                            # Fallback (should not happen after run() starts)
                            self.order_filled_event.set()

                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")
                    self.logger.log_transaction(order_id, side, message.get('size'), message.get('price'), status)
                elif status == "CANCELED":
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_canceled_event.set)
                        else:
                            self.order_canceled_event.set()

                        if self.order_filled_amount > 0:
                            self.logger.log_transaction(order_id, side, self.order_filled_amount, message.get('price'), status)
                            
                    # PATCH
                    if self.config.exchange == "extended":
                        self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                        f"{Decimal(message.get('size')) - filled_size} @ {message.get('price')}", "INFO")
                    else:
                        self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                        f"{message.get('size')} @ {message.get('price')}", "INFO")
                elif status == "PARTIALLY_FILLED":
                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{filled_size} @ {message.get('price')}", "INFO")
                else:
                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Setup order update handler
        self.exchange_client.setup_order_update_handler(order_update_handler)

    def _calculate_wait_time(self) -> Decimal:
        """Calculate wait time between orders."""
        cool_down_time = self.config.wait_time

        if len(self.active_close_orders) < self.last_close_orders:
            self.last_close_orders = len(self.active_close_orders)
            return 0

        self.last_close_orders = len(self.active_close_orders)
        if len(self.active_close_orders) >= self.config.max_orders:
            return 1

        if len(self.active_close_orders) / self.config.max_orders >= 2/3:
            cool_down_time = 2 * self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/3:
            cool_down_time = self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/6:
            cool_down_time = self.config.wait_time / 2
        else:
            cool_down_time = self.config.wait_time / 4

        # if the program detects active_close_orders during startup, it is necessary to consider cooldown_time
        if self.last_open_order_time == 0 and len(self.active_close_orders) > 0:
            self.last_open_order_time = time.time()

        if time.time() - self.last_open_order_time > cool_down_time:
            return 0
        else:
            return 1

    async def _place_and_monitor_open_order(self) -> bool:
        """Place an order and monitor its execution."""
        try:
            # Reset state before placing order
            self.order_filled_event.clear()
            self.current_order_status = 'OPEN'
            self.order_filled_amount = 0.0

            # Place the order
            order_result = await self.exchange_client.place_open_order(
                self.config.contract_id,
                self.config.quantity,
                self.config.direction
            )

            if not order_result.success:
                return False

            if order_result.status == 'FILLED':
                return await self._handle_order_result(order_result)
            elif not self.order_filled_event.is_set():
                try:
                    await asyncio.wait_for(self.order_filled_event.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass

            # Update order_result with latest status before handling
            # This ensures we have the most current information
            if self.config.exchange == "lighter" and self.exchange_client.current_order:
                # Use WebSocket data if available
                order_result = OrderResult(
                    success=True,
                    order_id=order_result.order_id,
                    side=self.exchange_client.current_order.side,
                    size=self.exchange_client.current_order.size,
                    price=self.exchange_client.current_order.price,
                    status=self.exchange_client.current_order.status,
                    filled_size=self.exchange_client.current_order.filled_size
                )
                self.logger.log(f"[OPEN] 更新订单状态为 {order_result.status}", "DEBUG")
            
            # Handle order result
            return await self._handle_order_result(order_result)

        except Exception as e:
            self.logger.log(f"Error placing order: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return False

    async def _handle_order_result(self, order_result) -> bool:
        """Handle the result of an order placement."""
        order_id = order_result.order_id
        filled_price = order_result.price

        if self.order_filled_event.is_set() or order_result.status == 'FILLED':
            if self.config.boost_mode:
                close_order_result = await self.exchange_client.place_market_order(
                    self.config.contract_id,
                    self.config.quantity,
                    self.config.close_order_side
                )
            else:
                self.last_open_order_time = time.time()
                if getattr(self.config, 'tp_sl_only', False):
                    # Place TP and SL limit close orders with proper order types
                    close_side = self.config.close_order_side
                    
                    self.logger.log(f"[TP/SL] 开始设置止盈止损订单，成交价={filled_price}", "INFO")
                    
                    # 计算价格变动百分比：收益率 / 杠杆
                    # 例如：5% 收益 / 20x 杠杆 = 0.25% 价格变动
                    profit_pct = self.config.take_profit / Decimal(100)
                    leverage = getattr(self.config, 'leverage', Decimal('20'))
                    
                    # 调试日志
                    self.logger.log(f"[DEBUG] config.leverage 类型={type(self.config.leverage)}, 值={self.config.leverage}", "DEBUG")
                    self.logger.log(f"[DEBUG] leverage 变量={leverage}, profit_pct={profit_pct}", "DEBUG")
                    
                    price_change_pct = profit_pct / leverage
                    
                    # 根据开仓方向计算止盈止损价格
                    if self.config.direction == 'buy':
                        # 开多：止盈价上涨，止损价下跌
                        tp_price = filled_price * (1 + price_change_pct)
                        sl_price = filled_price * (1 - price_change_pct)
                    else:
                        # 开空：止盈价下跌，止损价上涨
                        tp_price = filled_price * (1 - price_change_pct)
                        sl_price = filled_price * (1 + price_change_pct)

                    # Round to tick size if available
                    tick = self.config.tick_size or Decimal(0)
                    def round_tick(px: Decimal) -> Decimal:
                        if tick and tick > 0:
                            q = (px / tick).quantize(Decimal('1'))
                            return (q * tick).normalize()
                        return px
                    tp_price = round_tick(tp_price)
                    sl_price = round_tick(sl_price)
                    
                    self.logger.log(f"[TP/SL] 开仓方向={self.config.direction}, 平仓方向={close_side}", "INFO")
                    self.logger.log(f"[TP/SL] 杠杆={leverage}x, 目标收益={self.config.take_profit}%, 价格变动={price_change_pct*100:.4f}%", "INFO")
                    self.logger.log(f"[TP/SL] 开仓价={filled_price}, TP={tp_price} ({((tp_price/filled_price-1)*100):.4f}%), SL={sl_price} ({((sl_price/filled_price-1)*100):.4f}%)", "INFO")

                    # For Lighter exchange, use proper TP/SL order types
                    if self.config.exchange == "lighter":
                        self.logger.log(f"[TP/SL] 准备下止盈订单: {close_side} @ {tp_price} (trigger={tp_price})", "INFO")
                        tp_res = await self.exchange_client.place_limit_order(
                            self.config.contract_id,
                            self.config.quantity,
                            tp_price,
                            close_side,
                            order_type='TAKE_PROFIT_LIMIT',
                            trigger_price=tp_price,
                            post_only=False  # 止盈止损订单不使用 POST_ONLY
                        )
                        if not tp_res.success:
                            self.logger.log(f"[TP] Failed: {tp_res.error_message}", "ERROR")
                            raise Exception(f"[TP] Failed: {tp_res.error_message}")
                        
                        self.logger.log(f"[TP] 止盈订单已下单 ✓ Order ID: {tp_res.order_id} (type={type(tp_res.order_id)})", "INFO")
                        self.tp_order_id = tp_res.order_id  # 保存止盈订单ID
                        
                        # 等待 0.2 秒，确保下一个订单的 client_order_index 不同
                        await asyncio.sleep(0.2)
                        
                        self.logger.log(f"[TP/SL] 准备下止损订单: {close_side} @ {sl_price} (trigger={sl_price})", "INFO")
                        sl_res = await self.exchange_client.place_limit_order(
                            self.config.contract_id,
                            self.config.quantity,
                            sl_price,
                            close_side,
                            order_type='STOP_LOSS_LIMIT',
                            trigger_price=sl_price,
                            post_only=False  # 止盈止损订单不使用 POST_ONLY
                        )
                        if not sl_res.success:
                            self.logger.log(f"[SL] Failed: {sl_res.error_message}", "ERROR")
                            raise Exception(f"[SL] Failed: {sl_res.error_message}")
                        
                        self.logger.log(f"[SL] 止损订单已下单 ✓ Order ID: {sl_res.order_id} (type={type(sl_res.order_id)})", "INFO")
                        self.sl_order_id = sl_res.order_id  # 保存止损订单ID
                        
                        await asyncio.sleep(1)
                    else:
                        # For other exchanges, fallback to regular limit orders
                        tp_res = await self.exchange_client.place_close_order(
                            self.config.contract_id,
                            self.config.quantity,
                            tp_price,
                            close_side
                        )
                        if not tp_res.success:
                            self.logger.log(f"[TP] Failed: {tp_res.error_message}", "ERROR")
                            raise Exception(f"[TP] Failed: {tp_res.error_message}")

                        sl_res = await self.exchange_client.place_close_order(
                            self.config.contract_id,
                            self.config.quantity,
                            sl_price,
                            close_side
                        )
                        if not sl_res.success:
                            self.logger.log(f"[SL] Failed: {sl_res.error_message}", "ERROR")
                            raise Exception(f"[SL] Failed: {sl_res.error_message}")
                    
                    return True
                else:
                    # Place single close order (legacy behavior)
                    close_side = self.config.close_order_side
                    if close_side == 'sell':
                        close_price = filled_price * (1 + self.config.take_profit/100)
                    else:
                        close_price = filled_price * (1 - self.config.take_profit/100)

                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        self.config.quantity,
                        close_price,
                        close_side
                    )
                    if self.config.exchange == "lighter":
                        await asyncio.sleep(1)

                    if not close_order_result.success:
                        self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")
                        raise Exception(f"[CLOSE] Failed to place close order: {close_order_result.error_message}")

                    return True

        else:
            # 订单未成交的处理
            # 如果是 tp_sl_only 模式（对冲机器人），使用有限次数的取消重下
            if getattr(self.config, 'tp_sl_only', False):
                self.logger.log(f"[OPEN] [{order_id}] 订单未在规定时间内成交，开始重试逻辑", "WARNING")
                
                max_retries = 5  # 最多重试5次
                retry_count = 0
                max_price_diff_pct = Decimal('0.5')  # 允许0.5%的价差
                
                while retry_count < max_retries:
                    retry_count += 1
                    self.logger.log(f"[OPEN] 重试 {retry_count}/{max_retries}", "INFO")
                    
                    # 获取当前市场价格
                    new_order_price = await self.exchange_client.get_order_price(self.config.direction)
                    
                    # 检查价差是否在可接受范围内
                    price_diff_pct = abs((new_order_price - order_result.price) / order_result.price * 100)
                    self.logger.log(f"[OPEN] 原价={order_result.price}, 新价={new_order_price}, 价差={price_diff_pct:.4f}%", "INFO")
                    
                    if price_diff_pct > max_price_diff_pct:
                        self.logger.log(f"[OPEN] 价差超过 {max_price_diff_pct}%，停止重试", "WARNING")
                        break
                    
                    # 取消当前订单
                    try:
                        self.logger.log(f"[OPEN] 取消订单 {order_id}", "INFO")
                        cancel_result = await self.exchange_client.cancel_order(order_id)
                        if not cancel_result.success:
                            self.logger.log(f"[OPEN] 取消订单失败: {cancel_result.error_message}", "WARNING")
                            # 可能订单已经成交了，检查一下
                            order_info = await self.exchange_client.get_order_info(order_id)
                            if order_info and order_info.status == 'FILLED':
                                self.logger.log(f"[OPEN] 订单已成交，返回成功", "INFO")
                                # 递归调用自己，处理已成交的订单
                                filled_result = OrderResult(
                                    success=True,
                                    order_id=order_id,
                                    side=order_result.side,
                                    size=order_result.size,
                                    price=order_result.price,
                                    status='FILLED'
                                )
                                return await self._handle_order_result(filled_result)
                    except Exception as e:
                        self.logger.log(f"[OPEN] 取消订单异常: {e}", "ERROR")
                    
                    # 等待一下，确保取消生效
                    await asyncio.sleep(0.5)
                    
                    # 重新下单
                    try:
                        self.logger.log(f"[OPEN] 以新价格 {new_order_price} 重新下单", "INFO")
                        new_order_result = await self.exchange_client.place_limit_order(
                            self.config.contract_id,
                            self.config.quantity,
                            new_order_price,
                            self.config.direction
                        )
                        
                        if not new_order_result.success:
                            self.logger.log(f"[OPEN] 重新下单失败: {new_order_result.error_message}", "ERROR")
                            continue
                        
                        order_id = new_order_result.order_id
                        self.logger.log(f"[OPEN] 新订单已下单，ID={order_id}，等待成交...", "INFO")
                        
                        # 等待10秒看是否成交
                        start_time = time.time()
                        while time.time() - start_time < 10:
                            if self.config.exchange == "lighter":
                                if self.exchange_client.current_order and self.exchange_client.current_order.status == 'FILLED':
                                    self.logger.log(f"[OPEN] 订单已成交 ✓", "INFO")
                                    # 递归调用处理成交订单
                                    filled_result = OrderResult(
                                        success=True,
                                        order_id=order_id,
                                        side=self.config.direction,
                                        size=self.config.quantity,
                                        price=new_order_price,
                                        status='FILLED'
                                    )
                                    return await self._handle_order_result(filled_result)
                            else:
                                order_info = await self.exchange_client.get_order_info(order_id)
                                if order_info and order_info.status == 'FILLED':
                                    self.logger.log(f"[OPEN] 订单已成交 ✓", "INFO")
                                    filled_result = OrderResult(
                                        success=True,
                                        order_id=order_id,
                                        side=self.config.direction,
                                        size=self.config.quantity,
                                        price=new_order_price,
                                        status='FILLED'
                                    )
                                    return await self._handle_order_result(filled_result)
                            
                            await asyncio.sleep(0.5)
                        
                        # 10秒后仍未成交，继续下一次重试
                        order_result = OrderResult(
                            success=True,
                            order_id=order_id,
                            side=self.config.direction,
                            size=self.config.quantity,
                            price=new_order_price,
                            status='OPEN'
                        )
                        
                    except Exception as e:
                        self.logger.log(f"[OPEN] 重新下单异常: {e}", "ERROR")
                        continue
                
                # 重试失败，取消最后的订单并返回失败
                self.logger.log(f"[OPEN] 重试 {max_retries} 次后仍未成交，取消订单并返回失败", "ERROR")
                try:
                    await self.exchange_client.cancel_order(order_id)
                except:
                    pass
                return False
            
            # 普通模式：取消重下逻辑
            new_order_price = await self.exchange_client.get_order_price(self.config.direction)

            def should_wait(direction: str, new_order_price: Decimal, order_result_price: Decimal) -> bool:
                if direction == "buy":
                    return new_order_price <= order_result_price
                elif direction == "sell":
                    return new_order_price >= order_result_price
                return False

            if self.config.exchange == "lighter":
                current_order_status = self.exchange_client.current_order.status
            else:
                order_info = await self.exchange_client.get_order_info(order_id)
                current_order_status = order_info.status

            while (
                should_wait(self.config.direction, new_order_price, order_result.price)
                and current_order_status == "OPEN"
            ):
                self.logger.log(f"[OPEN] [{order_id}] Waiting for order to be filled @ {order_result.price}", "INFO")
                await asyncio.sleep(5)
                if self.config.exchange == "lighter":
                    current_order_status = self.exchange_client.current_order.status
                else:
                    order_info = await self.exchange_client.get_order_info(order_id)
                    if order_info is not None:
                        current_order_status = order_info.status
                new_order_price = await self.exchange_client.get_order_price(self.config.direction)

            self.order_canceled_event.clear()
            # Cancel the order if it's still open
            self.logger.log(f"[OPEN] [{order_id}] Cancelling order and placing a new order", "INFO")
            if self.config.exchange == "lighter":
                cancel_result = await self.exchange_client.cancel_order(order_id)
                start_time = time.time()
                while (time.time() - start_time < 10 and self.exchange_client.current_order.status != 'CANCELED' and
                        self.exchange_client.current_order.status != 'FILLED'):
                    await asyncio.sleep(0.1)

                if self.exchange_client.current_order.status not in ['CANCELED', 'FILLED']:
                    raise Exception(f"[OPEN] Error cancelling order: {self.exchange_client.current_order.status}")
                else:
                    self.order_filled_amount = self.exchange_client.current_order.filled_size
            else:
                try:
                    cancel_result = await self.exchange_client.cancel_order(order_id)
                    if not cancel_result.success:
                        self.order_canceled_event.set()
                        self.logger.log(f"[CLOSE] Failed to cancel order {order_id}: {cancel_result.error_message}", "WARNING")
                    else:
                        self.current_order_status = "CANCELED"

                except Exception as e:
                    self.order_canceled_event.set()
                    self.logger.log(f"[CLOSE] Error canceling order {order_id}: {e}", "ERROR")

                if self.config.exchange == "backpack" or self.config.exchange == "extended":
                    self.order_filled_amount = cancel_result.filled_size
                else:
                    # Wait for cancel event or timeout
                    if not self.order_canceled_event.is_set():
                        try:
                            await asyncio.wait_for(self.order_canceled_event.wait(), timeout=5)
                        except asyncio.TimeoutError:
                            order_info = await self.exchange_client.get_order_info(order_id)
                            self.order_filled_amount = order_info.filled_size

            if self.order_filled_amount > 0:
                close_side = self.config.close_order_side
                if self.config.boost_mode:
                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        self.order_filled_amount,
                        filled_price,
                        close_side
                    )
                else:
                    if close_side == 'sell':
                        close_price = filled_price * (1 + self.config.take_profit/100)
                    else:
                        close_price = filled_price * (1 - self.config.take_profit/100)

                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        self.order_filled_amount,
                        close_price,
                        close_side
                    )
                    if self.config.exchange == "lighter":
                        await asyncio.sleep(1)

                self.last_open_order_time = time.time()
                if not close_order_result.success:
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")

            return True

        return False

    async def _log_status_periodically(self):
        """Log status information periodically, including positions."""
        if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
            print("--------------------------------")
            try:
                # Get active orders
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)

                # Filter close orders
                self.active_close_orders = []
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append({
                            'id': order.order_id,
                            'price': order.price,
                            'size': order.size
                        })

                # Get positions
                position_amt = await self.exchange_client.get_account_positions()

                # Calculate active closing amount
                active_close_amount = sum(
                    Decimal(order.get('size', 0))
                    for order in self.active_close_orders
                    if isinstance(order, dict)
                )

                self.logger.log(f"Current Position: {position_amt} | Active closing amount: {active_close_amount} | "
                                f"Order quantity: {len(self.active_close_orders)}")
                self.last_log_time = time.time()
                # Check for position mismatch
                if abs(position_amt - active_close_amount) > (2 * self.config.quantity):
                    error_message = f"\n\nERROR: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] "
                    error_message += "Position mismatch detected\n"
                    error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    error_message += "Please manually rebalance your position and take-profit orders\n"
                    error_message += "请手动平衡当前仓位和正在关闭的仓位\n"
                    error_message += f"current position: {position_amt} | active closing amount: {active_close_amount} | "f"Order quantity: {len(self.active_close_orders)}\n"
                    error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    self.logger.log(error_message, "ERROR")

                    await self.send_notification(error_message.lstrip())

                    if not self.shutdown_requested:
                        self.shutdown_requested = True

                    mismatch_detected = True
                else:
                    mismatch_detected = False

                return mismatch_detected

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            print("--------------------------------")

    async def _meet_grid_step_condition(self) -> bool:
        if self.active_close_orders:
            picker = min if self.config.direction == "buy" else max
            next_close_order = picker(self.active_close_orders, key=lambda o: o["price"])
            next_close_price = next_close_order["price"]

            best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
            if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                raise ValueError("No bid/ask data available")

            if self.config.direction == "buy":
                new_order_close_price = best_ask * (1 + self.config.take_profit/100)
                if next_close_price / new_order_close_price > 1 + self.config.grid_step/100:
                    return True
                else:
                    return False
            elif self.config.direction == "sell":
                new_order_close_price = best_bid * (1 - self.config.take_profit/100)
                if new_order_close_price / next_close_price > 1 + self.config.grid_step/100:
                    return True
                else:
                    return False
            else:
                raise ValueError(f"Invalid direction: {self.config.direction}")
        else:
            return True

    async def _check_price_condition(self) -> bool:
        stop_trading = False
        pause_trading = False

        if self.config.pause_price == self.config.stop_price == -1:
            return stop_trading, pause_trading

        best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            raise ValueError("No bid/ask data available")

        if self.config.stop_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.stop_price:
                    stop_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.stop_price:
                    stop_trading = True

        if self.config.pause_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.pause_price:
                    pause_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.pause_price:
                    pause_trading = True

        return stop_trading, pause_trading

    async def send_notification(self, message: str):
        lark_token = os.getenv("LARK_TOKEN")
        if lark_token:
            async with LarkBot(lark_token) as lark_bot:
                await lark_bot.send_text(message)

        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if telegram_token and telegram_chat_id:
            with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                tg_bot.send_text(message)

    async def run(self):
        """Main trading loop."""
        try:
            self.config.contract_id, self.config.tick_size = await self.exchange_client.get_contract_attributes()

            # Log current TradingConfig
            self.logger.log("=== Trading Configuration ===", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"Contract ID: {self.config.contract_id}", "INFO")
            self.logger.log(f"Quantity: {self.config.quantity}", "INFO")
            self.logger.log(f"Take Profit: {self.config.take_profit}%", "INFO")
            self.logger.log(f"Direction: {self.config.direction}", "INFO")
            self.logger.log(f"Max Orders: {self.config.max_orders}", "INFO")
            self.logger.log(f"Wait Time: {self.config.wait_time}s", "INFO")
            self.logger.log(f"Exchange: {self.config.exchange}", "INFO")
            self.logger.log(f"Grid Step: {self.config.grid_step}%", "INFO")
            self.logger.log(f"Stop Price: {self.config.stop_price}", "INFO")
            self.logger.log(f"Pause Price: {self.config.pause_price}", "INFO")
            self.logger.log(f"Boost Mode: {self.config.boost_mode}", "INFO")
            self.logger.log("=============================", "INFO")

            # Capture the running event loop for thread-safe callbacks
            self.loop = asyncio.get_running_loop()
            # Connect to exchange
            await self.exchange_client.connect()

            # wait for connection to establish
            await asyncio.sleep(5)

            # Main trading loop
            while not self.shutdown_requested:
                # Update active orders
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)

                # Filter close orders
                self.active_close_orders = []
                has_open_orders = False
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append({
                            'id': order.order_id,
                            'price': order.price,
                            'size': order.size
                        })
                    else:
                        # Has open orders (not close orders)
                        has_open_orders = True

                # If using tp_sl_only mode and already have orders, don't place new ones
                if getattr(self.config, 'tp_sl_only', False):
                    if has_open_orders or len(self.active_close_orders) > 0:
                        # Already have active orders, wait and check again
                        await asyncio.sleep(5)
                        continue
                    elif self.last_close_orders > 0:
                        # 已经完成一轮交易（开仓+平仓），退出
                        self.logger.log("TP/SL only mode: One round completed, exiting", "INFO")
                        await self.graceful_shutdown("TP/SL only mode: One round completed")
                        return

                # Periodic logging
                mismatch_detected = await self._log_status_periodically()

                stop_trading, pause_trading = await self._check_price_condition()
                if stop_trading:
                    msg = f"\n\nWARNING: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] \n"
                    msg += "Stopped trading due to stop price triggered\n"
                    msg += "价格已经达到停止交易价格，脚本将停止交易\n"
                    await self.send_notification(msg.lstrip())
                    await self.graceful_shutdown(msg)
                    continue

                if pause_trading:
                    await asyncio.sleep(5)
                    continue

                if not mismatch_detected:
                    wait_time = self._calculate_wait_time()

                    if wait_time > 0:
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        meet_grid_step_condition = await self._meet_grid_step_condition()
                        if not meet_grid_step_condition:
                            await asyncio.sleep(1)
                            continue

                        await self._place_and_monitor_open_order()
                        self.last_close_orders += 1

        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
            raise
        finally:
            # Ensure all connections are closed even if graceful shutdown fails
            try:
                await self.exchange_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error disconnecting from exchange: {e}", "ERROR")
