"""
Lighter exchange client implementation.
"""

import os
import asyncio
import time
import logging
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger

# Import official Lighter SDK for API client
import lighter
from lighter import SignerClient, ApiClient, Configuration

# Import custom WebSocket implementation
from .lighter_custom_websocket import LighterCustomWebSocketManager

# Suppress Lighter SDK debug logs
logging.getLogger('lighter').setLevel(logging.WARNING)
# Also suppress root logger DEBUG messages that might be coming from Lighter SDK
root_logger = logging.getLogger()
if root_logger.level == logging.DEBUG:
    root_logger.setLevel(logging.WARNING)


class LighterClient(BaseExchangeClient):
    """Lighter exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Lighter client."""
        super().__init__(config)

        # Lighter credentials from environment
        self.api_key_private_key = os.getenv('API_KEY_PRIVATE_KEY')
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX', '0'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX', '0'))
        self.base_url = "https://mainnet.zklighter.elliot.ai"

        if not self.api_key_private_key:
            raise ValueError("API_KEY_PRIVATE_KEY must be set in environment variables")

        # Initialize logger
        self.logger = TradingLogger(exchange="lighter", ticker=self.config.ticker, log_to_console=False)
        self._order_update_handler = None

        # Initialize Lighter client (will be done in connect)
        self.lighter_client = None

        # Initialize API client (will be done in connect)
        self.api_client = None

        # Market configuration
        self.base_amount_multiplier = None
        self.price_multiplier = None
        self.orders_cache = {}
        self.current_order_client_id = None
        self.current_order = None

        # WebSocket控制开关 - 默认禁用，改为使用REST API
        self.use_websocket = os.getenv('USE_WEBSOCKET', 'false').lower() in ['true', '1', 'yes']
        
        # WebSocket-based order tracking (NEW: for event-driven order monitoring)
        self.active_orders_dict = {}  # {order_id: OrderInfo}
        self.order_update_event = asyncio.Event()  # Triggered on any order update
        self.ws_connected = False  # Track WebSocket connection status
        self.last_ws_update_time = 0  # Last time we received a WS update
        self.ws_manager = None  # WebSocket manager instance

    def _validate_config(self) -> None:
        """Validate Lighter configuration."""
        required_env_vars = ['API_KEY_PRIVATE_KEY', 'LIGHTER_ACCOUNT_INDEX', 'LIGHTER_API_KEY_INDEX']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    async def _get_market_config(self, ticker: str) -> Tuple[int, int, int]:
        """Get market configuration for a ticker using official SDK."""
        try:
            # Use shared API client
            order_api = lighter.OrderApi(self.api_client)

            # Get order books to find market info
            order_books = await order_api.order_books()

            for market in order_books.order_books:
                if market.symbol == ticker:
                    market_id = market.market_id
                    base_multiplier = pow(10, market.supported_size_decimals)
                    price_multiplier = pow(10, market.supported_price_decimals)

                    # Store market info for later use
                    self.config.market_info = market

                    self.logger.log(
                        f"Market config for {ticker}: ID={market_id}, "
                        f"Base multiplier={base_multiplier}, Price multiplier={price_multiplier}",
                        "INFO"
                    )
                    return market_id, base_multiplier, price_multiplier

            raise Exception(f"Ticker {ticker} not found in available markets")

        except Exception as e:
            self.logger.log(f"Error getting market config: {e}", "ERROR")
            raise

    async def _initialize_lighter_client(self):
        """Initialize the Lighter client using official SDK."""
        if self.lighter_client is None:
            try:
                self.lighter_client = SignerClient(
                    url=self.base_url,
                    private_key=self.api_key_private_key,
                    account_index=self.account_index,
                    api_key_index=self.api_key_index,
                )

                # Check client
                err = self.lighter_client.check_client()
                if err is not None:
                    raise Exception(f"CheckClient error: {err}")

                self.logger.log("Lighter client initialized successfully", "INFO")
            except Exception as e:
                self.logger.log(f"Failed to initialize Lighter client: {e}", "ERROR")
                raise
        return self.lighter_client

    async def connect(self) -> None:
        """Connect to Lighter."""
        try:
            # Initialize shared API client
            self.api_client = ApiClient(configuration=Configuration(host=self.base_url))

            # Initialize Lighter client
            await self._initialize_lighter_client()

            # 根据配置决定是否启用WebSocket
            if self.use_websocket:
                self.logger.log("✅ WebSocket模式已启用", "INFO")
                
                # Add market config to config for WebSocket manager
                self.config.market_index = self.config.contract_id
                self.config.account_index = self.account_index
                self.config.lighter_client = self.lighter_client

                # Initialize WebSocket manager (using custom implementation)
                self.ws_manager = LighterCustomWebSocketManager(
                    config=self.config,
                    order_update_callback=self._handle_websocket_order_update
                )

                # Set logger for WebSocket manager
                self.ws_manager.set_logger(self.logger)

                # Start WebSocket connection in background task
                asyncio.create_task(self.ws_manager.connect())
                # Wait a moment for connection to establish
                await asyncio.sleep(2)
                
                # Mark WebSocket as connected
                self.ws_connected = True
                self.last_ws_update_time = time.time()
            else:
                self.logger.log("🔄 REST API模式已启用（WebSocket已禁用）", "INFO")
                self.ws_connected = False
                self.ws_manager = None

        except Exception as e:
            self.logger.log(f"Error connecting to Lighter: {e}", "ERROR")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Lighter."""
        try:
            if hasattr(self, 'ws_manager') and self.ws_manager:
                await self.ws_manager.disconnect()

            # Close shared API client
            if self.api_client:
                await self.api_client.close()
                self.api_client = None
        except Exception as e:
            self.logger.log(f"Error during Lighter disconnect: {e}", "ERROR")

    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "lighter"

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        self._order_update_handler = handler

    def _handle_websocket_order_update(self, order_data_list: List[Dict[str, Any]]):
        """Handle order updates from WebSocket."""
        # Update last WS update time
        self.last_ws_update_time = time.time()
        
        for order_data in order_data_list:
            if order_data['market_index'] != self.config.contract_id:
                continue

            side = 'sell' if order_data['is_ask'] else 'buy'
            if side == self.config.close_order_side:
                order_type = "CLOSE"
            else:
                order_type = "OPEN"

            order_id = str(order_data['order_index'])
            status = order_data['status'].upper()
            filled_size = Decimal(order_data['filled_base_amount'])
            size = Decimal(order_data['initial_base_amount'])
            price = Decimal(order_data['price'])
            remaining_size = Decimal(order_data['remaining_base_amount'])

            # === NEW: Update active_orders_dict for event-driven monitoring ===
            # Do this BEFORE the continue check so events are always triggered
            order_state_changed = False
            
            if status in ['FILLED', 'CANCELED', 'CANCELLED']:
                # Remove completed orders
                if order_id in self.active_orders_dict:
                    del self.active_orders_dict[order_id]
                    order_state_changed = True  # Order completed - trigger event
            else:
                # Check if this is a real update
                if order_id in self.active_orders_dict:
                    old_order = self.active_orders_dict[order_id]
                    # Only trigger if status or filled_size changed
                    if (old_order.status != status or 
                        old_order.filled_size != filled_size):
                        order_state_changed = True
                else:
                    # New order - always trigger
                    order_state_changed = True
                
                # Update or add active order
                self.active_orders_dict[order_id] = OrderInfo(
                    order_id=order_id,
                    side=side,
                    size=remaining_size,
                    price=price,
                    status=status,
                    filled_size=filled_size,
                    remaining_size=remaining_size
                )
            
            # Trigger event to notify waiting tasks (BEFORE continue check!)
            if order_state_changed:
                self.logger.log(f"⚡ 订单状态变化，触发WebSocket事件通知", "DEBUG")
                self.order_update_event.set()
                self.order_update_event.clear()  # Reset for next update

            # === Original cache logic (keep for backward compatibility) ===
            if order_id in self.orders_cache.keys():
                if (self.orders_cache[order_id]['status'] == 'OPEN' and
                        status == 'OPEN' and
                        filled_size == self.orders_cache[order_id]['filled_size']):
                    continue  # Skip logging for unchanged orders
                elif status in ['FILLED', 'CANCELED']:
                    del self.orders_cache[order_id]
                else:
                    self.orders_cache[order_id]['status'] = status
                    self.orders_cache[order_id]['filled_size'] = filled_size
            elif status == 'OPEN':
                self.orders_cache[order_id] = {'status': status, 'filled_size': filled_size}

            if status == 'OPEN' and filled_size > 0:
                status = 'PARTIALLY_FILLED'

            if status == 'OPEN':
                self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                f"{size} @ {price}", "INFO")
            else:
                self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                f"{filled_size} @ {price}", "INFO")

            # Only update current_order if the client_order_index matches
            # This ensures we track the correct order we're waiting for
            if order_data['client_order_index'] == self.current_order_client_id:
                current_order = OrderInfo(
                    order_id=order_id,
                    side=side,
                    size=size,
                    price=price,
                    status=status,
                    filled_size=filled_size,
                    remaining_size=remaining_size,
                    cancel_reason=''
                )
                self.current_order = current_order
                self.logger.log(f"[WS] 匹配订单更新: client_index={order_data['client_order_index']}, order_id={order_id}, status={status}", "DEBUG")

            if status in ['FILLED', 'CANCELED']:
                self.logger.log_transaction(order_id, side, filled_size, price, status)

    @query_retry(default_return=(0, 0))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        """Get orderbook - 优先使用WebSocket，降级到REST API."""
        # 优先使用 WebSocket 数据（如果已启用且可用）
        if (self.use_websocket and 
            hasattr(self, 'ws_manager') and self.ws_manager and
            self.ws_manager.best_bid and self.ws_manager.best_ask):
            best_bid = Decimal(str(self.ws_manager.best_bid))
            best_ask = Decimal(str(self.ws_manager.best_ask))

            if best_bid > 0 and best_ask > 0 and best_bid < best_ask:
                return best_bid, best_ask
            else:
                self.logger.log("WebSocket价格无效，降级到REST API", "WARNING")
        
        # 降级到 REST API 获取盘口价格
        try:
            order_api = lighter.OrderApi(self.api_client)
            # 使用 order_book_orders 获取实时盘口订单（bids 和 asks）
            orderbook = await order_api.order_book_orders(market_id=contract_id, limit=1)
            
            if not orderbook:
                self.logger.log("无法从REST API获取盘口数据", "ERROR")
                raise ValueError("No orderbook data available from REST API")
            
            # 获取最优买卖价（第一个订单就是最优价格）
            best_bid = Decimal('0')
            best_ask = Decimal('0')
            
            if orderbook.bids and len(orderbook.bids) > 0:
                best_bid = Decimal(str(orderbook.bids[0].price))
            
            if orderbook.asks and len(orderbook.asks) > 0:
                best_ask = Decimal(str(orderbook.asks[0].price))
            
            if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                self.logger.log(f"REST API返回无效价格: bid={best_bid}, ask={best_ask}", "ERROR")
                raise ValueError("Invalid bid/ask prices from REST API")
            
            self.logger.log(f"📡 REST API盘口: bid={best_bid}, ask={best_ask}", "DEBUG")
            return best_bid, best_ask
            
        except Exception as e:
            self.logger.log(f"REST API获取盘口价格失败: {e}", "ERROR")
            raise

    async def _submit_order_with_retry(self, order_params: Dict[str, Any]) -> OrderResult:
        """Submit an order with Lighter using official SDK."""
        # Ensure client is initialized
        if self.lighter_client is None:
            # This is a sync method, so we need to handle this differently
            # For now, raise an error if client is not initialized
            raise ValueError("Lighter client not initialized. Call connect() first.")

        # Reset current_order before submitting to avoid stale data
        self.current_order = None
        client_order_index = order_params['client_order_index']
        self.current_order_client_id = client_order_index

        # Create order using official SDK
        create_order, tx_hash, error = await self.lighter_client.create_order(**order_params)
        if error is not None:
            return OrderResult(
                success=False, order_id=str(client_order_index),
                error_message=f"Order creation error: {error}")

        else:
            # Wait for WebSocket to return the real order_index
            start_time = time.time()
            real_order_id = None
            
            # Wait up to 5 seconds for WebSocket order update
            while time.time() - start_time < 5:
                await asyncio.sleep(0.1)
                if (self.current_order is not None and 
                    self.current_order_client_id == client_order_index):
                    real_order_id = self.current_order.order_id
                    self.logger.log(f"[ORDER] 成功获取真实订单 ID: {real_order_id} (client_index={client_order_index})", "DEBUG")
                    break
            
            # If we got the real order ID from WebSocket, use it; otherwise use client_order_index
            if real_order_id:
                return OrderResult(success=True, order_id=str(real_order_id))
            else:
                self.logger.log(f"[WARNING] 未能从 WebSocket 获取真实订单 ID，使用 client_order_index: {client_order_index}", "WARNING")
                return OrderResult(success=True, order_id=str(client_order_index))

    async def place_limit_order(self, contract_id: str, quantity: Decimal, price: Decimal,
                                side: str, order_type: str = 'LIMIT', trigger_price: Decimal = Decimal('0'),
                                post_only: bool = False, reduce_only: bool = False) -> OrderResult:
        """Place a limit order with Lighter using official SDK.
        
        Args:
            contract_id: Market contract ID
            quantity: Order quantity
            price: Limit price (for LIMIT orders) or fallback price (for market TP/SL orders)
            side: 'buy' or 'sell'
            order_type: 'LIMIT', 'TAKE_PROFIT_LIMIT', 'STOP_LOSS_LIMIT', 'TAKE_PROFIT', or 'STOP_LOSS'
            trigger_price: Trigger price for TP/SL orders
            post_only: If True, order will only be placed as maker (default: True)
            reduce_only: If True, order will only reduce position (default: False)
        """
        # Ensure client is initialized
        if self.lighter_client is None:
            await self._initialize_lighter_client()

        # Determine order side and price
        if side.lower() == 'buy':
            is_ask = False
        elif side.lower() == 'sell':
            is_ask = True
        else:
            raise Exception(f"Invalid side: {side}")

        # Generate unique client order index
        client_order_index = int(time.time() * 1000) % 1000000  # Simple unique ID
        # Note: current_order_client_id will be set in _submit_order_with_retry

        # Determine order type constant
        if order_type == 'TAKE_PROFIT_LIMIT':
            ot = self.lighter_client.ORDER_TYPE_TAKE_PROFIT_LIMIT
        elif order_type == 'STOP_LOSS_LIMIT':
            ot = self.lighter_client.ORDER_TYPE_STOP_LOSS_LIMIT
        elif order_type == 'TAKE_PROFIT':
            ot = self.lighter_client.ORDER_TYPE_TAKE_PROFIT
        elif order_type == 'STOP_LOSS':
            ot = self.lighter_client.ORDER_TYPE_STOP_LOSS
        else:
            ot = self.lighter_client.ORDER_TYPE_LIMIT

        # Determine time_in_force and order_expiry based on order type and post_only setting
        # 完全按照SDK示例设置参数
        if order_type in ['STOP_LOSS', 'TAKE_PROFIT']:
            # 市价止盈止损：按照SDK的create_tp_order和create_sl_order示例
            tif = self.lighter_client.DEFAULT_IOC_EXPIRY  # 0 (IOC)
            order_expiry = self.lighter_client.DEFAULT_28_DAY_ORDER_EXPIRY  # -1
            self.logger.log(f"[ORDER] 市价止盈止损：time_in_force=IOC(0), order_expiry=28天(-1)", "DEBUG")
        elif order_type in ['STOP_LOSS_LIMIT', 'TAKE_PROFIT_LIMIT']:
            # 限价止盈止损：按照SDK的create_tp_limit_order和create_sl_limit_order示例
            tif = self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME  # 1 (GTT)
            order_expiry = self.lighter_client.DEFAULT_28_DAY_ORDER_EXPIRY  # -1
            self.logger.log(f"[ORDER] 限价止盈止损：time_in_force=GTT(1), order_expiry=28天(-1)", "DEBUG")
        elif post_only and order_type == 'LIMIT':
            # 普通限价单POST_ONLY
            tif = self.lighter_client.ORDER_TIME_IN_FORCE_POST_ONLY  # 2
            order_expiry = self.lighter_client.DEFAULT_28_DAY_ORDER_EXPIRY  # -1
            self.logger.log(f"[ORDER] 使用 POST_ONLY 模式", "DEBUG")
        else:
            # 其他普通订单
            tif = self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME  # 1
            order_expiry = self.lighter_client.DEFAULT_28_DAY_ORDER_EXPIRY  # -1
            self.logger.log(f"[ORDER] 使用 GOOD_TILL_TIME 模式", "DEBUG")
        
        # Create order parameters
        order_params = {
            'market_index': self.config.contract_id,
            'client_order_index': client_order_index,
            'base_amount': int(quantity * self.base_amount_multiplier),
            'price': int(price * self.price_multiplier),
            'is_ask': is_ask,
            'order_type': ot,
            'time_in_force': tif,
            'reduce_only': reduce_only,
            'trigger_price': int(trigger_price * self.price_multiplier) if trigger_price > 0 else 0,
            'order_expiry': order_expiry,
        }

        order_result = await self._submit_order_with_retry(order_params)
        return order_result

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place an open order with Lighter - 优先WebSocket，降级到REST API."""

        # Note: current_order and current_order_client_id will be reset in _submit_order_with_retry
        order_price = await self.get_order_price(direction)

        order_price = self.round_to_tick(order_price)
        order_result = await self.place_limit_order(contract_id, quantity, order_price, direction)
        if not order_result.success:
            raise Exception(f"[OPEN] Error placing order: {order_result.error_message}")

        # Wait for order to be filled (up to 10 seconds)
        start_time = time.time()
        order_status = 'OPEN'
        max_wait = 10
        
        self.logger.log(f"[OPEN] 等待订单成交，订单ID={order_result.order_id}", "DEBUG")
        
        while time.time() - start_time < max_wait:
            # 优先通过 WebSocket 检查订单状态（如果启用）
            if self.use_websocket and self.current_order and self.current_order.status == 'FILLED':
                order_status = 'FILLED'
                self.logger.log(f"[OPEN] WebSocket 检测到订单已成交", "DEBUG")
                break
            
            # 降级使用 REST API 检查订单状态
            order_info = await self.get_order_info(order_result.order_id)
            if order_info and order_info.status == 'FILLED':
                order_status = 'FILLED'
                self.logger.log(f"[OPEN] REST API 检测到订单已成交", "DEBUG")
                break
            
            await asyncio.sleep(0.5)
        
        # 如果10秒后仍未成交，获取最终状态
        if order_status != 'FILLED':
            order_info = await self.get_order_info(order_result.order_id)
            if order_info:
                order_status = order_info.status
                self.logger.log(f"[OPEN] 10秒后订单状态={order_status}", "WARNING")
            else:
                # 如果REST API也无法获取状态，使用WebSocket状态（如果有）
                if self.use_websocket and self.current_order:
                    order_status = self.current_order.status
                    self.logger.log(f"[OPEN] 使用WebSocket状态={order_status}", "WARNING")
                else:
                    order_status = 'OPEN'
                    self.logger.log(f"[OPEN] 无法获取订单状态，使用默认状态={order_status}", "WARNING")

        self.logger.log(f"[OPEN] 最终状态={order_status}，将传递给 _handle_order_result", "DEBUG")
        
        return OrderResult(
            success=True,
            order_id=order_result.order_id,
            side=direction,
            size=quantity,
            price=order_price,
            status=order_status
        )

    async def _get_active_close_orders(self, contract_id: str) -> int:
        """Get active close orders for a contract using official SDK."""
        active_orders = await self.get_active_orders(contract_id)
        active_close_orders = 0
        for order in active_orders:
            if order.side == self.config.close_order_side:
                active_close_orders += 1
        return active_close_orders

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a close order with Lighter using official SDK."""
        self.current_order = None
        self.current_order_client_id = None
        order_result = await self.place_limit_order(contract_id, quantity, price, side)

        # wait for 5 seconds to ensure order is placed
        await asyncio.sleep(5)
        if order_result.success:
            return OrderResult(
                success=True,
                order_id=order_result.order_id,
                side=side,
                size=quantity,
                price=price,
                status='OPEN'
            )
        else:
            raise Exception(f"[CLOSE] Error placing order: {order_result.error_message}")
    
    async def get_order_price(self, side: str = '') -> Decimal:
        """Get the price of an order with Lighter using official SDK."""
        # Get current market prices
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            self.logger.log("Invalid bid/ask prices", "ERROR")
            raise ValueError("Invalid bid/ask prices")

        order_price = (best_bid + best_ask) / 2

        active_orders = await self.get_active_orders(self.config.contract_id)
        close_orders = [order for order in active_orders if order.side == self.config.close_order_side]
        for order in close_orders:
            if side == 'buy':
                order_price = min(order_price, order.price - self.config.tick_size)
            else:
                order_price = max(order_price, order.price + self.config.tick_size)

        return order_price

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order with Lighter."""
        # Ensure client is initialized
        if self.lighter_client is None:
            await self._initialize_lighter_client()

        # Cancel order using official SDK
        cancel_order, tx_hash, error = await self.lighter_client.cancel_order(
            market_index=self.config.contract_id,
            order_index=int(order_id)  # Assuming order_id is the order index
        )

        if error is not None:
            return OrderResult(success=False, error_message=f"Cancel order error: {error}")

        if tx_hash:
            return OrderResult(success=True)
        else:
            return OrderResult(success=False, error_message='Failed to send cancellation transaction')
    
    async def cancel_all_orders(self) -> OrderResult:
        """Cancel all orders for the account (tx_type=16).
        
        根据 Lighter SDK 和 UI 请求示例：
        - time_in_force: 0 (CANCEL_ALL_TIF_IMMEDIATE)
        - time: 0
        """
        try:
            # Ensure client is initialized
            if self.lighter_client is None:
                await self._initialize_lighter_client()
            
            self.logger.log("🗑️ 开始取消所有订单...", "INFO")
            
            # Use lighter SDK's cancel_all_orders method
            # This corresponds to tx_type=16
            # Parameters based on SDK constants and UI example:
            # - time_in_force=0 (CANCEL_ALL_TIF_IMMEDIATE)
            # - time=0
            tx_info, api_response, error = await self.lighter_client.cancel_all_orders(
                time_in_force=0,  # CANCEL_ALL_TIF_IMMEDIATE
                time=0
            )
            
            if error is not None:
                self.logger.log(f"❌ 取消所有订单失败: {error}", "ERROR")
                return OrderResult(success=False, error_message=f"Cancel all orders error: {error}")
            
            if api_response and api_response.code == 200:
                self.logger.log(f"✅ 所有订单已取消", "INFO")
                # Wait a moment for cancellations to be processed
                await asyncio.sleep(1)
                return OrderResult(success=True)
            else:
                error_code = api_response.code if api_response else 'None'
                error_msg = api_response.message if api_response and hasattr(api_response, 'message') else 'No message'
                self.logger.log(
                    f"❌ 取消订单失败: code={error_code}, message={error_msg}",
                    "ERROR"
                )
                return OrderResult(success=False, error_message=f"code={error_code}, msg={error_msg}")
                
        except Exception as e:
            self.logger.log(f"❌ 取消所有订单异常: {e}", "ERROR")
            import traceback
            self.logger.log(f"详细错误: {traceback.format_exc()}", "ERROR")
            return OrderResult(success=False, error_message=str(e))
    
    async def close_all_positions(self) -> OrderResult:
        """Close all positions by creating market reduce-only orders."""
        try:
            # Ensure client is initialized
            if self.lighter_client is None:
                await self._initialize_lighter_client()
            
            self.logger.log("🔄 开始平仓所有持仓...", "INFO")
            
            # Get current positions
            positions = await self._fetch_positions_with_retry()
            
            closed_positions = 0
            actual_positions = []
            
            # First, filter out real positions (not just orders)
            for position in positions:
                position_amount = float(getattr(position, 'position', 0))
                
                if abs(position_amount) < 0.00001:  # Skip near-zero positions
                    continue
                
                market_id = getattr(position, 'market_id', None)
                if market_id is None:
                    continue
                
                actual_positions.append((position, position_amount, market_id))
            
            if len(actual_positions) == 0:
                self.logger.log("ℹ️ 没有实际持仓（可能只有挂单）", "INFO")
                return OrderResult(success=True)
            
            self.logger.log(f"📊 检测到 {len(actual_positions)} 个实际持仓，开始平仓", "INFO")
            
            for pos_tuple in actual_positions:
                position, position_amount, market_id = pos_tuple
                
                # 使用sign字段判断持仓方向（重要！position_amount已经是绝对值）
                # sign > 0 表示多头持仓，需要卖出平仓（is_ask=True）
                # sign <= 0 表示空头持仓，需要买入平仓（is_ask=False）
                position_sign = getattr(position, 'sign', 1)
                is_ask = position_sign > 0
                base_amount = abs(position_amount)
                
                # 检查持仓是否太小（可能是"僵尸持仓"无法平掉）
                if base_amount < 0.0001:
                    self.logger.log(
                        f"⚠️ 持仓太小，跳过: 市场={market_id}, 数量={base_amount:.8f}",
                        "WARNING"
                    )
                    continue
                
                self.logger.log(
                    f"📊 持仓详情: 市场={market_id}, symbol={getattr(position, 'symbol', 'unknown')}, "
                    f"sign={position_sign}, 方向={'多' if is_ask else '空'}, 数量={base_amount:.8f}, "
                    f"平仓方向={'SELL' if is_ask else 'BUY'}",
                    "INFO"
                )
                
                # Get market config for this market
                market_info = await self._get_market_info(market_id)
                if not market_info:
                    self.logger.log(f"⚠️ 无法获取市场 {market_id} 的配置，跳过", "WARNING")
                    continue
                
                base_multiplier = pow(10, market_info.supported_size_decimals)
                price_multiplier = pow(10, market_info.supported_price_decimals)
                
                # Create market reduce-only order (tx_type=14)
                # Based on SDK's create_market_order and UI example:
                # - Type=1 (ORDER_TYPE_MARKET)
                # - TimeInForce=0 (ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL)
                # - ReduceOnly=1
                # - OrderExpiry=0 (DEFAULT_IOC_EXPIRY)
                client_order_index = int(time.time() * 1000) % 1000000
                
                self.logger.log(
                    f"📤 平仓订单: 市场={market_id}, 方向={'SELL' if is_ask else 'BUY'}, "
                    f"数量={base_amount:.8f}",
                    "INFO"
                )
                
                # 使用SDK自带的市价限滑点单平仓（方向判断已修复）
                actual_base_amount = int(base_amount * base_multiplier)
                max_slippage = 0.01  # 1% 滑点
                
                self.logger.log(
                    f"🔧 使用市价限滑点单: market={market_id}, base_amount={actual_base_amount} ({base_amount:.8f}), "
                    f"max_slippage=1%, 方向={'SELL' if is_ask else 'BUY'}, reduce_only=True",
                    "INFO"
                )
                
                # Use SDK's create_market_order_limited_slippage
                create_order, api_response, error = await self.lighter_client.create_market_order_limited_slippage(
                    market_index=market_id,
                    client_order_index=client_order_index,
                    base_amount=actual_base_amount,
                    max_slippage=max_slippage,
                    is_ask=is_ask,
                    reduce_only=True
                )
                
                # Log detailed response
                if api_response:
                    self.logger.log(
                        f"📡 API响应: code={api_response.code}, "
                        f"error={error if error else 'None'}",
                        "INFO"
                    )
                    if hasattr(api_response, 'message'):
                        self.logger.log(f"   message: {api_response.message}", "INFO")
                else:
                    self.logger.log(f"📡 API响应为空, error={error}", "ERROR")
                
                if error is not None:
                    self.logger.log(f"❌ 平仓订单失败: {error}", "ERROR")
                    continue
                
                if api_response and api_response.code == 200:
                    self.logger.log(f"✅ 平仓订单已提交（响应200）", "INFO")
                    closed_positions += 1
                else:
                    error_code = api_response.code if api_response else 'None'
                    error_msg = api_response.message if api_response and hasattr(api_response, 'message') else 'No message'
                    self.logger.log(
                        f"⚠️ 平仓订单提交失败（响应码: {error_code}, 消息: {error_msg}）", 
                        "WARNING"
                    )
            
            if closed_positions > 0:
                self.logger.log(f"✅ 已提交 {closed_positions} 个平仓订单", "INFO")
                
                # Wait longer for orders to be processed and filled
                # BaseAmount=0 needs more time for system to process
                self.logger.log("⏳ 等待平仓订单成交（最多5秒）...", "INFO")
                await asyncio.sleep(5)
                
                # Verify closure by checking positions again
                self.logger.log("🔍 验证平仓结果...", "INFO")
                verify_positions = await self._fetch_positions_with_retry()
                remaining = 0
                remaining_positions = []
                
                for position in verify_positions:
                    position_amount = abs(float(getattr(position, 'position', 0)))
                    if position_amount > 0.00001:
                        remaining += 1
                        market_id = getattr(position, 'market_id', 'unknown')
                        # 使用sign字段判断方向，而不是position的正负
                        position_sign = getattr(position, 'sign', 1)
                        remaining_positions.append({
                            'market_id': market_id,
                            'amount': position_amount,
                            'is_ask': position_sign > 0  # sign > 0 = 多头 = 卖出平仓
                        })
                        self.logger.log(
                            f"⚠️ 持仓未完全平掉: 市场={market_id}, 剩余={position_amount:.8f}",
                            "WARNING"
                        )
                
                # Multiple retry attempts (up to 3 times)
                max_retries = 3
                for retry_attempt in range(max_retries):
                    if remaining == 0:
                        break
                    
                    self.logger.log(
                        f"⚠️ 仍有 {remaining} 个持仓未完全平掉，尝试第{retry_attempt + 1}次重试平仓...",
                        "WARNING"
                    )
                    
                    # Retry closing remaining positions
                    retry_closed = 0
                    for pos_info in remaining_positions:
                        market_id = pos_info['market_id']
                        position_amount = pos_info['amount']
                        is_ask = pos_info['is_ask']
                        
                        # Get market info
                        market_info = await self._get_market_info(market_id)
                        if not market_info:
                            continue
                        
                        base_multiplier = pow(10, market_info.supported_size_decimals)
                        price_multiplier = pow(10, market_info.supported_price_decimals)
                        
                        client_order_index = int(time.time() * 1000) % 1000000
                        
                        # Use actual position amount for retry
                        actual_base_amount = int(position_amount * base_multiplier)
                        
                        # 重试时使用更大的滑点：2%
                        retry_max_slippage = 0.02
                        
                        self.logger.log(
                            f"🔄 重试平仓(市价限滑点单): 市场={market_id}, 方向={'SELL' if is_ask else 'BUY'}, "
                            f"数量={position_amount:.8f}, max_slippage=2%",
                            "INFO"
                        )
                        
                        # Use SDK's create_market_order_limited_slippage for retry
                        create_order, api_response, error = await self.lighter_client.create_market_order_limited_slippage(
                            market_index=market_id,
                            client_order_index=client_order_index,
                            base_amount=actual_base_amount,
                            max_slippage=retry_max_slippage,
                            is_ask=is_ask,
                            reduce_only=True
                        )
                        
                        # 详细记录重试的API响应
                        if api_response:
                            self.logger.log(
                                f"📡 重试API响应: code={api_response.code}, error={error if error else 'None'}",
                                "INFO"
                            )
                            if hasattr(api_response, 'message') and api_response.message:
                                self.logger.log(f"   message: {api_response.message}", "INFO")
                        else:
                            self.logger.log(f"📡 重试API响应为空, error={error}", "ERROR")
                        
                        if error is None and api_response and api_response.code == 200:
                            retry_closed += 1
                            self.logger.log(f"✅ 重试订单提交成功", "INFO")
                        else:
                            error_code = api_response.code if api_response else 'None'
                            error_msg = api_response.message if api_response and hasattr(api_response, 'message') else 'No message'
                            self.logger.log(
                                f"❌ 重试订单失败: code={error_code}, msg={error_msg}, error={error}",
                                "ERROR"
                            )
                    
                    if retry_closed > 0:
                        self.logger.log(f"✅ 重试提交了 {retry_closed} 个平仓订单", "INFO")
                        self.logger.log("⏳ 等待重试订单成交（最多5秒）...", "INFO")
                        await asyncio.sleep(5)
                        
                        # Re-verify after retry
                        self.logger.log("🔍 重新验证平仓结果...", "INFO")
                        verify_positions = await self._fetch_positions_with_retry()
                        remaining = 0
                        remaining_positions = []
                        
                        for position in verify_positions:
                            position_amount = abs(float(getattr(position, 'position', 0)))
                            if position_amount > 0.00001:
                                remaining += 1
                                market_id = getattr(position, 'market_id', 'unknown')
                                # 使用sign字段判断方向
                                position_sign = getattr(position, 'sign', 1)
                                remaining_positions.append({
                                    'market_id': market_id,
                                    'amount': position_amount,
                                    'is_ask': position_sign > 0  # sign > 0 = 多头 = 卖出平仓
                                })
                                self.logger.log(
                                    f"⚠️ 重试后仍有持仓: 市场={market_id}, 剩余={position_amount:.8f}",
                                    "WARNING"
                                )
                    else:
                        # No orders submitted in retry, break retry loop
                        break
                
                # After all retries, check final result
                if remaining > 0:
                    self.logger.log(
                        f"❌ 平仓失败：经过{max_retries}次重试后仍有 {remaining} 个持仓未平掉",
                        "ERROR"
                    )
                    # Return FAILURE to prevent opening new positions
                    return OrderResult(
                        success=False,
                        error_message=f"Failed to close {remaining} positions after {max_retries} retries"
                    )
                else:
                    self.logger.log("✅ 所有持仓已完全平掉", "INFO")
                    return OrderResult(success=True)
            else:
                self.logger.log("ℹ️ 没有需要平仓的持仓", "INFO")
                return OrderResult(success=True)
                
        except Exception as e:
            self.logger.log(f"❌ 平仓所有持仓异常: {e}", "ERROR")
            import traceback
            self.logger.log(f"详细错误: {traceback.format_exc()}", "ERROR")
            return OrderResult(success=False, error_message=str(e))
    
    async def _get_market_info(self, market_id: int):
        """Get market information for a given market ID."""
        try:
            order_api = lighter.OrderApi(self.api_client)
            order_books = await order_api.order_books()
            
            for market in order_books.order_books:
                if market.market_id == market_id:
                    return market
            
            return None
        except Exception as e:
            self.logger.log(f"获取市场信息失败: {e}", "ERROR")
            return None

    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information from Lighter - 优先WebSocket，降级到REST API."""
        try:
            # 优先使用 WebSocket 缓存数据（如果启用）
            if self.use_websocket and order_id in self.active_orders_dict:
                order_info = self.active_orders_dict[order_id]
                self.logger.log(f"📡 从WebSocket缓存获取订单 {order_id} 状态={order_info.status}", "DEBUG")
                return order_info
            
            # 降级到 REST API：获取活动订单
            order_list = await self._fetch_orders_with_retry()
            
            for order in order_list:
                if str(order.order_index) == str(order_id):
                    side = "sell" if order.is_ask else "buy"
                    order_info = OrderInfo(
                        order_id=str(order.order_index),
                        side=side,
                        size=Decimal(order.initial_base_amount),
                        price=Decimal(order.price),
                        status=order.status.upper(),
                        filled_size=Decimal(order.filled_base_amount),
                        remaining_size=Decimal(order.remaining_base_amount)
                    )
                    self.logger.log(f"📡 从REST API获取订单 {order_id} 状态={order_info.status}", "DEBUG")
                    
                    # 更新到缓存（如果启用WebSocket）
                    if self.use_websocket:
                        self.active_orders_dict[order_id] = order_info
                    
                    return order_info
            
            # 如果在活动订单中没找到，可能已经成交或取消
            # 检查账户持仓来判断是否已成交
            account_api = lighter.AccountApi(self.api_client)
            account_data = await account_api.account(by="index", value=str(self.account_index))

            if account_data and account_data.accounts:
                for position in account_data.accounts[0].positions:
                    if position.symbol == self.config.ticker:
                        position_amt = abs(float(position.position))
                        if position_amt > 0.001:
                            # 使用sign字段判断方向
                            position_sign = getattr(position, 'sign', 1)
                            # 有持仓，说明订单可能已成交
                            return OrderInfo(
                                order_id=order_id,
                                side="buy" if position_sign > 0 else "sell",
                                size=Decimal(str(position_amt)),
                                price=Decimal(str(position.avg_entry_price)),  # ✅ 修复：使用正确的属性名
                                status="FILLED",
                                filled_size=Decimal(str(position_amt)),
                                remaining_size=Decimal('0')
                            )

            self.logger.log(f"订单 {order_id} 未找到（可能已取消或完成）", "DEBUG")
            return None

        except Exception as e:
            self.logger.log(f"获取订单信息失败: {e}", "ERROR")
            return None

    @query_retry(reraise=True)
    async def _fetch_orders_with_retry(self) -> List[Dict[str, Any]]:
        """Get orders using official SDK."""
        # Ensure client is initialized
        if self.lighter_client is None:
            await self._initialize_lighter_client()

        # Generate auth token for API call
        auth_token, error = self.lighter_client.create_auth_token_with_expiry()
        if error is not None:
            self.logger.log(f"Error creating auth token: {error}", "ERROR")
            raise ValueError(f"Error creating auth token: {error}")

        # Use OrderApi to get active orders
        order_api = lighter.OrderApi(self.api_client)

        # Get active orders for the specific market
        orders_response = await order_api.account_active_orders(
            account_index=self.account_index,
            market_id=self.config.contract_id,
            auth=auth_token
        )

        if not orders_response:
            self.logger.log("Failed to get orders", "ERROR")
            raise ValueError("Failed to get orders")

        return orders_response.orders

    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract.
        
        BEHAVIOR: 
        - 如果启用WebSocket且健康：优先使用WebSocket数据
        - 否则：使用REST API
        """
        # 检查 WebSocket 是否健康（30秒内有更新）
        ws_is_healthy = (
            self.use_websocket and
            self.ws_connected and 
            self.last_ws_update_time > 0 and
            (time.time() - self.last_ws_update_time) < 30
        )
        
        # 如果 WebSocket 健康，使用内存中的订单数据
        if ws_is_healthy and len(self.active_orders_dict) >= 0:
            # 从 WebSocket 更新返回活动订单
            orders = list(self.active_orders_dict.values())
            # 记录 WebSocket 使用情况（DEBUG级别减少日志噪音）
            if self.last_ws_update_time > 0:
                ws_age = int(time.time() - self.last_ws_update_time)
                self.logger.log(f"📡 使用WebSocket数据 ({len(orders)} 订单, 更新于 {ws_age}秒前)", "DEBUG")
            return orders
        
        # 降级到 REST API
        if self.use_websocket:
            self.logger.log("🔄 WebSocket过期或断开，降级到REST API", "WARNING")
        else:
            self.logger.log("🔄 使用REST API获取活动订单", "DEBUG")
            
        order_list = await self._fetch_orders_with_retry()

        # 过滤特定市场的订单
        contract_orders = []
        for order in order_list:
            # 将 Lighter Order 转换为 OrderInfo
            side = "sell" if order.is_ask else "buy"
            size = Decimal(order.initial_base_amount)
            price = Decimal(order.price)

            # 只包含剩余数量 > 0 的订单
            if size > 0:
                order_info = OrderInfo(
                    order_id=str(order.order_index),
                    side=side,
                    size=Decimal(order.remaining_base_amount),
                    price=price,
                    status=order.status.upper(),
                    filled_size=Decimal(order.filled_base_amount),
                    remaining_size=Decimal(order.remaining_base_amount)
                )
                contract_orders.append(order_info)
                
                # 同步到 active_orders_dict（如果启用了WebSocket）
                if self.use_websocket:
                    self.active_orders_dict[order_info.order_id] = order_info

        return contract_orders
    
    async def wait_for_order_update(self, timeout: float = 60) -> bool:
        """Wait for any order update from WebSocket.
        
        Returns:
            True if update received, False if timeout
        """
        try:
            await asyncio.wait_for(self.order_update_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
    
    async def get_all_active_orders(self) -> List[OrderInfo]:
        """Get all active orders/positions across all markets."""
        try:
            positions = await self._fetch_positions_with_retry()
            
            all_positions = []
            # Check all positions across all markets
            for pos in positions:
                # position 对象有 market_id 和 position 字段
                position_amount = abs(float(getattr(pos, 'position', 0)))
                if position_amount > 0:
                    market_id = getattr(pos, 'market_id', 'unknown')
                    # 使用sign字段判断方向
                    position_sign = getattr(pos, 'sign', 1)
                    # Return position info to indicate there are active positions
                    all_positions.append(OrderInfo(
                        order_id=f"position_{market_id}",
                        side="long" if position_sign > 0 else "short",
                        size=Decimal(position_amount),
                        price=Decimal('0'),
                        status="POSITION",
                        filled_size=Decimal('0'),
                        remaining_size=Decimal('0')
                    ))
            
            return all_positions
        except Exception as e:
            self.logger.log(f"Error checking all active orders: {e}", "ERROR")
            return []

    @query_retry(reraise=True)
    async def _fetch_positions_with_retry(self) -> List[Dict[str, Any]]:
        """Get positions using official SDK."""
        # Use shared API client
        account_api = lighter.AccountApi(self.api_client)

        # Get account info
        account_data = await account_api.account(by="index", value=str(self.account_index))

        if not account_data or not account_data.accounts:
            self.logger.log("Failed to get positions", "ERROR")
            raise ValueError("Failed to get positions")

        return account_data.accounts[0].positions

    async def get_account_positions(self) -> Decimal:
        """Get account positions using official SDK."""
        # Get account info which includes positions
        positions = await self._fetch_positions_with_retry()

        # Find position for current market
        for position in positions:
            if position.market_id == self.config.contract_id:
                return Decimal(position.position)

        return Decimal(0)

    async def get_account_balance(self) -> Decimal:
        """Get account USDC balance using official SDK."""
        try:
            # Use shared API client
            account_api = lighter.AccountApi(self.api_client)

            # Get account info
            account_data = await account_api.account(by="index", value=str(self.account_index))

            if not account_data or not account_data.accounts:
                self.logger.log("Failed to get account balance", "ERROR")
                raise ValueError("Failed to get account balance")

            # Get account info
            account_info = account_data.accounts[0]
            
            # Debug: 打印账户对象的所有属性
            self.logger.log(f"账户对象属性: {dir(account_info)}", "DEBUG")
            self.logger.log(f"账户对象类型: {type(account_info)}", "DEBUG")
            
            # 尝试不同的可能属性名
            balance = None
            possible_attrs = ['free_collateral', 'available_balance', 'balance', 'free_balance', 
                            'collateral', 'equity', 'total_collateral', 'wallet_balance']
            
            for attr in possible_attrs:
                if hasattr(account_info, attr):
                    balance = Decimal(str(getattr(account_info, attr)))
                    self.logger.log(f"找到余额属性 '{attr}': {balance} USDC", "INFO")
                    return balance
            
            # 如果没找到，打印对象的字符串表示
            self.logger.log(f"账户对象内容: {account_info}", "DEBUG")
            raise ValueError(f"无法找到余额属性。可用属性: {[a for a in dir(account_info) if not a.startswith('_')]}")

        except Exception as e:
            self.logger.log(f"获取账户余额失败: {e}", "ERROR")
            raise

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """Get contract ID for a ticker."""
        ticker = self.config.ticker
        if len(ticker) == 0:
            self.logger.log("Ticker is empty", "ERROR")
            raise ValueError("Ticker is empty")

        order_api = lighter.OrderApi(self.api_client)
        # Get all order books to find the market for our ticker
        order_books = await order_api.order_books()

        # Find the market that matches our ticker
        market_info = None
        for market in order_books.order_books:
            if market.symbol == ticker:
                market_info = market
                break

        if market_info is None:
            self.logger.log("Failed to get markets", "ERROR")
            raise ValueError("Failed to get markets")

        market_summary = await order_api.order_book_details(market_id=market_info.market_id)
        order_book_details = market_summary.order_book_details[0]
        # Set contract_id to market name (Lighter uses market IDs as identifiers)
        self.config.contract_id = market_info.market_id
        self.base_amount_multiplier = pow(10, market_info.supported_size_decimals)
        self.price_multiplier = pow(10, market_info.supported_price_decimals)

        try:
            self.config.tick_size = Decimal("1") / (Decimal("10") ** order_book_details.price_decimals)
        except Exception:
            self.logger.log("Failed to get tick size", "ERROR")
            raise ValueError("Failed to get tick size")

        return self.config.contract_id, self.config.tick_size
