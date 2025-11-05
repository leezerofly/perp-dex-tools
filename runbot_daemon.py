#!/usr/bin/env python3
"""
常驻进程版本的 runbot - 支持通过标准输入接收交易指令
"""
import sys
import json
import asyncio
import argparse
import os
from decimal import Decimal
from dotenv import load_dotenv
from pathlib import Path

from trading_bot import TradingBot, TradingConfig


def log_output(message: str):
    """输出日志消息到标准输出（JSON 格式）"""
    print(json.dumps({"type": "log", "message": message}, ensure_ascii=False), flush=True)


async def execute_single_trade(config: TradingConfig):
    """执行单次交易"""
    bot = TradingBot(config)
    
    try:
        # 初始化
        log_output(f"正在初始化 {config.ticker}...")
        config.contract_id, config.tick_size = await bot.exchange_client.get_contract_attributes()
        bot.loop = asyncio.get_running_loop()
        await bot.exchange_client.connect()
        await asyncio.sleep(5)
        
        # 检查是否已有活跃订单（检查所有市场，不仅仅是当前ticker）
        all_orders = await bot.exchange_client.get_all_active_orders()
        if len(all_orders) > 0:
            log_output(f"账号已有 {len(all_orders)} 个活跃订单/持仓，跳过开仓")
            return {"success": False, "error": "Already have active orders"}
        
        log_output(f"准备开仓: {config.direction} {config.quantity} {config.ticker}")
        
        # 开仓并设置止盈止损
        success = await bot._place_and_monitor_open_order()
        if not success:
            log_output("开仓失败 ✗")
            return {"success": False, "error": "Failed to place order"}
        
        log_output("开仓成功，止盈止损订单已设置 ✓")
        
        # 检查止盈止损订单ID是否已设置
        if bot.tp_order_id and bot.sl_order_id:
            log_output(f"[验证] TP_ID={bot.tp_order_id}, SL_ID={bot.sl_order_id} 已保存")
        else:
            log_output(f"[警告] 止盈止损订单ID未正确保存！TP_ID={bot.tp_order_id}, SL_ID={bot.sl_order_id}")
        
        # 短暂等待，确保止盈止损订单已经生效
        await asyncio.sleep(2)
        
        # 等待止盈/止损
        log_output("等待止盈或止损触发...")
        max_wait = 7200  # 最多等待2小时 (7200秒)
        start_time = asyncio.get_event_loop().time()
        
        last_status_log_time = start_time  # 上次输出状态的时间
        initial_check_done = False  # 标记是否完成初始检查
        
        while asyncio.get_event_loop().time() - start_time < max_wait:
            # === NEW: Wait for order update event instead of fixed sleep ===
            # This dramatically reduces API calls while maintaining real-time responsiveness
            update_received = await bot.exchange_client.wait_for_order_update(timeout=60)
            
            # Get active orders (will use WebSocket data, very cheap)
            active_orders = await bot.exchange_client.get_active_orders(config.contract_id)
            
            if len(active_orders) == 0:
                log_output("所有订单已完成 ✓")
                break
            
            # 注意：由于订单设置了 reduce_only=True，当其中一个止盈/止损订单触发并平仓后，
            # 另一个订单会因为没有仓位而被交易所自动取消，不需要手动处理 OCO 逻辑
            
            # 状态日志输出逻辑（优化：减少日志噪音）
            current_time = asyncio.get_event_loop().time()
            elapsed = int(current_time - start_time)
            
            # 初始确认（前60秒内）
            if not initial_check_done and elapsed >= 5:
                log_output(f"✓ 已确认 {len(active_orders)} 个订单处于挂单状态，等待市场成交...")
                initial_check_done = True
                last_status_log_time = current_time
            
            # 有变化时立即输出
            elif update_received:
                log_output(f"⚡ WebSocket事件触发！订单状态有变化 (已等待 {elapsed}s)")
                last_status_log_time = current_time
            
            # 无变化时：每10分钟输出一次心跳（而非每120秒）
            elif current_time - last_status_log_time >= 600:  # 600秒 = 10分钟
                log_output(f"💤 仍有 {len(active_orders)} 个订单挂单中... (已等待 {elapsed}s / {max_wait}s)")
                last_status_log_time = current_time
            
            # No more fixed sleep! Event-driven approach
            # Will wait for next order update or 60s timeout in next iteration
        else:
            # 超时，返回失败并标记需要停止整个程序
            elapsed = int(asyncio.get_event_loop().time() - start_time)
            log_output(f"⚠️ 等待超时 ({elapsed}s)！止盈止损订单未触发，停止程序")
            return {"success": False, "error": "TIMEOUT", "message": f"Timeout after {elapsed}s"}

        return {"success": True, "message": "Trade completed"}
        
    except Exception as e:
        log_output(f"交易失败: {e}")
        return {"success": False, "error": str(e)}
    finally:
        await bot.exchange_client.disconnect()


async def daemon_mode(env_file: str):
    """守护进程模式 - 持续接收命令"""
    # 加载环境变量
    load_dotenv(env_file, override=True)
    
    print(json.dumps({"status": "ready"}, ensure_ascii=False), flush=True)
    
    # 从标准输入读取命令
    loop = asyncio.get_event_loop()
    
    while True:
        try:
            # 读取一行命令
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            
            line = line.strip()
            if not line:
                continue
            
            # 解析命令
            cmd = json.loads(line)
            
            if cmd.get('action') == 'trade':
                # 创建配置
                config = TradingConfig(
                    ticker=cmd['ticker'],
                    contract_id='',
                    tick_size=Decimal(0),
                    quantity=Decimal(str(cmd['quantity'])),
                    take_profit=Decimal(str(cmd['take_profit'])),
                    direction=cmd['direction'],
                    max_orders=1,
                    wait_time=0,
                    exchange='lighter',
                    grid_step=Decimal(0),
                    stop_price=Decimal(0),
                    pause_price=Decimal(0),
                    boost_mode=False,
                    tp_sl_only=True,
                    leverage=Decimal(str(cmd.get('leverage', '20')))
                )
                
                # 执行交易
                result = await execute_single_trade(config)
                print(json.dumps(result, ensure_ascii=False), flush=True)
                
            elif cmd.get('action') == 'exit':
                break
                
        except json.JSONDecodeError as e:
            print(json.dumps({"error": f"Invalid JSON: {e}"}, ensure_ascii=False), flush=True)
        except Exception as e:
            print(json.dumps({"error": str(e)}, ensure_ascii=False), flush=True)


def main():
    parser = argparse.ArgumentParser(description='Runbot Daemon Mode')
    parser.add_argument('--env', required=True, help='Environment file path')
    args = parser.parse_args()
    
    if not Path(args.env).exists():
        print(json.dumps({"error": f"Environment file not found: {args.env}"}, ensure_ascii=False), flush=True)
        return 1
    
    asyncio.run(daemon_mode(args.env))
    return 0


if __name__ == '__main__':
    sys.exit(main())

