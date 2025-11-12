#!/usr/bin/env python3
"""
常驻进程版本的 runbot - 支持通过标准输入接收交易指令
增强版：添加持仓时间和盈亏统计
"""
import sys
import json
import asyncio
import argparse
import os
import time
from decimal import Decimal
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

from trading_bot import TradingBot, TradingConfig


def log_output(message: str):
    """输出日志消息到标准输出（JSON 格式）"""
    print(json.dumps({"type": "log", "message": message}, ensure_ascii=False), flush=True)


async def get_account_balance(env_file: str, ticker: str) -> dict:
    """获取账户余额和市场价格"""
    from dotenv import load_dotenv
    load_dotenv(env_file, override=True)
    
    config = TradingConfig(
        ticker=ticker,
        contract_id='',
        tick_size=Decimal(0),
        quantity=Decimal(1),
        take_profit=Decimal(1),
        direction='buy',
        max_orders=1,
        wait_time=0,
        exchange='lighter',
        grid_step=Decimal(0),
        stop_price=Decimal(0),
        pause_price=Decimal(0),
        boost_mode=False,
        tp_sl_only=True,
        leverage=Decimal('20')
    )
    
    bot = TradingBot(config)
    
    try:
        # 初始化
        config.contract_id, config.tick_size = await bot.exchange_client.get_contract_attributes()
        await bot.exchange_client.connect()
        await asyncio.sleep(2)
        
        # 获取余额
        balance = await bot.exchange_client.get_account_balance()
        
        # 获取当前价格
        best_bid, best_ask = await bot.exchange_client.fetch_bbo_prices(config.contract_id)
        mid_price = (best_bid + best_ask) / 2
        
        return {
            'success': True,
            'balance': float(balance),
            'price': float(mid_price),
            'ticker': ticker
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }
    finally:
        await bot.exchange_client.disconnect()


async def cleanup_account(env_file: str, ticker: str) -> dict:
    """清理账户：检查并清理所有活跃订单和持仓"""
    from dotenv import load_dotenv
    load_dotenv(env_file, override=True)
    
    config = TradingConfig(
        ticker=ticker,
        contract_id='',
        tick_size=Decimal(0),
        quantity=Decimal(1),
        take_profit=Decimal(1),
        direction='buy',
        max_orders=1,
        wait_time=0,
        exchange='lighter',
        grid_step=Decimal(0),
        stop_price=Decimal(0),
        pause_price=Decimal(0),
        boost_mode=False,
        tp_sl_only=True,
        leverage=Decimal('20')
    )
    
    bot = TradingBot(config)
    
    try:
        # 初始化
        config.contract_id, config.tick_size = await bot.exchange_client.get_contract_attributes()
        await bot.exchange_client.connect()
        await asyncio.sleep(2)
        
        # 检查是否有活跃订单/持仓
        all_orders = await bot.exchange_client.get_all_active_orders()
        
        # 直接检查订单数量，不打印中间日志避免缓冲区满
        if len(all_orders) == 0:
            # 账户干净，直接返回结果（不打印日志）
            result = {'cleanup_success': True, 'cleaned': False}
            print(json.dumps(result, ensure_ascii=False), flush=True)
            return result
        
        log_output(f"⚠️ 检测到 {len(all_orders)} 个活跃订单/持仓，开始清理...")
        
        # 步骤1: 取消所有订单
        log_output("🗑️ 步骤1: 取消所有订单...")
        cancel_result = await bot.exchange_client.cancel_all_orders()
        if not cancel_result.success:
            log_output(f"❌ 取消订单失败: {cancel_result.error_message}")
        else:
            log_output("✅ 所有订单已取消")
        
        await asyncio.sleep(3)
        
        # 步骤2: 平仓所有持仓
        log_output("🔄 步骤2: 平仓所有持仓...")
        try:
            close_result = await asyncio.wait_for(
                bot.exchange_client.close_all_positions(),
                timeout=120.0  # 最多等2分钟
            )
            log_output(f"🔍 平仓操作完成，结果: success={close_result.success}")
        except asyncio.TimeoutError:
            log_output(f"❌ 平仓操作超时（120秒）")
            result = {
                'cleanup_success': False,
                'error': 'Close positions timeout after 120 seconds'
            }
            log_output(f"🔙 准备返回超时结果: {result}")
            return result
        
        if not close_result.success:
            log_output(f"❌ 平仓失败: {close_result.error_message}")
            result = {
                'cleanup_success': False,
                'error': f'Failed to close positions: {close_result.error_message}'
            }
            log_output(f"🔙 准备返回平仓失败结果: {result}")
            return result
        else:
            log_output("✅ 所有持仓已平仓（含重试机制）")
        
        # close_all_positions 内部已等待，这里再等待2秒确保
        await asyncio.sleep(2)
        
        # 验证清理结果
        all_orders_after = await bot.exchange_client.get_all_active_orders()
        
        # 再次检查实际持仓
        try:
            positions_after = await bot.exchange_client._fetch_positions_with_retry()
            actual_position_count_after = sum(1 for p in positions_after if abs(float(getattr(p, 'position', 0))) > 0.00001)
            
            # 检查是否有极小持仓（僵尸持仓）
            tiny_positions = []
            for p in positions_after:
                pos_amount = abs(float(getattr(p, 'position', 0)))
                if 0.00000001 < pos_amount < 0.0001:  # 极小但非零
                    tiny_positions.append({
                        'market': p.market_id,
                        'symbol': p.symbol,
                        'amount': pos_amount
                    })
            
            log_output(f"📊 清理后状态: {len(all_orders_after)} 个订单/持仓记录, 其中 {actual_position_count_after} 个实际持仓")
            
            if tiny_positions:
                log_output(f"⚠️ 发现 {len(tiny_positions)} 个极小持仓（僵尸持仓，无法平掉）:")
                for tp in tiny_positions:
                    log_output(f"   {tp['symbol']}: {tp['amount']:.10f}")
            
            # 如果有订单记录但没有实际持仓，可能只是挂单
            if len(all_orders_after) > 0 and actual_position_count_after == 0:
                log_output(f"ℹ️ 剩余 {len(all_orders_after)} 个可能是止盈止损单，非实际持仓")
        except Exception as e:
            log_output(f"⚠️ 无法获取清理后持仓详情: {e}")
            actual_position_count_after = -1
        
        if len(all_orders_after) > 0:
            log_output(f"❌ 清理失败：仍有 {len(all_orders_after)} 个订单/持仓未清空")
            result = {
                'cleanup_success': False,
                'error': f'Cleanup failed: {len(all_orders_after)} orders/positions remaining'
            }
            log_output(f"🔙 准备返回验证失败结果: {result}")
            return result
        else:
            log_output("✅ 清理完成，账户已清空")
        
        result = {
            'cleanup_success': True,
            'cleaned': True,
            'orders_before': len(all_orders),
            'orders_after': len(all_orders_after)
        }
        log_output(f"🔙 准备返回成功结果: {result}")
        return result
        
    except Exception as e:
        log_output(f"清理失败: {e}")
        result = {'cleanup_success': False, 'error': str(e)}
        log_output(f"🔙 准备返回异常结果: {result}")
        return result
    finally:
        # Don't disconnect here - it blocks the return
        # The connection will be reused for trading
        pass


async def get_position_stats(bot, config):
    """获取持仓统计信息"""
    try:
        # 获取账户持仓详情
        positions = await bot.exchange_client._fetch_positions_with_retry()
        
        for position in positions:
            if position.symbol == config.ticker:
                return {
                    'symbol': position.symbol,
                    'position_size': float(position.position),
                    'avg_entry_price': float(position.avg_entry_price),
                    'position_value': float(position.position_value),
                    'unrealized_pnl': float(position.unrealized_pnl),
                    'realized_pnl': float(position.realized_pnl),
                    'total_funding': float(position.total_funding_paid_out) if position.total_funding_paid_out else 0.0
                }
        return None
    except Exception as e:
        log_output(f"获取持仓统计失败: {e}")
        return None


async def execute_single_trade(config: TradingConfig):
    """执行单次交易（增强版：记录统计数据）"""
    bot = TradingBot(config)
    
    # 统计数据
    stats = {
        'ticker': config.ticker,
        'direction': config.direction,
        'quantity': float(config.quantity),
        'leverage': float(config.leverage),
        'target_profit_pct': float(config.take_profit),
        'open_time': None,
        'close_time': None,
        'holding_seconds': None,
        'entry_price': None,
        'exit_price': None,
        'pnl': None,
        'pnl_pct': None,
        'funding_cost': None,
        'result': None  # 'TP' or 'SL' or 'TIMEOUT' or 'ERROR'
    }
    
    try:
        # 初始化
        log_output(f"正在初始化 {config.ticker}...")
        config.contract_id, config.tick_size = await bot.exchange_client.get_contract_attributes()
        bot.loop = asyncio.get_running_loop()
        await bot.exchange_client.connect()
        await asyncio.sleep(5)
        
        # 注意：清理机制已经在主进程中执行（在余额检测之前）
        # 这里不再重复检查和清理
        
        log_output(f"准备开仓: {config.direction} {config.quantity} {config.ticker}")
        
        # 记录开仓时间
        stats['open_time'] = datetime.now().isoformat()
        open_timestamp = time.time()
        
        # 开仓并设置止盈止损
        success = await bot._place_and_monitor_open_order()
        if not success:
            log_output("开仓失败 ✗")
            stats['result'] = 'ERROR'
            return {"success": False, "error": "Failed to place order", "stats": stats}
        
        # 获取开仓后的持仓信息
        await asyncio.sleep(1)
        position_after_open = await get_position_stats(bot, config)
        if position_after_open:
            stats['entry_price'] = position_after_open['avg_entry_price']
            log_output(f"✓ 开仓价格: {stats['entry_price']}")
        
        log_output("开仓成功，止盈止损订单已设置 ✓")
        
        # 短暂等待，确保止盈止损订单已经生效
        await asyncio.sleep(0.2)
        
        # 等待止盈/止损
        log_output("等待止盈或止损触发...")
        max_wait = 7200 * 2  # 最多等待4小时
        start_time = asyncio.get_event_loop().time()
        
        last_status_log_time = start_time
        initial_check_done = False
        
        # 记录最后一次持仓信息（用于计算盈亏）
        last_position_info = position_after_open
        
        while asyncio.get_event_loop().time() - start_time < max_wait:
            # 等待订单更新事件
            update_received = await bot.exchange_client.wait_for_order_update(timeout=60)
            
            # 获取活动订单
            active_orders = await bot.exchange_client.get_active_orders(config.contract_id)
            
            # 如果订单有更新，尝试获取最新的持仓信息
            if update_received and len(active_orders) > 0:
                current_position = await get_position_stats(bot, config)
                if current_position:
                    last_position_info = current_position
            
            if len(active_orders) == 0:
                # 记录平仓时间
                stats['close_time'] = datetime.now().isoformat()
                close_timestamp = time.time()
                stats['holding_seconds'] = int(close_timestamp - open_timestamp)
                
                log_output(f"所有订单已完成 ✓ (持仓时间: {stats['holding_seconds']}秒)")
                
                # 获取最终的账户信息来确定盈亏
                await asyncio.sleep(1)
                final_position = await get_position_stats(bot, config)
                
                # 计算盈亏（通过 realized_pnl 的变化）
                if last_position_info and final_position:
                    # 如果还有持仓，说明只是部分平仓（不应该发生）
                    if abs(final_position['position_size']) > 0.0001:
                        log_output(f"⚠️ 警告：仍有持仓 {final_position['position_size']}")
                    
                    # 计算盈亏（使用 unrealized_pnl 的变化）
                    stats['pnl'] = last_position_info['unrealized_pnl']
                    stats['funding_cost'] = last_position_info.get('total_funding', 0.0)
                    
                    # 计算盈亏百分比（相对于持仓价值）
                    if last_position_info['position_value'] != 0:
                        stats['pnl_pct'] = (stats['pnl'] / abs(last_position_info['position_value'])) * 100
                    
                    # 判断是止盈还是止损
                    if stats['pnl'] > 0:
                        stats['result'] = 'TP'
                    else:
                        stats['result'] = 'SL'
                    
                    log_output(f"📊 交易统计:")
                    log_output(f"   持仓时间: {stats['holding_seconds']}秒 ({stats['holding_seconds']/60:.1f}分钟)")
                    log_output(f"   开仓价格: {stats['entry_price']}")
                    log_output(f"   盈亏: {stats['pnl']:.4f} ({stats['pnl_pct']:.2f}%)")
                    log_output(f"   资金费用: {stats['funding_cost']:.4f}")
                    log_output(f"   结果: {stats['result']}")
                
                break
            
            # 状态日志输出逻辑
            current_time = asyncio.get_event_loop().time()
            elapsed = int(current_time - start_time)
            
            if not initial_check_done and elapsed >= 5:
                log_output(f"✓ 已确认 {len(active_orders)} 个订单处于挂单状态，等待市场成交...")
                initial_check_done = True
                last_status_log_time = current_time
            elif update_received:
                log_output(f"⚡ WebSocket事件触发！订单状态有变化 (已等待 {elapsed}s)")
                last_status_log_time = current_time
            elif current_time - last_status_log_time >= 600:
                log_output(f"💤 仍有 {len(active_orders)} 个订单挂单中... (已等待 {elapsed}s / {max_wait}s)")
                last_status_log_time = current_time
        else:
            # 超时 - 不再停止程序，继续下一轮
            elapsed = int(asyncio.get_event_loop().time() - start_time)
            stats['close_time'] = datetime.now().isoformat()
            stats['holding_seconds'] = elapsed
            stats['result'] = 'TIMEOUT'
            
            # 尝试获取当前持仓状态
            try:
                final_position = await get_position_stats(bot, config)
                if final_position and last_position_info:
                    stats['pnl'] = last_position_info['unrealized_pnl']
                    stats['funding_cost'] = last_position_info.get('total_funding', 0.0)
                    if last_position_info['position_value'] != 0:
                        stats['pnl_pct'] = (stats['pnl'] / abs(last_position_info['position_value'])) * 100
            except:
                pass
            
            log_output(f"⚠️ 等待超时 ({elapsed}s)！止盈止损订单未触发，跳过本轮交易")
            log_output(f"📊 超时时盈亏: {stats.get('pnl', 0):.4f}")
            
            # 返回成功但标记为超时，这样不会停止程序
            return {"success": True, "timeout": True, "stats": stats}

        return {"success": True, "message": "Trade completed", "stats": stats}
        
    except Exception as e:
        log_output(f"交易失败: {e}")
        stats['result'] = 'ERROR'
        return {"success": False, "error": str(e), "stats": stats}
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
                
            elif cmd.get('action') == 'check_balance':
                # 检查余额和价格
                ticker = cmd.get('ticker', '')
                result = await get_account_balance(env_file, ticker)
                print(json.dumps(result, ensure_ascii=False), flush=True)
            
            elif cmd.get('action') == 'cleanup':
                # 清理账户（取消所有订单+平仓所有持仓）
                ticker = cmd.get('ticker', '')
                result = await cleanup_account(env_file, ticker)
                # 立即输出结果
                json_result = json.dumps(result, ensure_ascii=False)
                print(json_result, flush=True)
                sys.stdout.flush()  # 强制刷新
                
            elif cmd.get('action') == 'exit':
                break
                
        except json.JSONDecodeError as e:
            print(json.dumps({"error": f"Invalid JSON: {e}"}, ensure_ascii=False), flush=True)
        except Exception as e:
            print(json.dumps({"error": str(e)}, ensure_ascii=False), flush=True)


def main():
    parser = argparse.ArgumentParser(description='Runbot Daemon Mode with Stats')
    parser.add_argument('--env', required=True, help='Environment file path')
    args = parser.parse_args()
    
    if not Path(args.env).exists():
        print(json.dumps({"error": f"Environment file not found: {args.env}"}, ensure_ascii=False), flush=True)
        return 1
    
    asyncio.run(daemon_mode(args.env))
    return 0


if __name__ == '__main__':
    sys.exit(main())

