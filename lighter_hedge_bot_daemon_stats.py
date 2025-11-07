#!/usr/bin/env python3
"""
使用常驻进程的对冲机器人控制器（带统计功能）
- 三个 runbot_daemon_with_stats.py 持续运行
- 父进程通过 stdin/stdout 与子进程通信
- 收集并分析对冲交易统计数据
"""

import argparse
import json
import random
import signal
import subprocess
import sys
import time
from pathlib import Path
from decimal import Decimal
from typing import List, Optional

from hedge_stats_collector import HedgeStatsCollector


# ANSI 颜色代码
class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def parse_arguments():
    parser = argparse.ArgumentParser(description='Lighter 对冲 - 守护进程版本（带统计）')
    
    parser.add_argument('--group', help='组名（从 hedge_config_multi.json 读取配置）')
    parser.add_argument('--env1', help='账号1 .env 路径')
    parser.add_argument('--env2', help='账号2 .env 路径')
    parser.add_argument('--env3', help='账号3 .env 路径')
    parser.add_argument('--config', default='hedge_config.json', help='对冲配置文件')
    parser.add_argument('--multi-config', default='hedge_config_multi.json', help='多组配置文件路径')
    parser.add_argument('--log-dir', default='./logs', help='日志输出目录')
    parser.add_argument('--rounds', type=int, default=2000, help='运行轮数')
    parser.add_argument('--interval', type=int, default=60, help='多轮之间的间隔秒数')
    return parser.parse_args()


def load_hedge_config(config_file: str) -> List[List]:
    cfg_path = Path(config_file)
    if not cfg_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    data = json.loads(cfg_path.read_text(encoding='utf-8'))
    if not isinstance(data, list) or not data:
        raise ValueError('配置内容不合法或为空')
    return data


def load_group_config(multi_config_file: str, group_name: str):
    """从多组配置文件加载指定组的配置"""
    cfg_path = Path(multi_config_file)
    if not cfg_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {multi_config_file}")
    
    data = json.loads(cfg_path.read_text(encoding='utf-8'))
    
    for group in data.get('groups', []):
        if group.get('name') == group_name:
            if not group.get('enabled', True):
                raise ValueError(f"组 {group_name} 已被禁用")
            
            accounts = group.get('accounts', {})
            if len(accounts) != 3:
                raise ValueError(f"组 {group_name} 必须配置3个账号")
            
            return {
                'env1': list(accounts.values())[0],
                'env2': list(accounts.values())[1],
                'env3': list(accounts.values())[2],
                'trading_pairs': group.get('trading_pairs', []),
                'rounds': group.get('rounds', 2000),
                'log_prefix': group.get('log_prefix', group_name.lower())
            }
    
    raise ValueError(f"未找到组: {group_name}")


def calculate_hedge_quantities(balances: List[float], price: float, leverage: float, 
                               min_usage: float = 0.4, max_usage: float = 0.8) -> dict:
    """
    根据三个账户的余额计算对冲开仓数量
    
    Args:
        balances: 三个账户的USDC余额列表 [balance1, balance2, balance3]
        price: 当前市场价格
        leverage: 杠杆倍数
        min_usage: 最小资金使用率（默认40%）
        max_usage: 最大资金使用率（默认80%）
    
    Returns:
        dict: {
            'success': bool,
            'quantities': [qty1, qty2, qty3],  # 三个账户的开仓数量
            'usage_rates': [rate1, rate2, rate3],  # 资金使用率
            'error': str  # 如果失败，返回错误信息
        }
    """
    
    if len(balances) != 3:
        return {'success': False, 'error': '必须提供3个账户的余额'}
    
    if price <= 0:
        return {'success': False, 'error': '价格必须大于0'}
    
    if leverage <= 0:
        return {'success': False, 'error': '杠杆倍数必须大于0'}
    
    if any(b <= 0 for b in balances):
        return {'success': False, 'error': '所有账户余额必须大于0'}
    
    balance1 = balances[0]
    balance2 = balances[1]
    balance3 = balances[2]
    total_hedge_balance = balance2 + balance3
    
    # 目标使用率（尽量接近最大值以最大化资金利用）
    target_usage = max_usage  # 改为直接使用最大值80%
    
    # 对于对冲策略：
    # bot1 做主方向：qty1
    # bot2 + bot3 做对冲：qty2 + qty3 = qty1
    #
    # 关键约束：
    # 1. usage1 = (qty1 * price) / (balance1 * leverage) 在 [min_usage, max_usage]
    # 2. usage_hedge = (qty1 * price) / (total_hedge_balance * leverage) 在 [min_usage, max_usage]
    #
    # 从约束1：qty1 的范围是 [(min_usage * balance1 * leverage / price), (max_usage * balance1 * leverage / price)]
    # 从约束2：qty1 的范围是 [(min_usage * total_hedge_balance * leverage / price), (max_usage * total_hedge_balance * leverage / price)]
    #
    # 需要找到这两个范围的交集
    
    # Bot1的qty1范围
    qty1_min_from_bot1 = (min_usage * balance1 * leverage) / price
    qty1_max_from_bot1 = (max_usage * balance1 * leverage) / price
    
    # 对冲账户的qty1范围
    qty1_min_from_hedge = (min_usage * total_hedge_balance * leverage) / price
    qty1_max_from_hedge = (max_usage * total_hedge_balance * leverage) / price
    
    # 取交集
    qty1_min = max(qty1_min_from_bot1, qty1_min_from_hedge)
    qty1_max = min(qty1_max_from_bot1, qty1_max_from_hedge)
    
    # 检查是否有有效的交集
    if qty1_min > qty1_max:
        # 没有交集，计算一下为什么
        usage1_at_hedge_min = (qty1_min_from_hedge * price) / (balance1 * leverage)
        usage1_at_hedge_max = (qty1_max_from_hedge * price) / (balance1 * leverage)
        usage_hedge_at_bot1_min = (qty1_min_from_bot1 * price) / (total_hedge_balance * leverage)
        usage_hedge_at_bot1_max = (qty1_max_from_bot1 * price) / (total_hedge_balance * leverage)
        
        return {
            'success': False,
            'error': f'无法找到同时满足所有账户资金使用率要求的数量。Bot1余额={balance1:.2f}, 对冲总余额={total_hedge_balance:.2f}',
            'details': {
                'bot1_balance': balance1,
                'hedge_balance': total_hedge_balance,
                'balance_ratio': balance1 / total_hedge_balance if total_hedge_balance > 0 else 0
            }
        }
    
    # 使用最大使用率计算qty1（在有效范围内）
    # 优先以最大值为目标，最大化资金利用率
    qty1_target = (target_usage * balance1 * leverage) / price
    
    # 确保在有效范围内，优先选择更大的值
    if qty1_target > qty1_max:
        qty1 = qty1_max  # 如果目标超过上限，使用上限
    elif qty1_target < qty1_min:
        qty1 = qty1_min  # 如果目标低于下限，使用下限
    else:
        qty1 = qty1_target  # 使用目标值
    
    # 按余额比例分配对冲数量到bot2和bot3
    qty2 = qty1 * (balance2 / total_hedge_balance)
    qty3 = qty1 * (balance3 / total_hedge_balance)
    
    # 计算实际资金使用率
    usage1 = (qty1 * price) / (balance1 * leverage)
    usage2 = (qty2 * price) / (balance2 * leverage)
    usage3 = (qty3 * price) / (balance3 * leverage)
    
    usage_rates = [usage1, usage2, usage3]
    
    # 验证所有账户的资金使用率（应该都在范围内，因为我们已经计算了交集）
    for i, usage in enumerate(usage_rates):
        if usage < min_usage - 0.001:  # 允许小误差
            return {
                'success': False,
                'error': f'账户{i+1}的资金使用率({usage*100:.2f}%)低于最小值({min_usage*100}%)',
                'usage_rates': usage_rates
            }
        if usage > max_usage + 0.001:  # 允许小误差
            return {
                'success': False,
                'error': f'账户{i+1}的资金使用率({usage*100:.2f}%)超过最大值({max_usage*100}%)',
                'usage_rates': usage_rates
            }
    
    # 验证对冲关系：qty2 + qty3 应该约等于 qty1
    hedge_diff = abs((qty2 + qty3) - qty1) / qty1
    if hedge_diff > 0.01:  # 允许1%的误差
        return {
            'success': False,
            'error': f'对冲数量不匹配：bot1={qty1:.6f}, bot2+bot3={qty2+qty3:.6f}',
            'quantities': [qty1, qty2, qty3],
            'usage_rates': usage_rates
        }
    
    return {
        'success': True,
        'quantities': [qty1, qty2, qty3],
        'usage_rates': usage_rates,
        'balances': balances,
        'price': price
    }


class DaemonBot:
    """管理单个 runbot_daemon_with_stats.py 守护进程"""
    def __init__(self, bot_id: int, env_file: str, log_file: Optional[Path] = None):
        self.bot_id = bot_id
        self.env_file = env_file
        self.log_file = log_file
        self.log_fd = None
        self.proc: Optional[subprocess.Popen] = None
        self.ready = False
        
        # 使用env文件名作为标识（去掉路径和.env后缀）
        self.env_name = Path(env_file).stem
        
        self.color = {
            1: Colors.RED,
            2: Colors.GREEN,
            3: Colors.YELLOW
        }.get(bot_id, Colors.RESET)
        
        # 清理失败计数器
        self.cleanup_failure_count = 0
        
    def start(self):
        """启动守护进程"""
        try:
            self.proc = subprocess.Popen(
                [sys.executable, 'runbot_daemon_with_stats.py', '--env', self.env_file],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            
            response = self.proc.stdout.readline().strip()
            data = json.loads(response)
            if data.get('status') == 'ready':
                self.ready = True
                print(f"{self.color}✓ {self.env_name}{Colors.RESET} 守护进程已启动 (PID: {self.proc.pid})")
                return True
            else:
                print(f"{self.color}✗ {self.env_name}{Colors.RESET} 启动失败: {data}")
                return False
                
        except Exception as e:
            print(f"{self.color}✗ {self.env_name}{Colors.RESET} 启动异常: {e}")
            return False
    
    def check_balance(self, ticker: str) -> dict:
        """检查账户余额和市场价格"""
        if not self.proc or not self.ready:
            return {'success': False, 'error': 'NOT_READY'}
        
        try:
            cmd = {
                'action': 'check_balance',
                'ticker': ticker
            }
            
            self.proc.stdin.write(json.dumps(cmd) + '\n')
            self.proc.stdin.flush()
            
            # 等待响应（最多10秒）
            start_time = time.time()
            while time.time() - start_time < 10:
                line = self.proc.stdout.readline()
                if not line:
                    return {'success': False, 'error': 'PIPE_CLOSED'}
                
                line = line.strip()
                if not line:
                    continue
                
                try:
                    response = json.loads(line)
                    
                    # 跳过日志消息
                    if response.get('type') == 'log':
                        continue
                    
                    # 返回余额响应
                    if 'balance' in response or 'error' in response:
                        return response
                        
                except json.JSONDecodeError:
                    continue
            
            return {'success': False, 'error': 'TIMEOUT'}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def send_trade_command(self, ticker: str, quantity: Decimal, direction: str, 
                          take_profit: Decimal, leverage: Decimal = Decimal('20')) -> bool:
        """发送交易指令"""
        if not self.proc or not self.ready:
            return False
        
        try:
            cmd = {
                'action': 'trade',
                'ticker': ticker,
                'quantity': str(quantity),
                'direction': direction,
                'take_profit': str(take_profit),
                'leverage': str(leverage)
            }
            
            self.proc.stdin.write(json.dumps(cmd) + '\n')
            self.proc.stdin.flush()
            
            print(f"  {self.color}→ {self.env_name}{Colors.RESET}: {direction} {quantity} {ticker} @ TP={take_profit}%")
            return True
            
        except Exception as e:
            print(f"{self.color}✗ {self.env_name}{Colors.RESET} 发送指令失败: {e}")
            return False
    
    def wait_for_completion(self, timeout: float = 7500) -> dict:
        """等待交易完成，返回结果字典（包含统计数据）"""
        if not self.proc:
            return {'success': False, 'error': 'NO_PROC'}
        
        try:
            start_time = time.time()
            while time.time() - start_time < timeout:
                line = self.proc.stdout.readline()
                if not line:
                    return {'success': False, 'error': 'PIPE_CLOSED'}
                
                line = line.strip()
                if not line:
                    continue
                
                if self.log_fd:
                    self.log_fd.write(line + '\n')
                    self.log_fd.flush()
                
                try:
                    response = json.loads(line)
                    
                    if response.get('type') == 'log':
                        msg = response.get('message', '')
                        print(f"    {self.color}[{self.env_name}]{Colors.RESET} {msg}")
                        continue
                    
                    if 'success' in response:
                        if response['success']:
                            # 检查是否是超时完成
                            if response.get('timeout'):
                                print(f"  {self.color}⏱ {self.env_name}{Colors.RESET} 交易超时（未触发止盈止损）")
                            else:
                                print(f"  {self.color}✓ {self.env_name}{Colors.RESET} 交易完成")
                            # 返回结果（包含stats）
                            return {
                                'success': True,
                                'timeout': response.get('timeout', False),
                                'stats': response.get('stats', {})
                            }
                        else:
                            error = response.get('error', 'UNKNOWN')
                            print(f"  {self.color}✗ {self.env_name}{Colors.RESET} 交易失败: {error}")
                            if error == 'TIMEOUT':
                                return {
                                    'success': False,
                                    'error': 'TIMEOUT',
                                    'bot_id': self.bot_id,
                                    'env_name': self.env_name,
                                    'stats': response.get('stats', {})
                                }
                            return {
                                'success': False,
                                'error': error,
                                'stats': response.get('stats', {})
                            }
                            
                except json.JSONDecodeError:
                    print(f"    {self.color}[{self.env_name}]{Colors.RESET} {line}")
                    continue
            
            print(f"  {self.color}⏱ {self.env_name}{Colors.RESET} 等待超时（主进程超时）")
            return {'success': False, 'error': 'MAIN_TIMEOUT'}
            
        except Exception as e:
            print(f"{self.color}✗ {self.env_name}{Colors.RESET} 等待异常: {e}")
            return {'success': False, 'error': str(e)}
    
    def stop(self):
        """停止守护进程"""
        if self.proc:
            try:
                cmd = {'action': 'exit'}
                self.proc.stdin.write(json.dumps(cmd) + '\n')
                self.proc.stdin.flush()
                self.proc.wait(timeout=5)
            except:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=5)
                except:
                    self.proc.kill()
            self.ready = False
        
        if self.log_fd:
            self.log_fd.close()
            self.log_fd = None


def main():
    args = parse_arguments()
    
    # 判断使用哪种模式
    if args.group:
        try:
            group_config = load_group_config(args.multi_config, args.group)
            args.env1 = group_config['env1']
            args.env2 = group_config['env2']
            args.env3 = group_config['env3']
            args.rounds = group_config['rounds']
            hedge_items = group_config['trading_pairs']
            
            print(f"{Colors.BOLD}🚀 启动组: {args.group}{Colors.RESET}")
            print(f"📁 账号: {args.env1}, {args.env2}, {args.env3}")
            print(f"📊 交易对数量: {len(hedge_items)}")
            print(f"🔄 运行轮数: {args.rounds}")
            print()
            
        except Exception as e:
            print(f"❌ 加载组配置失败: {e}")
            return 1
    else:
        if not all([args.env1, args.env2, args.env3]):
            print("❌ 错误: 必须指定 --group 或同时指定 --env1、--env2、--env3")
            return 1
        hedge_items = load_hedge_config(args.config)
    
    # 验证 env 文件
    for p in [args.env1, args.env2, args.env3]:
        if not Path(p).exists():
            print(f"错误: 环境文件不存在: {p}")
            return 1
    
    # 创建日志目录
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 创建统计收集器
    group_name = args.group if args.group else 'default'
    stats_collector = HedgeStatsCollector(group_name, args.log_dir)
    
    # 获取环境文件名
    env_names = [Path(env).stem for env in [args.env1, args.env2, args.env3]]
    
    # 创建三个守护进程
    bots = [
        DaemonBot(1, args.env1, log_dir / f'bot_{env_names[0]}_activity.log'),
        DaemonBot(2, args.env2, log_dir / f'bot_{env_names[1]}_activity.log'),
        DaemonBot(3, args.env3, log_dir / f'bot_{env_names[2]}_activity.log'),
    ]
    
    # 信号处理
    exiting = {'flag': False}
    
    def handle_sig(sig, frame):
        if exiting['flag']:
            return
        exiting['flag'] = True
        print('\n正在停止所有守护进程...')
        for bot in bots:
            bot.stop()
        # 打印最终统计
        print("\n" + "="*60)
        stats_collector.print_summary()
    
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)
    
    print(f"对冲配置: {hedge_items}\n")
    
    # 启动所有守护进程
    print("正在启动守护进程...")
    for bot in bots:
        if bot.log_file:
            bot.log_fd = open(bot.log_file, 'a', buffering=1)
        
        if not bot.start():
            print("启动失败，退出")
            return 1
        time.sleep(0.5)
    
    print(f"\n所有守护进程已就绪，开始交易循环\n")
    
    # 多轮执行
    for round_idx in range(args.rounds):
        if exiting['flag']:
            break
        
        print(f"{'='*60}")
        print(f"[Round {round_idx+1}/{args.rounds}]")
        print(f"{'='*60}")
        
        # 随机选择交易对和方向
        item = random.choice(hedge_items)
        ticker = str(item[0]).upper()
        profit_pct = Decimal(str(item[2]))
        leverage = Decimal(str(item[3])) if len(item) > 3 else Decimal('20')
        
        direction_main = random.choice(['buy', 'sell'])
        direction_hedge = 'sell' if direction_main == 'buy' else 'buy'
        
        print(f"交易对: {ticker}")
        print(f"主方向: {direction_main}, 对冲方向: {direction_hedge}")
        print(f"杠杆: {leverage}x, 目标收益: {profit_pct}%\n")
        
        # ========== 步骤1：清理检测（并发执行） ==========
        print(f"{Colors.CYAN}【步骤1/3】检查并清理所有账户（并发）...{Colors.RESET}")
        
        # 并发发送清理命令到所有子进程
        cleanup_cmd = {
            'action': 'cleanup',
            'ticker': ticker
        }
        
        # 初始化清理结果字典
        cleanup_results = {}
        
        # 发送命令到所有bot
        for bot in bots:
            # 显示失败计数（用于诊断）
            if bot.cleanup_failure_count > 0:
                print(f"  {bot.color}[{bot.env_name} 历史失败:{bot.cleanup_failure_count}次]{Colors.RESET}")
            
            try:
                bot.proc.stdin.write(json.dumps(cleanup_cmd) + '\n')
                bot.proc.stdin.flush()
                print(f"  → 发送清理命令到 {bot.color}{bot.env_name}{Colors.RESET}")
            except Exception as e:
                print(f"  {bot.color}✗ {bot.env_name}{Colors.RESET} 发送命令失败: {e}")
        
        print()
        
        # 并发等待所有bot的清理结果（最多90秒）
        start_time = time.time()
        timeout = 90  # 增加超时时间
        
        while time.time() - start_time < timeout:
            if len(cleanup_results) == len(bots):
                break  # 所有bot都返回结果了
            
            # 小延迟避免CPU占用过高
            time.sleep(0.05)
            
            for bot in bots:
                if bot.env_name in cleanup_results:
                    continue  # 已经有结果了
                
                try:
                    # 非阻塞读取
                    import select
                    ready, _, _ = select.select([bot.proc.stdout], [], [], 0)
                    if ready:
                        line = bot.proc.stdout.readline()
                        if not line:
                            continue
                        
                        line = line.strip()
                        if not line:
                            continue
                        
                        try:
                            response = json.loads(line)
                            
                            # 跳过日志消息
                            if response.get('type') == 'log':
                                msg = response.get('message', '')
                                print(f"    {bot.color}[{bot.env_name}]{Colors.RESET} {msg}")
                                continue
                            
                            # 清理完成响应
                            if 'cleanup_success' in response:
                                cleanup_results[bot.env_name] = response
                                if response['cleanup_success']:
                                    print(f"  {bot.color}✓ {bot.env_name}{Colors.RESET} 清理完成")
                                    # 清理成功，重置失败计数器
                                    bot.cleanup_failure_count = 0
                                else:
                                    error = response.get('error', 'UNKNOWN')
                                    print(f"  {bot.color}❌ {bot.env_name}{Colors.RESET} 清理失败: {error}")
                                    # 清理失败，增加失败计数器
                                    bot.cleanup_failure_count += 1
                        except json.JSONDecodeError as e:
                            # 打印调试信息
                            print(f"    {bot.color}[{bot.env_name}] JSON解析错误: {line[:100]}{Colors.RESET}")
                            continue
                except Exception as e:
                    # 打印异常信息以便调试
                    print(f"    {bot.color}[{bot.env_name}] 读取异常: {e}{Colors.RESET}")
                    continue
        
        # 检查超时或失败的bot
        failed_bots = []
        for bot in bots:
            if bot.env_name not in cleanup_results:
                failed_bots.append(bot.env_name)
                print(f"  {bot.color}⚠ {bot.env_name}{Colors.RESET} 清理超时")
            elif not cleanup_results[bot.env_name].get('cleanup_success'):
                failed_bots.append(bot.env_name)
        
        if failed_bots:
            print(f"{Colors.YELLOW}⚠️ {len(failed_bots)} 个账户清理失败/超时，跳过本轮交易{Colors.RESET}")
            print()
            continue  # 跳到下一个round
        
        print()
        
        # ========== 步骤2：余额检测 ==========
        print(f"{Colors.CYAN}【步骤2/3】检测清理后的账户余额...{Colors.RESET}")
        
        balance_results = []
        for i, bot in enumerate(bots):
            result = bot.check_balance(ticker)
            if not result.get('success'):
                error_msg = result.get('error', 'UNKNOWN')
                print(f"{Colors.RED}❌ {bot.env_name} 余额检测失败: {error_msg}{Colors.RESET}")
                print(f"{Colors.RED}无法继续交易，终止程序！{Colors.RESET}")
                for b in bots:
                    b.stop()
                return 1
            balance_results.append(result)
            print(f"  {bot.color}✓ {bot.env_name}{Colors.RESET}: 余额={result['balance']:.2f} USDC")
        
        # 获取价格（使用第一个bot返回的价格）
        current_price = balance_results[0]['price']
        print(f"\n当前市场价格: {current_price:.4f} USDC")
        
        # 提取余额
        balances = [result['balance'] for result in balance_results]
        
        # ========== 新增：动态选择主账户（余额最大的） ==========
        max_balance_idx = balances.index(max(balances))
        print(f"{Colors.CYAN}💡 自动选择余额最大的账户作为主账户：{bots[max_balance_idx].env_name} (余额={balances[max_balance_idx]:.2f} USDC){Colors.RESET}\n")
        
        # 重新排列账户顺序：主账户放在第一位
        # 创建新的排序：主账户在第一位，其他两个账户在后面
        sorted_indices = [max_balance_idx]
        for i in range(3):
            if i != max_balance_idx:
                sorted_indices.append(i)
        
        # 重新排列 bots 和 balances
        bots_sorted = [bots[i] for i in sorted_indices]
        balances_sorted = [balances[i] for i in sorted_indices]
        
        # 计算动态开仓数量（使用排序后的余额）
        print(f"{Colors.CYAN}【步骤3/3】计算对冲开仓数量...{Colors.RESET}")
        calc_result = calculate_hedge_quantities(
            balances=balances_sorted,
            price=current_price,
            leverage=float(leverage),
            min_usage=0.4,
            max_usage=0.8
        )
        
        if not calc_result['success']:
            error_msg = calc_result.get('error', 'UNKNOWN')
            print(f"{Colors.RED}❌ 计算开仓数量失败: {error_msg}{Colors.RESET}")
            if 'usage_rates' in calc_result:
                usage_rates = calc_result['usage_rates']
                for i, rate in enumerate(usage_rates):
                    print(f"  账户{i+1} ({bots_sorted[i].env_name}): 资金使用率={rate*100:.2f}%")
            print(f"{Colors.RED}不满足开仓条件，终止程序！{Colors.RESET}")
            for bot in bots:
                bot.stop()
            return 1
        
        # 获取计算出的数量
        quantities = calc_result['quantities']
        usage_rates = calc_result['usage_rates']
        
        qty_main = Decimal(str(quantities[0])).quantize(Decimal('0.00000001'))
        qty2 = Decimal(str(quantities[1])).quantize(Decimal('0.00000001'))
        qty3 = Decimal(str(quantities[2])).quantize(Decimal('0.00000001'))
        
        # 打印开仓计划（使用排序后的顺序）
        print(f"{Colors.GREEN}✓ 开仓数量计算成功：{Colors.RESET}")
        print(f"  {bots_sorted[0].color}{bots_sorted[0].env_name} (主账户){Colors.RESET}: {direction_main} {qty_main} (资金使用率: {usage_rates[0]*100:.2f}%)")
        print(f"  {bots_sorted[1].color}{bots_sorted[1].env_name} (对冲){Colors.RESET}: {direction_hedge} {qty2} (资金使用率: {usage_rates[1]*100:.2f}%)")
        print(f"  {bots_sorted[2].color}{bots_sorted[2].env_name} (对冲){Colors.RESET}: {direction_hedge} {qty3} (资金使用率: {usage_rates[2]*100:.2f}%)")
        print(f"  对冲验证: {qty_main} vs {qty2 + qty3} = {abs(qty_main - (qty2 + qty3)):.8f} (差值)")
        print()
        
        # 发送交易指令（使用排序后的顺序）
        print("发送交易指令:")
        bots_sorted[0].send_trade_command(ticker, qty_main, direction_main, profit_pct, leverage)
        time.sleep(0.2)
        bots_sorted[1].send_trade_command(ticker, qty2, direction_hedge, profit_pct, leverage)
        time.sleep(0.2)
        bots_sorted[2].send_trade_command(ticker, qty3, direction_hedge, profit_pct, leverage)
        
        # 等待所有机器人完成（使用排序后的顺序）
        print("\n等待所有交易完成...")
        
        import threading
        results = [None, None, None]
        
        def wait_bot(idx):
            results[idx] = bots_sorted[idx].wait_for_completion(timeout=7500)
        
        threads = []
        for i in range(3):
            t = threading.Thread(target=wait_bot, args=(i,))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()
        
        # 检查超时错误
        timeout_detected = False
        for idx, result in enumerate(results):
            if result and result.get('error') == 'TIMEOUT':
                timeout_detected = True
                env_name = result.get('env_name', bots_sorted[idx].env_name)
                print(f"\n❌ {env_name} 止盈止损等待超时，停止整个程序！\n")
                break
        
        if timeout_detected:
            print("正在停止所有守护进程...")
            for bot in bots:
                bot.stop()
            print("程序已终止")
            stats_collector.print_summary()
            return 1
        
        success_count = sum(1 for r in results if r and r.get('success'))
        print(f"\n本轮结果: {success_count}/3 成功\n")
        
        # 收集统计数据（需要恢复原始顺序以正确记录）
        if success_count == 3:
            # 将结果按原始bot顺序重新排列
            results_original_order = [None, None, None]
            for i, sorted_idx in enumerate(sorted_indices):
                results_original_order[sorted_idx] = results[i]
            
            bot_stats = [r.get('stats', {}) for r in results_original_order]
            stats_collector.add_round_stats(round_idx + 1, ticker, bot_stats)
            print(f"📊 统计数据已记录到: {stats_collector.stats_file}")
        
        # 轮间等待
        if round_idx < args.rounds - 1 and not exiting['flag']:
            wait_time = random.randint(30, 60)
            print(f"等待 {wait_time} 秒后进入下一轮...\n")
            time.sleep(wait_time)
    
    # 清理
    print("\n正在停止所有守护进程...")
    for bot in bots:
        bot.stop()
    
    # 打印最终统计
    print("\n" + "="*60)
    print("📊 最终统计报告")
    stats_collector.print_summary()
    
    print("退出")
    return 0


if __name__ == '__main__':
    sys.exit(main())

