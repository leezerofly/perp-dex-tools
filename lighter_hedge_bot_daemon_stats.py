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


class DaemonBot:
    """管理单个 runbot_daemon_with_stats.py 守护进程"""
    def __init__(self, bot_id: int, env_file: str, log_file: Optional[Path] = None):
        self.bot_id = bot_id
        self.env_file = env_file
        self.log_file = log_file
        self.log_fd = None
        self.proc: Optional[subprocess.Popen] = None
        self.ready = False
        
        self.color = {
            1: Colors.RED,
            2: Colors.GREEN,
            3: Colors.YELLOW
        }.get(bot_id, Colors.RESET)
        
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
                print(f"{self.color}✓ Bot{self.bot_id}{Colors.RESET} 守护进程已启动 (PID: {self.proc.pid})")
                return True
            else:
                print(f"{self.color}✗ Bot{self.bot_id}{Colors.RESET} 启动失败: {data}")
                return False
                
        except Exception as e:
            print(f"{self.color}✗ Bot{self.bot_id}{Colors.RESET} 启动异常: {e}")
            return False
    
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
            
            print(f"  {self.color}→ Bot{self.bot_id}{Colors.RESET}: {direction} {quantity} {ticker} @ TP={take_profit}%")
            return True
            
        except Exception as e:
            print(f"{self.color}✗ Bot{self.bot_id}{Colors.RESET} 发送指令失败: {e}")
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
                        print(f"    {self.color}[Bot{self.bot_id}]{Colors.RESET} {msg}")
                        continue
                    
                    if 'success' in response:
                        if response['success']:
                            print(f"  {self.color}✓ Bot{self.bot_id}{Colors.RESET} 交易完成")
                            # 返回结果（包含stats）
                            return {
                                'success': True,
                                'stats': response.get('stats', {})
                            }
                        else:
                            error = response.get('error', 'UNKNOWN')
                            print(f"  {self.color}✗ Bot{self.bot_id}{Colors.RESET} 交易失败: {error}")
                            if error == 'TIMEOUT':
                                return {
                                    'success': False,
                                    'error': 'TIMEOUT',
                                    'bot_id': self.bot_id,
                                    'stats': response.get('stats', {})
                                }
                            return {
                                'success': False,
                                'error': error,
                                'stats': response.get('stats', {})
                            }
                            
                except json.JSONDecodeError:
                    print(f"    {self.color}[Bot{self.bot_id}]{Colors.RESET} {line}")
                    continue
            
            print(f"  {self.color}⏱ Bot{self.bot_id}{Colors.RESET} 等待超时（主进程超时）")
            return {'success': False, 'error': 'MAIN_TIMEOUT'}
            
        except Exception as e:
            print(f"{self.color}✗ Bot{self.bot_id}{Colors.RESET} 等待异常: {e}")
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
        qty_main = Decimal(str(item[1]))
        profit_pct = Decimal(str(item[2]))
        leverage = Decimal(str(item[3])) if len(item) > 3 else Decimal('20')
        
        direction_main = random.choice(['buy', 'sell'])
        direction_hedge = 'sell' if direction_main == 'buy' else 'buy'
        
        # 数量分配
        qty2 = (qty_main / Decimal('2')).quantize(Decimal('0.00000001'))
        qty3 = (qty_main - qty2).quantize(Decimal('0.00000001'))
        
        print(f"交易对: {ticker}")
        print(f"方向: Bot1={direction_main}, Bot2/3={direction_hedge}")
        print(f"数量: Bot1={qty_main}, Bot2={qty2}, Bot3={qty3}")
        print(f"杠杆: {leverage}x, 目标收益: {profit_pct}%\n")
        
        # 发送交易指令
        print("发送交易指令:")
        bots[0].send_trade_command(ticker, qty_main, direction_main, profit_pct, leverage)
        time.sleep(0.2)
        bots[1].send_trade_command(ticker, qty2, direction_hedge, profit_pct, leverage)
        time.sleep(0.2)
        bots[2].send_trade_command(ticker, qty3, direction_hedge, profit_pct, leverage)
        
        # 等待所有机器人完成
        print("\n等待所有交易完成...")
        
        import threading
        results = [None, None, None]
        
        def wait_bot(idx):
            results[idx] = bots[idx].wait_for_completion(timeout=7500)
        
        threads = []
        for i in range(3):
            t = threading.Thread(target=wait_bot, args=(i,))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()
        
        # 检查超时错误
        timeout_detected = False
        for result in results:
            if result and result.get('error') == 'TIMEOUT':
                timeout_detected = True
                bot_id = result.get('bot_id', '?')
                print(f"\n❌ Bot{bot_id} 止盈止损等待超时，停止整个程序！\n")
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
        
        # 收集统计数据
        if success_count == 3:
            bot_stats = [r.get('stats', {}) for r in results]
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

