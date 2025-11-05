#!/usr/bin/env python3
"""
对冲交易统计收集器
收集三个机器人的交易数据并生成对冲分析报表
"""
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict
from decimal import Decimal


class HedgeStatsCollector:
    """对冲交易统计收集器"""
    
    def __init__(self, group_name: str, log_dir: str = './logs'):
        self.group_name = group_name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # 统计文件路径
        self.stats_file = self.log_dir / f'hedge_stats_{group_name}.csv'
        self.summary_file = self.log_dir / f'hedge_summary_{group_name}.json'
        
        # 初始化CSV文件
        self._init_csv_file()
    
    def _init_csv_file(self):
        """初始化CSV文件（如果不存在）"""
        if not self.stats_file.exists():
            with open(self.stats_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'round',           # 轮次
                    'timestamp',       # 时间戳
                    'ticker',          # 交易对
                    'bot1_direction',  # Bot1方向
                    'bot1_quantity',   # Bot1数量
                    'bot1_leverage',   # Bot1杠杆
                    'bot1_holding_seconds',  # Bot1持仓时间（秒）
                    'bot1_entry_price',      # Bot1开仓价
                    'bot1_pnl',              # Bot1盈亏
                    'bot1_pnl_pct',          # Bot1盈亏%
                    'bot1_funding',          # Bot1资金费用
                    'bot1_result',           # Bot1结果(TP/SL)
                    'bot2_direction',
                    'bot2_quantity',
                    'bot2_leverage',
                    'bot2_holding_seconds',
                    'bot2_entry_price',
                    'bot2_pnl',
                    'bot2_pnl_pct',
                    'bot2_funding',
                    'bot2_result',
                    'bot3_direction',
                    'bot3_quantity',
                    'bot3_leverage',
                    'bot3_holding_seconds',
                    'bot3_entry_price',
                    'bot3_pnl',
                    'bot3_pnl_pct',
                    'bot3_funding',
                    'bot3_result',
                    'total_pnl',           # 总盈亏
                    'total_funding',       # 总资金费用
                    'net_pnl',             # 净盈亏（扣除资金费用）
                    'hedge_loss',          # 对冲损耗
                    'avg_holding_seconds', # 平均持仓时间
                ])
    
    def add_round_stats(self, round_num: int, ticker: str, bot_stats: List[Dict]):
        """
        添加一轮交易统计
        
        Args:
            round_num: 轮次编号
            ticker: 交易对
            bot_stats: 三个机器人的统计数据列表
        """
        if len(bot_stats) != 3:
            raise ValueError("需要提供3个机器人的统计数据")
        
        # 计算汇总数据
        total_pnl = sum(float(stats.get('pnl', 0)) for stats in bot_stats if stats.get('pnl') is not None)
        total_funding = sum(float(stats.get('funding_cost', 0)) for stats in bot_stats if stats.get('funding_cost') is not None)
        net_pnl = total_pnl - total_funding
        
        # 对冲损耗 = |Bot1盈亏| - (|Bot2盈亏| + |Bot3盈亏|)
        # 理论上应该接近0，偏离越大说明对冲效果越差
        bot1_pnl = abs(float(bot_stats[0].get('pnl', 0)))
        bot2_pnl = abs(float(bot_stats[1].get('pnl', 0)))
        bot3_pnl = abs(float(bot_stats[2].get('pnl', 0)))
        hedge_loss = bot1_pnl - (bot2_pnl + bot3_pnl)
        
        # 平均持仓时间
        holding_times = [stats.get('holding_seconds', 0) for stats in bot_stats if stats.get('holding_seconds')]
        avg_holding = sum(holding_times) / len(holding_times) if holding_times else 0
        
        # 写入CSV
        with open(self.stats_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            row = [
                round_num,
                datetime.now().isoformat(),
                ticker,
            ]
            
            # 添加每个机器人的数据
            for i, stats in enumerate(bot_stats, 1):
                row.extend([
                    stats.get('direction', ''),
                    stats.get('quantity', ''),
                    stats.get('leverage', ''),
                    stats.get('holding_seconds', ''),
                    stats.get('entry_price', ''),
                    stats.get('pnl', ''),
                    stats.get('pnl_pct', ''),
                    stats.get('funding_cost', ''),
                    stats.get('result', ''),
                ])
            
            # 添加汇总数据
            row.extend([
                f"{total_pnl:.4f}",
                f"{total_funding:.4f}",
                f"{net_pnl:.4f}",
                f"{hedge_loss:.4f}",
                int(avg_holding),
            ])
            
            writer.writerow(row)
        
        # 更新汇总文件
        self._update_summary()
    
    def _update_summary(self):
        """更新汇总统计"""
        try:
            # 读取所有交易记录
            trades = []
            with open(self.stats_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                trades = list(reader)
            
            if not trades:
                return
            
            # 计算汇总统计
            total_rounds = len(trades)
            total_net_pnl = sum(float(t['net_pnl']) for t in trades if t['net_pnl'])
            total_hedge_loss = sum(float(t['hedge_loss']) for t in trades if t['hedge_loss'])
            avg_hedge_loss = total_hedge_loss / total_rounds if total_rounds > 0 else 0
            
            # 统计TP/SL次数
            tp_count = sum(1 for t in trades if 'TP' in [t['bot1_result'], t['bot2_result'], t['bot3_result']])
            sl_count = sum(1 for t in trades if 'SL' in [t['bot1_result'], t['bot2_result'], t['bot3_result']])
            
            # 平均持仓时间
            avg_holding_times = [float(t['avg_holding_seconds']) for t in trades if t['avg_holding_seconds']]
            overall_avg_holding = sum(avg_holding_times) / len(avg_holding_times) if avg_holding_times else 0
            
            # 最佳和最差表现
            best_trade = max(trades, key=lambda t: float(t['net_pnl']) if t['net_pnl'] else -999999)
            worst_trade = min(trades, key=lambda t: float(t['net_pnl']) if t['net_pnl'] else 999999)
            
            summary = {
                'group_name': self.group_name,
                'generated_at': datetime.now().isoformat(),
                'total_rounds': total_rounds,
                'total_net_pnl': round(total_net_pnl, 4),
                'avg_net_pnl_per_round': round(total_net_pnl / total_rounds, 4) if total_rounds > 0 else 0,
                'total_hedge_loss': round(total_hedge_loss, 4),
                'avg_hedge_loss_per_round': round(avg_hedge_loss, 4),
                'avg_holding_seconds': int(overall_avg_holding),
                'avg_holding_minutes': round(overall_avg_holding / 60, 1),
                'tp_count': tp_count,
                'sl_count': sl_count,
                'tp_rate': round(tp_count / (tp_count + sl_count) * 100, 2) if (tp_count + sl_count) > 0 else 0,
                'best_trade': {
                    'round': best_trade['round'],
                    'ticker': best_trade['ticker'],
                    'net_pnl': float(best_trade['net_pnl']),
                    'timestamp': best_trade['timestamp']
                },
                'worst_trade': {
                    'round': worst_trade['round'],
                    'ticker': worst_trade['ticker'],
                    'net_pnl': float(worst_trade['net_pnl']),
                    'timestamp': worst_trade['timestamp']
                },
            }
            
            # 写入汇总文件
            with open(self.summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            return summary
            
        except Exception as e:
            print(f"更新汇总统计失败: {e}")
            return None
    
    def print_summary(self):
        """打印汇总统计"""
        if not self.summary_file.exists():
            print("暂无统计数据")
            return
        
        with open(self.summary_file, 'r', encoding='utf-8') as f:
            summary = json.load(f)
        
        print("\n" + "="*60)
        print(f"📊 对冲交易统计报告 - {summary['group_name']}")
        print("="*60)
        print(f"总轮数: {summary['total_rounds']}")
        print(f"总净盈亏: {summary['total_net_pnl']:.4f}")
        print(f"平均每轮净盈亏: {summary['avg_net_pnl_per_round']:.4f}")
        print(f"总对冲损耗: {summary['total_hedge_loss']:.4f}")
        print(f"平均每轮对冲损耗: {summary['avg_hedge_loss_per_round']:.4f}")
        print(f"平均持仓时间: {summary['avg_holding_minutes']:.1f} 分钟")
        print(f"止盈次数: {summary['tp_count']} ({summary['tp_rate']:.1f}%)")
        print(f"止损次数: {summary['sl_count']}")
        print(f"\n最佳交易: Round {summary['best_trade']['round']} - {summary['best_trade']['ticker']} - 盈亏: {summary['best_trade']['net_pnl']:.4f}")
        print(f"最差交易: Round {summary['worst_trade']['round']} - {summary['worst_trade']['ticker']} - 盈亏: {summary['worst_trade']['net_pnl']:.4f}")
        print("="*60 + "\n")


def generate_report(group_name: str, log_dir: str = './logs'):
    """生成并打印统计报告"""
    collector = HedgeStatsCollector(group_name, log_dir)
    collector.print_summary()


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        generate_report(sys.argv[1])
    else:
        print("用法: python hedge_stats_collector.py <group_name>")
        print("示例: python hedge_stats_collector.py BTC")

