# API降级处理说明

## 概述

对冲机器人现在支持WebSocket和REST API两种模式，实现了完整的降级处理机制。默认使用REST API模式以提高稳定性。

## 功能特性

### 1. WebSocket模式控制
- **默认禁用WebSocket**：系统默认使用REST API，更稳定可靠
- **可选启用WebSocket**：通过环境变量控制是否启用WebSocket
- **完整保留WebSocket逻辑**：所有WebSocket代码都保留，随时可以启用

### 2. 智能降级策略

系统在以下场景会自动降级到REST API：

1. **盘口价格获取** (`fetch_bbo_prices`)
   - 优先：WebSocket实时数据
   - 降级：REST API orderbook查询

2. **活动订单查询** (`get_active_orders`)
   - 优先：WebSocket内存缓存（30秒内有效）
   - 降级：REST API订单列表查询

3. **订单状态查询** (`get_order_info`)
   - 优先：WebSocket缓存数据
   - 降级：REST API订单详情查询

4. **订单成交监控** (`place_open_order`)
   - 优先：WebSocket实时推送
   - 降级：REST API轮询（0.5秒间隔）

## 使用方法

### 方式1：使用REST API模式（默认，推荐）

不需要任何额外配置，系统默认使用REST API：

```bash
# 直接运行，自动使用REST API模式
python lighter_hedge_bot_daemon.py --group BTC
```

### 方式2：启用WebSocket模式

如果需要启用WebSocket（更快但可能不太稳定），在环境变量文件中添加：

```bash
# 在 .env 文件中添加
USE_WEBSOCKET=true
```

或者在运行时设置：

```bash
USE_WEBSOCKET=true python lighter_hedge_bot_daemon.py --group BTC
```

## 环境变量说明

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `USE_WEBSOCKET` | `false` | 是否启用WebSocket（支持 true/1/yes） |
| `API_KEY_PRIVATE_KEY` | - | Lighter API密钥（必需） |
| `LIGHTER_ACCOUNT_INDEX` | `0` | Lighter账户索引 |
| `LIGHTER_API_KEY_INDEX` | `0` | Lighter API密钥索引 |

## 日志说明

系统会在日志中显示当前使用的模式：

### REST API模式
```
🔄 REST API模式已启用（WebSocket已禁用）
🔄 使用REST API获取活动订单
📡 REST API盘口: bid=67890.5, ask=67891.0
```

### WebSocket模式
```
✅ WebSocket模式已启用
📡 使用WebSocket数据 (3 订单, 更新于 2秒前)
```

### 自动降级
```
🔄 WebSocket过期或断开，降级到REST API
WebSocket价格无效，降级到REST API
```

## 性能对比

| 特性 | WebSocket模式 | REST API模式 |
|------|--------------|-------------|
| 响应速度 | 极快（实时推送） | 较快（轮询） |
| 稳定性 | 中等（可能断线） | 高（HTTP请求） |
| API调用频率 | 低（只需连接） | 中等（按需查询） |
| 适用场景 | 高频交易 | 对冲交易（推荐） |

## 修改的文件

1. **exchanges/lighter.py**
   - 添加 `use_websocket` 配置参数
   - 修改 `connect()` 方法支持可选WebSocket
   - 为所有查询方法添加REST API降级

2. **trading_bot.py**
   - 更新订单监控逻辑，兼容两种模式
   - 优化订单状态检查，自动选择最佳方式

## 故障排查

### 问题1：REST API调用频率过高
**解决方案**：启用WebSocket模式以减少API调用

### 问题2：WebSocket连接不稳定
**解决方案**：使用默认的REST API模式（推荐）

### 问题3：订单状态更新延迟
**现象**：REST API模式下，订单状态更新有0.5-1秒延迟  
**说明**：这是正常的，REST API采用轮询机制，对对冲交易影响不大

## 兼容性说明

- ✅ 完全向后兼容现有配置
- ✅ 不影响现有的.env文件
- ✅ 不影响hedge_config_multi.json配置
- ✅ 保留所有WebSocket功能，随时可启用

## 建议配置

对于对冲机器人，建议使用**REST API模式**（默认），因为：
1. 对冲交易对延迟要求不高（秒级可接受）
2. REST API更稳定，不会因网络波动断开
3. 减少了WebSocket连接管理的复杂性
4. 降低了系统维护成本

如需更快的响应速度，可以启用WebSocket模式，系统会在WebSocket失败时自动降级到REST API。

