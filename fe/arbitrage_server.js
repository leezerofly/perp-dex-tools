// ========== 多会话套利通信中继服务器 ==========
// 单个服务器管理多对GRVT-VAR客户端
// 运行方式: node arbitrage_server.js

const WebSocket = require('ws');

const PORT = 8765;
const wss = new WebSocket.Server({ port: PORT });

// 存储所有会话
const sessions = new Map();

// 订单状态枚举
const OrderStatus = {
    NONE: 'none',
    PENDING: 'pending',      // 正在发送
    ACTIVE: 'active',        // 已挂单，等待成交
    FILLED: 'filled',        // 已成交
    CANCELLED: 'cancelled',  // 已取消
    FAILED: 'failed'         // 失败
};

// 程序休眠
function sleepProgram(sessionId, durationMs) {
    const session = sessions.get(sessionId);
    if (!session) return;

    session.isRunning = false;
    log(`😴 程序进入休眠状态 ${durationMs / 1000 / 60} 分钟...`, 'warning', sessionId);

    // 通知所有客户端停止
    broadcastToSession(sessionId, { type: 'STOP' });

    // 设置定时器重新启动
    setTimeout(() => {
        log('🌅 休眠结束，重新启动程序', 'info', sessionId);
        session.isRunning = true;
        broadcastToSession(sessionId, { type: 'START' });
    }, durationMs);
}

// 创建新会话
function createSession(sessionId) {
    return {
        id: sessionId,
        name: `会话${sessions.size + 1}`,
        clients: {
            grvt: null,
            var: null,
            controller: null
        },
        isRunning: false,
        isWaitingInterval: false,  // 是否正在等待套利间隔
        intervalWaitEndTime: null, // 间隔等待结束时间

        // 仓位信息
        position: {
            isOpen: false,
            grvtOpen: false,      // GRVT是否已开仓
            varOpen: false,       // VAR是否已开仓
            grvtSide: null,       // 'long' or 'short'
            varSide: null,
            grvtEntryPrice: 0,
            varEntryPrice: 0,
            quantity: 0
        },
        
        // 订单管理 (GRVT双限价单策略)
        orders: {
            // 开仓订单
            openGrvt: {
                status: OrderStatus.NONE,
                side: null,
                price: 0,
                quantity: 0,
                createdAt: null,
                retryCount: 0
            },
            openVar: {
                status: OrderStatus.NONE,
                side: null,
                price: 0,
                quantity: 0,
                createdAt: null
            },
            // 平仓订单
            closeGrvt: {
                status: OrderStatus.NONE,
                side: null,
                price: 0,
                quantity: 0,
                createdAt: null,
                retryCount: 0
            },
            closeVar: {
                status: OrderStatus.NONE,
                side: null,
                quantity: 0,
                createdAt: null
            }
        },
        
        prices: {
            grvt: { bid: 0, ask: 0 },
            var: { bid: 0, ask: 0, spread: 0 }
        },
        
        config: {
            symbol: 'BTC/USDT',
            orderSize: 0.003,

            // === 收益阈值 (GRVT双Maker策略) ===
            // 总成本 = VAR点差(~0.05%) - GRVT双返佣(0.002%) ≈ 0.048%
            minProfitToOpen: 0,          // 开仓最小净收益率 (0=保本就开)
            minProfitToClose: 0,         // 平仓最小净收益率 (0=可以立即平)

            // GRVT费率
            grvtMakerFee: -0.00001,      // -0.001% maker返佣
            grvtTakerFee: 0.00037,       // 0.037% taker费用 (备用)

            // === 持仓时间控制 ===
            minHoldMinutes: 15,           // 最小持仓时间(分钟)
            maxHoldMinutes: 20,           // 最大持仓时间(分钟)

            // === 套利间隔控制 ===
            arbitrageIntervalSeconds: 10, // 两次套利间的间隔时间(秒)，0=无间隔

            // === 订单超时设置 ===
            orderTimeout: 30000,          // 限价单超时(ms)，超时后取消重挂
            maxRetries: 10,              // 最大重试次数
            retryDelay: 500,             // 重试间隔(ms)
            priceOffset: 0.0001          // 重挂时价格偏移(更激进)
        },
        
        pendingVarOrder: null,
        createdAt: new Date()
    };
}

console.log(`
╔══════════════════════════════════════════════════════════════════╗
║          GRVT-VAR 多会话套利服务器 v2.0                           ║
╠══════════════════════════════════════════════════════════════════╣
║  WebSocket端口: ${PORT}                                             ║
║                                                                  ║
║  特性:                                                            ║
║  • 单服务器支持多个套利会话                                         ║
║  • 每对GRVT+VAR客户端自动配对                                       ║
║  • 统一管理面板查看所有会话                                         ║
║                                                                  ║
║  客户端连接时需指定 sessionId:                                      ║
║  { type: 'REGISTER', client: 'grvt', sessionId: 'session1' }     ║
╚══════════════════════════════════════════════════════════════════╝
`);

// 日志
function log(msg, type = 'info', sessionId = null) {
    const prefix = {
        info: '📊',
        success: '✅',
        warning: '⚠️',
        error: '❌',
        trade: '💹'
    }[type] || '📊';
    const time = new Date().toLocaleTimeString('zh-CN', { hour12: false });
    const sessionTag = sessionId ? `[${sessionId}]` : '[全局]';
    console.log(`[${time}] ${prefix} ${sessionTag} ${msg}`);
}

// 广播给会话内所有客户端
function broadcastToSession(sessionId, message, excludeType = null) {
    const session = sessions.get(sessionId);
    if (!session) return;

    // 确保消息包含sessionId
    const messageWithSessionId = { ...message, sessionId };

    const msgStr = JSON.stringify(messageWithSessionId);
    Object.entries(session.clients).forEach(([type, client]) => {
        if (client && client.readyState === WebSocket.OPEN && type !== excludeType) {
            client.send(msgStr);
        }
    });
}

// 广播给所有管理控制台
function broadcastToControllers(message) {
    const msgStr = JSON.stringify(message);
    sessions.forEach((session) => {
        if (session.clients.controller && session.clients.controller.readyState === WebSocket.OPEN) {
            session.clients.controller.send(msgStr);
        }
    });
    // 也发给全局控制器
    globalControllers.forEach(ws => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(msgStr);
        }
    });
}

// 全局控制器（可查看所有会话）
const globalControllers = new Set();

// 发送给指定客户端
function sendTo(sessionId, clientType, message) {
    const session = sessions.get(sessionId);
    if (!session) return;

    const client = session.clients[clientType];
    if (client && client.readyState === WebSocket.OPEN) {
        // 确保消息包含sessionId
        const messageWithSessionId = { ...message, sessionId };
        client.send(JSON.stringify(messageWithSessionId));
    }
}

// ============================================
// GRVT双限价单策略 - 收益计算
// ============================================

/**
 * 计算完整往返套利收益（开仓+平仓，GRVT双Maker策略）
 * 
 * 费用结构:
 * - GRVT开仓: 限价单 → Maker返佣 -0.001%
 * - GRVT平仓: 限价单 → Maker返佣 -0.001%
 * - VAR开仓: 市价单 → 点差成本
 * - VAR平仓: 市价单 → 点差成本
 * 
 * 总成本 ≈ VAR点差(~0.05%) - GRVT双返佣(0.002%) ≈ 0.048%
 */
function calcRoundTripProfit(session, side, quantity) {
    const { grvt, var: varPrices } = session.prices;
    const { grvtMakerFee } = session.config;
    const varSpread = varPrices.spread || 0.0005;
    
    let grvtEntryPrice, grvtExitPrice, varEntryPrice, varExitPrice;
    
    if (side === 'grvt_buy_var_sell') {
        // GRVT做多(限价买), VAR做空(市价卖)
        grvtEntryPrice = grvt.bid;      // 限价买入挂bid
        grvtExitPrice = grvt.ask;       // 限价卖出挂ask (maker平仓)
        varEntryPrice = varPrices.bid;  // 市价卖出成交于bid
        varExitPrice = varPrices.ask;   // 市价买入平仓成交于ask
    } else {
        // GRVT做空(限价卖), VAR做多(市价买)
        grvtEntryPrice = grvt.ask;      // 限价卖出挂ask
        grvtExitPrice = grvt.bid;       // 限价买入挂bid (maker平仓)
        varEntryPrice = varPrices.ask;  // 市价买入成交于ask
        varExitPrice = varPrices.bid;   // 市价卖出平仓成交于bid
    }
    
    const grvtValue = grvtEntryPrice * quantity;
    
    // === GRVT双Maker费用 (都是返佣!) ===
    const grvtOpenRebate = Math.abs(grvtMakerFee) * grvtEntryPrice * quantity;
    const grvtCloseRebate = Math.abs(grvtMakerFee) * grvtExitPrice * quantity;
    const totalGrvtRebate = grvtOpenRebate + grvtCloseRebate;
    
    // === VAR点差成本 ===
    const varOpenSpreadCost = varEntryPrice * quantity * (varSpread / 2);
    const varCloseSpreadCost = varExitPrice * quantity * (varSpread / 2);
    const totalVarSpreadCost = varOpenSpreadCost + varCloseSpreadCost;
    
    // === 两交易所间价差收益 ===
    let crossExchangeSpread;
    if (side === 'grvt_buy_var_sell') {
        crossExchangeSpread = (varPrices.bid - grvt.bid) * quantity;
    } else {
        crossExchangeSpread = (grvt.ask - varPrices.ask) * quantity;
    }
    
    // === 总成本 (VAR点差 - GRVT双返佣) ===
    const totalCost = totalVarSpreadCost - totalGrvtRebate;
    
    // === 净收益 ===
    const netProfit = crossExchangeSpread - totalCost;
    
    return {
        side,
        grvtEntryPrice,
        grvtExitPrice,
        varEntryPrice,
        varExitPrice,
        crossExchangeSpread,
        totalGrvtRebate,
        totalVarSpreadCost,
        totalCost,
        netProfit,
        profitPercent: grvtValue > 0 ? netProfit / grvtValue : 0
    };
}

// ============================================
// 开仓逻辑
// ============================================

// 检查开仓机会 (简化版：只计算价差和方向)
function checkOpenOpportunity(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || !session.isRunning) return;

    // 如果正在等待套利间隔，跳过开仓检查
    if (session.isWaitingInterval) return;

    // 如果已有完整仓位或GRVT正在开仓，跳过
    if (session.position.isOpen || session.position.grvtOpen) return;
    if (session.orders.openGrvt.status === OrderStatus.PENDING ||
        session.orders.openGrvt.status === OrderStatus.ACTIVE) return;

    const { grvt, var: varPrices } = session.prices;
    if (!grvt.bid || !grvt.ask || !varPrices.bid || !varPrices.ask) return;

    // 计算两个方向的价差
    const grvtBuyVarSellSpread = varPrices.bid - grvt.bid;
    const grvtSellVarBuySpread = grvt.ask - varPrices.ask;

    // 判断是否有套利机会（价差为正）
    const hasOpportunity1 = grvtBuyVarSellSpread > 0;
    const hasOpportunity2 = grvtSellVarBuySpread > 0;

    if (hasOpportunity1 || hasOpportunity2) {
        let bestDirection;
        let spreadValue;

        if (hasOpportunity1 && hasOpportunity2) {
            // 两个方向都有机会，选择价差更大的
            if (grvtBuyVarSellSpread > grvtSellVarBuySpread) {
                bestDirection = 'grvt_buy_var_sell';
                spreadValue = grvtBuyVarSellSpread;
            } else {
                bestDirection = 'grvt_sell_var_buy';
                spreadValue = grvtSellVarBuySpread;
            }
        } else if (hasOpportunity1) {
            bestDirection = 'grvt_buy_var_sell';
            spreadValue = grvtBuyVarSellSpread;
        } else {
            bestDirection = 'grvt_sell_var_buy';
            spreadValue = grvtSellVarBuySpread;
        }

        log(`🎯 发现套利机会!`, 'success', sessionId);
        log(`   方向: ${bestDirection === 'grvt_buy_var_sell' ? 'GRVT买/VAR卖' : 'GRVT卖/VAR买'}`, 'info', sessionId);
        log(`   价差: $${spreadValue.toFixed(4)}`, 'info', sessionId);

        executeOpenGrvtOrder(sessionId, bestDirection, spreadValue);
    }
}

// 执行GRVT开仓限价单 (简化版：不指定价格，由GRVT客户端点击订单簿)
function executeOpenGrvtOrder(sessionId, direction, spreadValue) {
    const session = sessions.get(sessionId);
    if (!session) return;

    const grvtSide = direction === 'grvt_buy_var_sell' ? 'buy' : 'sell';
    const varSide = direction === 'grvt_buy_var_sell' ? 'sell' : 'buy';

    // 更新订单状态
    session.orders.openGrvt = {
        status: OrderStatus.PENDING,
        side: grvtSide,
        price: 0, // 不预设价格，由GRVT客户端点击订单簿获取
        quantity: session.config.orderSize,
        createdAt: Date.now(),
        retryCount: 0
    };

    // 记录待执行的VAR订单
    session.pendingVarOrder = {
        side: varSide,
        quantity: session.config.orderSize,
        orderType: 'open'
    };

    // 预设仓位信息
    session.position.grvtSide = grvtSide === 'buy' ? 'long' : 'short';
    session.position.varSide = varSide === 'buy' ? 'long' : 'short';
    session.position.quantity = session.config.orderSize;
    session.position.openTime = Date.now(); // 记录开仓时间

    log(`📤 发送GRVT开仓指令: ${grvtSide} (价差: $${spreadValue.toFixed(4)})`, 'trade', sessionId);

    sendTo(sessionId, 'grvt', {
        type: 'OPEN_POSITION',
        orderId: `open_${sessionId}_${Date.now()}`,
        side: grvtSide,
        quantity: session.config.orderSize,
        direction: direction
    });
}

// 重新挂GRVT开仓单 (超时或失败后调用)
function retryOpenGrvtOrder(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || !session.isRunning) return;

    const order = session.orders.openGrvt;
    // 如果订单已经成交，不要重试
    if (order.status === OrderStatus.FILLED) {
        log(`订单已成交，停止重试`, 'info', sessionId);
        return;
    }

    if (order.retryCount >= session.config.maxRetries) {
        log(`❌ GRVT开仓重试次数已达上限(${session.config.maxRetries})，取消开仓`, 'error', sessionId);
        resetOpenOrders(sessionId);
        return;
    }
    
    // 获取最新价格
    const { grvt } = session.prices;
    const priceOffset = session.config.priceOffset;
    
    // 更激进的价格
    let newPrice;
    if (order.side === 'buy') {
        newPrice = grvt.bid * (1 + priceOffset * (order.retryCount + 1));
    } else {
        newPrice = grvt.ask * (1 - priceOffset * (order.retryCount + 1));
    }
    
    order.price = newPrice;
    order.retryCount++;
    order.createdAt = Date.now();
    order.status = OrderStatus.PENDING;
    
    log(`🔄 重新挂GRVT开仓单 (第${order.retryCount}次): ${order.side} @ ${newPrice.toFixed(5)}`, 'warning', sessionId);
    
    sendTo(sessionId, 'grvt', {
        type: 'PLACE_LIMIT_ORDER',
        orderId: `open_${sessionId}_${Date.now()}`,
        side: order.side,
        price: newPrice,
        quantity: order.quantity
    });
}

// 重置开仓订单状态
function resetOpenOrders(sessionId) {
    const session = sessions.get(sessionId);
    if (!session) return;

    session.orders.openGrvt = {
        status: OrderStatus.NONE,
        side: null,
        price: 0,
        quantity: 0,
        createdAt: null,
        retryCount: 0
    };
    session.pendingVarOrder = null;
    session.position.grvtOpen = false;  // 重置GRVT开仓状态
    session.position.varOpen = false;   // 重置VAR开仓状态
    session.position.grvtSide = null;
    session.position.varSide = null;
    session.position.quantity = 0;
}

// ============================================
// 平仓逻辑 (GRVT限价单)
// ============================================

// 检查平仓机会 (简化版：只检查持仓时间)
function checkCloseOpportunity(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || !session.position.isOpen || !session.isRunning) return;

    // 如果正在平仓，跳过
    if (session.orders.closeGrvt.status === OrderStatus.PENDING ||
        session.orders.closeGrvt.status === OrderStatus.ACTIVE) return;

    const now = Date.now();
    const holdTimeMinutes = (now - session.position.openTime) / (1000 * 60);

    // 检查持仓时间是否达到配置的持仓时间
    const { minHoldMinutes, maxHoldMinutes } = session.config;

    if (holdTimeMinutes >= minHoldMinutes) {
        if (holdTimeMinutes >= maxHoldMinutes) {
            // 强制平仓
            log(`⏰ 持仓时间已达${maxHoldMinutes}分钟，强制平仓`, 'warning', sessionId);
        } else {
            // 正常平仓
            log(`⏰ 持仓时间已达${minHoldMinutes}分钟，开始平仓`, 'info', sessionId);
        }

        log(`   持仓时间: ${holdTimeMinutes.toFixed(1)}分钟`, 'info', sessionId);
        executeCloseGrvtOrder(sessionId);
    }
}

// 执行GRVT平仓限价单 (简化版：不指定价格，由GRVT客户端点击订单簿)
function executeCloseGrvtOrder(sessionId) {
    const session = sessions.get(sessionId);
    if (!session) return;

    const pos = session.position;
    const closeSide = pos.grvtSide === 'long' ? 'sell' : 'buy';

    // 更新平仓订单状态
    session.orders.closeGrvt = {
        status: OrderStatus.PENDING,
        side: closeSide,
        price: 0, // 不预设价格，由GRVT客户端点击订单簿获取
        quantity: pos.quantity,
        createdAt: Date.now(),
        retryCount: 0
    };

    // 记录待执行的VAR平仓订单
    session.pendingVarOrder = {
        side: closeSide,
        quantity: pos.quantity,
        orderType: 'close'
    };

    log(`📤 发送GRVT平仓指令: ${closeSide}`, 'trade', sessionId);

    sendTo(sessionId, 'grvt', {
        type: 'CLOSE_POSITION',
        orderId: `close_${sessionId}_${Date.now()}`,
        side: closeSide,
        quantity: pos.quantity
    });
}

// 重新挂GRVT平仓单
function retryCloseGrvtOrder(sessionId) {
    const session = sessions.get(sessionId);
    if (!session) return;
    
    const order = session.orders.closeGrvt;
    if (order.retryCount >= session.config.maxRetries) {
        log(`⚠️ GRVT平仓重试次数已达上限，改用市价平仓`, 'warning', sessionId);
        executeEmergencyClose(sessionId);
        return;
    }
    
    const { grvt } = session.prices;
    const pos = session.position;
    const priceOffset = session.config.priceOffset;
    
    // 更激进的价格
    let newPrice;
    if (order.side === 'sell') {
        // 卖出时降低价格
        newPrice = grvt.ask * (1 - priceOffset * (order.retryCount + 1));
    } else {
        // 买入时提高价格
        newPrice = grvt.bid * (1 + priceOffset * (order.retryCount + 1));
    }
    
    order.price = newPrice;
    order.retryCount++;
    order.createdAt = Date.now();
    order.status = OrderStatus.PENDING;
    
    log(`🔄 重新挂GRVT平仓单 (第${order.retryCount}次): ${order.side} @ ${newPrice.toFixed(5)}`, 'warning', sessionId);
    
    sendTo(sessionId, 'grvt', {
        type: 'PLACE_LIMIT_ORDER',
        orderId: `close_${sessionId}_${Date.now()}`,
        side: order.side,
        price: newPrice,
        quantity: pos.quantity
    });
}

// GRVT限价平仓 (用于VAR失败时的紧急处理)
function executeLimitCloseGrvt(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || !session.position.grvtOpen || !session.position.grvtSide) return;

    log('🚨 VAR开仓失败，执行GRVT限价平仓...', 'warning', sessionId);
    const pos = session.position;

    // 发送GRVT限价平仓指令
    sendTo(sessionId, 'grvt', {
        type: 'PLACE_LIMIT_ORDER',
        orderId: `emergency_close_${sessionId}_${Date.now()}`,
        side: pos.grvtSide === 'long' ? 'sell' : 'buy',
        quantity: pos.quantity,
        orderType: 'close'
    });

    // 等待平仓完成后休眠程序
    setTimeout(() => {
        if (session.position.grvtOpen) {
            log('GRVT仓位仍未平仓，强制休眠程序2小时', 'error', sessionId);
        } else {
            log('GRVT仓位已平仓，休眠程序2小时后重启', 'info', sessionId);
        }
        sleepProgram(sessionId, 2 * 60 * 60 * 1000); // 2小时
    }, 30000); // 30秒后检查
}

// 紧急平仓 (市价单)
function executeEmergencyClose(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || (!session.position.isOpen && !session.position.grvtOpen && !session.position.varOpen)) return;

    log('🚨 执行紧急市价平仓...', 'warning', sessionId);
    const pos = session.position;

    // GRVT市价平仓（如果GRVT已开仓）
    if (session.position.grvtOpen && pos.grvtSide && pos.quantity > 0) {
        sendTo(sessionId, 'grvt', {
            type: 'PLACE_MARKET_ORDER',
            side: pos.grvtSide === 'long' ? 'sell' : 'buy',
            quantity: pos.quantity
        });
    }

    // VAR市价平仓（如果VAR已开仓）
    if (session.position.varOpen && pos.varSide && pos.quantity > 0) {
        sendTo(sessionId, 'var', {
            type: 'PLACE_MARKET_ORDER',
            side: pos.varSide === 'long' ? 'sell' : 'buy',
            quantity: pos.quantity,
            orderType: 'close'
        });
    }

    // 重置仓位和订单
    resetPosition(sessionId);
}

// 重置仓位状态
function resetPosition(sessionId) {
    const session = sessions.get(sessionId);
    if (!session) return;

    session.position = {
        isOpen: false,
        grvtOpen: false,
        varOpen: false,
        grvtSide: null,
        varSide: null,
        grvtEntryPrice: 0,
        varEntryPrice: 0,
        quantity: 0
    };
    
    session.orders.closeGrvt = {
        status: OrderStatus.NONE,
        side: null,
        price: 0,
        quantity: 0,
        createdAt: null,
        retryCount: 0
    };
    session.orders.closeVar = {
        status: OrderStatus.NONE,
        side: null,
        quantity: 0,
        createdAt: null
    };
    
    broadcastToSession(sessionId, { type: 'POSITION_CLOSED', sessionId });
    broadcastSessionList();
}

// 获取所有会话状态
function getAllSessionsStatus() {
    const result = [];
    sessions.forEach((session, id) => {
        result.push({
            id: session.id,
            name: session.name,
            isRunning: session.isRunning,
            isWaitingInterval: session.isWaitingInterval,
            intervalWaitEndTime: session.intervalWaitEndTime,
            position: session.position,
            prices: session.prices,
            config: session.config,
            clients: {
                grvt: !!session.clients.grvt,
                var: !!session.clients.var
            },
            createdAt: session.createdAt
        });
    });
    return result;
}

// 处理WebSocket连接
wss.on('connection', (ws) => {
    log('新客户端连接', 'info');
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            handleMessage(ws, msg);
        } catch (e) {
            log(`消息解析错误: ${e.message}`, 'error');
        }
    });
    
    ws.on('close', () => {
        // 从全局控制器移除
        globalControllers.delete(ws);
        
        // 从会话中移除
        sessions.forEach((session, sessionId) => {
            Object.entries(session.clients).forEach(([type, client]) => {
                if (client === ws) {
                    session.clients[type] = null;
                    log(`${type.toUpperCase()} 客户端断开`, 'warning', sessionId);
                    
                    // 通知其他客户端
                    broadcastToSession(sessionId, {
                        type: 'CLIENT_DISCONNECTED',
                        clientType: type,
                        sessionId
                    });
                }
            });
        });
        
        broadcastSessionList();
    });
});

// 广播会话列表更新
function broadcastSessionList() {
    const sessionList = getAllSessionsStatus();
    globalControllers.forEach(ws => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'SESSION_LIST', sessions: sessionList }));
        }
    });
}

// 处理消息
function handleMessage(ws, msg) {
    const sessionId = msg.sessionId;
    
    switch (msg.type) {
        // 客户端注册
        case 'REGISTER':
            if (msg.client === 'global_controller') {
                // 全局控制器，可查看所有会话
                globalControllers.add(ws);
                log('全局控制器已连接', 'success');
                ws.send(JSON.stringify({ 
                    type: 'REGISTERED', 
                    role: 'global_controller',
                    sessions: getAllSessionsStatus()
                }));
            } else if (sessionId) {
                // 会话客户端
                if (!sessions.has(sessionId)) {
                    sessions.set(sessionId, createSession(sessionId));
                    log(`创建新会话: ${sessionId}`, 'success');
                }
                
                const session = sessions.get(sessionId);
                session.clients[msg.client] = ws;
                ws.sessionId = sessionId;
                ws.clientType = msg.client;
                
                log(`${msg.client.toUpperCase()} 客户端已注册`, 'success', sessionId);
                
                ws.send(JSON.stringify({ 
                    type: 'REGISTERED', 
                    sessionId,
                    config: session.config 
                }));
                
                // 检查是否两边都连接了
                if (session.clients.grvt && session.clients.var) {
                    log('✅ GRVT和VAR都已连接，可以开始套利', 'success', sessionId);
                    broadcastToSession(sessionId, { type: 'PAIR_READY', sessionId });
                }
                
                broadcastSessionList();
            }
            break;
        
        // 价格更新
        case 'PRICE_UPDATE':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                if (msg.source === 'grvt') {
                    session.prices.grvt = { bid: msg.bid, ask: msg.ask };
                } else if (msg.source === 'var') {
                    session.prices.var = { bid: msg.bid, ask: msg.ask, spread: msg.spread };
                }
                broadcastToSession(sessionId, { type: 'PRICES', prices: session.prices, sessionId }, msg.source);
            }
            break;
        
        // GRVT订单挂单成功
        case 'ORDER_PLACED':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                if (msg.orderType === 'open') {
                    session.orders.openGrvt.status = OrderStatus.ACTIVE;
                    // 保存实际挂单价格（从订单簿点击获取的）
                    if (msg.price) {
                        session.orders.openGrvt.price = msg.price;
                        session.position.grvtEntryPrice = msg.price;
                    }
                    log(`✓ GRVT开仓订单已挂出 @ ${msg.price}`, 'info', sessionId);
                } else if (msg.orderType === 'close') {
                    session.orders.closeGrvt.status = OrderStatus.ACTIVE;
                    // 保存实际挂单价格
                    if (msg.price) {
                        session.orders.closeGrvt.price = msg.price;
                    }
                    log(`✓ GRVT平仓订单已挂出 @ ${msg.price}`, 'info', sessionId);
                }
            }
            break;
        
        // GRVT订单挂单失败
        case 'ORDER_FAILED':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                if (msg.orderType === 'open') {
                    log(`❌ GRVT开仓订单失败: ${msg.reason}`, 'error', sessionId);
                    session.orders.openGrvt.status = OrderStatus.FAILED;
                    // 延迟后重试
                    setTimeout(() => retryOpenGrvtOrder(sessionId), session.config.retryDelay);
                } else if (msg.orderType === 'close') {
                    log(`❌ GRVT平仓订单失败: ${msg.reason}`, 'error', sessionId);
                    session.orders.closeGrvt.status = OrderStatus.FAILED;
                    // 延迟后重试
                    setTimeout(() => retryCloseGrvtOrder(sessionId), session.config.retryDelay);
                }
            }
            break;
        
        // GRVT订单成交
        case 'ORDER_FILLED':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);

                if (msg.orderType === 'open') {
                    // 检查是否已经处理过这个订单成交，避免重复处理
                    if (session.orders.openGrvt.status === OrderStatus.FILLED) {
                        log(`⚠️ GRVT开仓订单已成交，忽略重复的成交消息`, 'warning', sessionId);
                        return;
                    }

                    // 开仓订单成交
                    log(`✅ GRVT开仓成交 @ ${msg.price}`, 'success', sessionId);
                    session.orders.openGrvt.status = OrderStatus.FILLED;
                    session.position.grvtEntryPrice = msg.price;
                    session.position.grvtOpen = true;  // 标记GRVT已开仓

                    // 立即执行VAR开仓
                    if (session.pendingVarOrder) {
                        log(`📤 发送VAR开仓市价单: ${session.pendingVarOrder.side} ${session.pendingVarOrder.quantity}`, 'trade', sessionId);
                        sendTo(sessionId, 'var', {
                            type: 'PLACE_MARKET_ORDER',
                            ...session.pendingVarOrder,
                            urgent: true,
                            orderType: 'open'
                        });
                        session.pendingVarOrder = null;
                    } else {
                        log(`⚠️ GRVT开仓成交但没有待执行的VAR订单`, 'warning', sessionId);
                    }
                } else if (msg.orderType === 'close') {
                    // 检查是否已经处理过这个平仓订单成交，避免重复处理
                    if (session.orders.closeGrvt.status === OrderStatus.FILLED) {
                        log(`⚠️ GRVT平仓订单已成交，忽略重复的成交消息`, 'warning', sessionId);
                        return;
                    }

                    // 平仓订单成交
                    log(`✅ GRVT平仓成交 @ ${msg.price}`, 'success', sessionId);
                    session.orders.closeGrvt.status = OrderStatus.FILLED;

                    // 检查是否为VAR失败导致的紧急平仓
                    const isEmergencyClose = msg.orderId && msg.orderId.includes('emergency_close');

                    if (isEmergencyClose) {
                        // 紧急平仓完成，休眠程序2小时
                        log('🚨 紧急平仓完成，程序休眠2小时', 'warning', sessionId);
                        session.position.grvtOpen = false;
                        session.position.varOpen = false;
                        session.position.isOpen = false;
                        sleepProgram(sessionId, 2 * 60 * 60 * 1000); // 2小时
                    } else {
                        // 正常平仓流程，执行VAR平仓
                        const pos = session.position;
                        log(`📤 发送VAR平仓市价单...`, 'trade', sessionId);
                        sendTo(sessionId, 'var', {
                            type: 'PLACE_MARKET_ORDER',
                            side: pos.varSide === 'long' ? 'sell' : 'buy',
                            quantity: pos.quantity,
                            orderType: 'close',
                            urgent: true
                        });
                    }
                }
            }
            break;
        
        // GRVT订单取消
        case 'ORDER_CANCELLED':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                if (msg.orderType === 'open') {
                    log(`订单已取消，重新挂单...`, 'warning', sessionId);
                    session.orders.openGrvt.status = OrderStatus.CANCELLED;
                    retryOpenGrvtOrder(sessionId);
                } else if (msg.orderType === 'close') {
                    log(`平仓订单已取消，重新挂单...`, 'warning', sessionId);
                    session.orders.closeGrvt.status = OrderStatus.CANCELLED;
                    retryCloseGrvtOrder(sessionId);
                }
            }
            break;
        
        // VAR订单已提交
        case 'VAR_ORDER_SUBMITTED':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                if (msg.orderType === 'open') {
                    session.orders.openVar.status = OrderStatus.ACTIVE;
                    session.orders.openVar.side = msg.side;
                    session.orders.openVar.quantity = msg.quantity;
                    session.orders.openVar.createdAt = Date.now();
                    log(`📤 VAR开仓订单已提交，等待成交确认...`, 'info', sessionId);
                } else if (msg.orderType === 'close') {
                    session.orders.closeVar.status = OrderStatus.ACTIVE;
                    session.orders.closeVar.side = msg.side;
                    session.orders.closeVar.quantity = msg.quantity;
                    session.orders.closeVar.createdAt = Date.now();
                    log(`📤 VAR平仓订单已提交，等待成交确认...`, 'info', sessionId);
                }
            }
            break;

        // VAR订单成交
        case 'VAR_ORDER_FILLED':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);

                if (msg.orderType === 'open') {
                    log(`✅ VAR开仓成交 @ ${msg.price}`, 'success', sessionId);
                    session.orders.openVar.status = OrderStatus.FILLED;
                    session.position.varEntryPrice = msg.price;
                    session.position.varOpen = true;  // 标记VAR已开仓
                    session.position.isOpen = true;   // 完整仓位已建立

                    log(`🎉 套利仓位已建立!`, 'success', sessionId);
                    log(`   GRVT ${session.position.grvtSide} @ ${session.position.grvtEntryPrice}`, 'info', sessionId);
                    log(`   VAR ${session.position.varSide} @ ${session.position.varEntryPrice}`, 'info', sessionId);

                    broadcastToSession(sessionId, { type: 'POSITION_OPENED', position: session.position, sessionId });
                    broadcastSessionList();
                } else if (msg.orderType === 'close') {
                    log(`✅ VAR平仓成交 @ ${msg.price}`, 'success', sessionId);
                    session.orders.closeVar.status = OrderStatus.FILLED;

                    log(`🎉 套利仓位已平仓!`, 'success', sessionId);

                    // 检查是否有套利间隔设置
                    const intervalSeconds = session.config.arbitrageIntervalSeconds;
                    if (intervalSeconds > 0) {
                        log(`⏳ 开始套利间隔等待 ${intervalSeconds} 秒...`, 'info', sessionId);
                        session.isWaitingInterval = true;
                        session.intervalWaitEndTime = Date.now() + (intervalSeconds * 1000);

                        setTimeout(() => {
                            session.isWaitingInterval = false;
                            session.intervalWaitEndTime = null;
                            log(`✅ 套利间隔等待完成，可以开始下一次套利`, 'success', sessionId);
                            resetPosition(sessionId);
                        }, intervalSeconds * 1000);
                    } else {
                        resetPosition(sessionId);
                    }
                }
            }
            break;

        // VAR订单失败
        case 'VAR_ORDER_FAILED':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                const retryCount = msg.retryCount || 0;

                if (msg.orderType === 'open') {
                    log(`❌ VAR开仓订单失败: ${msg.reason} (重试${retryCount}次)`, 'error', sessionId);
                    session.orders.openVar.status = OrderStatus.FAILED;

                    // 如果重试次数未达上限，重新发送VAR开仓指令
                    if (retryCount < session.config.maxRetries) {
                        log(`🔄 重新发送VAR开仓指令...`, 'warning', sessionId);
                        setTimeout(() => {
                        sendTo(sessionId, 'var', {
                            type: 'PLACE_MARKET_ORDER',
                            side: session.pendingVarOrder?.side || msg.side,
                            quantity: session.pendingVarOrder?.quantity || msg.quantity,
                            orderType: 'open',
                            urgent: true,
                            retry: true  // 标记为重试指令
                        });
                        }, session.config.retryDelay);
                    } else {
                        log(`❌ VAR开仓重试次数已达上限，GRVT已开仓，执行限价平仓`, 'error', sessionId);

                        // 如果GRVT已经开仓成功，限价平仓GRVT仓位
                        if (session.position.grvtOpen && session.position.grvtSide) {
                            executeLimitCloseGrvt(sessionId);
                        } else {
                            // 如果GRVT还没开仓，只取消订单
                            if (session.orders.openGrvt.status === OrderStatus.ACTIVE) {
                                sendTo(sessionId, 'grvt', { type: 'CANCEL_ORDER', orderType: 'open' });
                            }
                            resetOpenOrders(sessionId);
                        }
                    }
                } else if (msg.orderType === 'close') {
                    log(`❌ VAR平仓订单失败: ${msg.reason} (重试${retryCount}次)`, 'error', sessionId);
                    session.orders.closeVar.status = OrderStatus.FAILED;

                    // 如果重试次数未达上限，重新发送VAR平仓指令
                    if (retryCount < session.config.maxRetries) {
                        log(`🔄 重新发送VAR平仓指令...`, 'warning', sessionId);
                        setTimeout(() => {
                            const pos = session.position;
                            sendTo(sessionId, 'var', {
                                type: 'PLACE_MARKET_ORDER',
                                side: pos.varSide === 'long' ? 'sell' : 'buy',
                                quantity: pos.quantity,
                                orderType: 'close',
                                urgent: true,
                                retry: true  // 标记为重试指令
                            });
                        }, session.config.retryDelay);
                    } else {
                        log(`❌ VAR平仓重试次数已达上限，使用紧急平仓`, 'error', sessionId);
                        executeEmergencyClose(sessionId);
                    }
                }
            }
            break;

        // VAR订单成交状态不确定
        case 'VAR_ORDER_UNCONFIRMED':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                log(`⚠️ VAR订单成交状态不确定，等待进一步确认...`, 'warning', sessionId);

                // 设置超时检查，如果在一定时间内还没确认，就重试
                setTimeout(() => {
                    if (msg.orderType === 'open' && session.orders.openVar.status !== OrderStatus.FILLED) {
                        log(`⏰ VAR开仓订单确认超时，重新发送...`, 'warning', sessionId);
                        sendTo(sessionId, 'var', {
                            type: 'PLACE_MARKET_ORDER',
                            side: msg.side,
                            quantity: msg.quantity,
                            orderType: 'open',
                            urgent: true,
                            retry: true  // 标记为重试指令
                        });
                    } else if (msg.orderType === 'close' && session.orders.closeVar.status !== OrderStatus.FILLED) {
                        log(`⏰ VAR平仓订单确认超时，重新发送...`, 'warning', sessionId);
                        const pos = session.position;
                        sendTo(sessionId, 'var', {
                            type: 'PLACE_MARKET_ORDER',
                            side: pos.varSide === 'long' ? 'sell' : 'buy',
                            quantity: pos.quantity,
                            orderType: 'close',
                            urgent: true,
                            retry: true  // 标记为重试指令
                        });
                    }
                }, 3000); // 3秒后检查
            }
            break;
        
        // 启动策略
        case 'START':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                session.isRunning = true;
                log('套利策略已启动', 'success', sessionId);
                broadcastToSession(sessionId, { type: 'STATUS', running: true, sessionId });
                broadcastSessionList();
            }
            break;
        
        // 停止策略
        case 'STOP':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                session.isRunning = false;
                log('套利策略已停止', 'warning', sessionId);
                broadcastToSession(sessionId, { type: 'STATUS', running: false, sessionId });
                broadcastSessionList();
            }
            break;
        
        // 紧急平仓
        case 'EMERGENCY_CLOSE':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                if (session.position.isOpen || session.position.grvtOpen || session.position.varOpen) {
                    executeEmergencyClose(sessionId);
                } else if (session.orders.openGrvt.status === OrderStatus.ACTIVE ||
                           session.orders.openGrvt.status === OrderStatus.PENDING) {
                    // 取消正在进行的开仓
                    log(`取消开仓订单...`, 'warning', sessionId);
                    sendTo(sessionId, 'grvt', { type: 'CANCEL_ORDER', orderType: 'open' });
                    resetOpenOrders(sessionId);
                }
            }
            break;
        
        // 更新配置
        case 'UPDATE_CONFIG':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                Object.assign(session.config, msg.config);
                log(`配置已更新`, 'info', sessionId);
                broadcastToSession(sessionId, { type: 'CONFIG_UPDATED', config: session.config, sessionId });
            }
            break;

        // 请求GRVT状态检查
        case 'CHECK_GRVT_STATUS':
            if (sessionId && sessions.has(sessionId)) {
                sendTo(sessionId, 'grvt', { type: 'REPORT_STATUS' });
            }
            break;

        // 客户端状态确认
        case 'CLIENT_STATUS':
            // 静默处理客户端状态确认，无需日志
            break;

        // GRVT状态报告
        case 'GRVT_STATUS_REPORT':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                const { hasPosition, hasPendingOrders, positionInfo } = msg;
                const prevGrvtOpen = session.position.grvtOpen;

                // 更新会话状态
                if (hasPosition !== undefined) {
                    session.position.grvtOpen = hasPosition;
                    session.position.isOpen = hasPosition && session.position.varOpen;  // 只有当GRVT和VAR都开仓时，才算完整仓位
                    if (positionInfo) {
                        session.position.grvtSide = positionInfo.side === 'long' ? 'long' : 'short';
                        // 只在数量合理的情况下更新（避免GRVT报告错误的价值数据）
                        if (positionInfo.size > 0 && positionInfo.size <= session.config.orderSize * 2) {
                            session.position.quantity = positionInfo.size;
                        } else {
                            log(`GRVT报告的仓位大小异常: ${positionInfo.size}，保持原有数量: ${session.position.quantity}`, 'warning', sessionId);
                        }
                        session.position.grvtEntryPrice = positionInfo.entryPrice;
                    }

                    // 如果GRVT刚刚开仓成功，且有待执行的VAR订单，立即执行VAR开仓
                    if (hasPosition && !prevGrvtOpen && session.pendingVarOrder &&
                        session.orders.openGrvt.status === OrderStatus.ACTIVE) {
                        log(`📤 GRVT仓位已确认，通过状态同步触发VAR开仓: ${session.pendingVarOrder.side} ${session.pendingVarOrder.quantity}`, 'trade', sessionId);
                        sendTo(sessionId, 'var', {
                            type: 'PLACE_MARKET_ORDER',
                            ...session.pendingVarOrder,
                            urgent: true,
                            orderType: 'open'
                        });
                        session.pendingVarOrder = null;
                        // 标记GRVT订单为已成交
                        session.orders.openGrvt.status = OrderStatus.FILLED;
                    }
                }

                // 检查订单状态
                if (hasPendingOrders !== undefined) {
                    // 如果有未完成订单，说明可能有挂单
                    if (hasPendingOrders && session.orders.openGrvt.status === OrderStatus.NONE) {
                        session.orders.openGrvt.status = OrderStatus.ACTIVE;
                    }
                }

                if (hasPosition !== prevGrvtOpen) {
                    log(`GRVT状态更新: 仓位=${hasPosition}, 未完成订单=${hasPendingOrders}`, 'info', sessionId);
                }
                broadcastSessionList();
            }
            break;
        
        // 重命名会话
        case 'RENAME_SESSION':
            if (sessionId && sessions.has(sessionId)) {
                sessions.get(sessionId).name = msg.name;
                log(`会话重命名为: ${msg.name}`, 'info', sessionId);
                broadcastSessionList();
            }
            break;
        
        // 获取会话状态
        case 'GET_STATUS':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                ws.send(JSON.stringify({
                    type: 'FULL_STATUS',
                    sessionId,
                    state: {
                        isRunning: session.isRunning,
                        position: session.position,
                        prices: session.prices,
                        config: session.config,
                        clients: {
                            grvt: !!session.clients.grvt,
                            var: !!session.clients.var
                        }
                    }
                }));
            }
            break;
        
        // 获取所有会话
        case 'GET_ALL_SESSIONS':
            ws.send(JSON.stringify({
                type: 'SESSION_LIST',
                sessions: getAllSessionsStatus()
            }));
            break;
        
        // 删除会话
        case 'DELETE_SESSION':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                // 断开所有客户端
                Object.values(session.clients).forEach(client => {
                    if (client && client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({ type: 'SESSION_DELETED', sessionId }));
                        client.close();
                    }
                });
                sessions.delete(sessionId);
                log(`会话已删除`, 'warning', sessionId);
                broadcastSessionList();
            }
            break;
    }
}

// ============================================
// 定时任务
// ============================================

// 检查套利机会 (每100ms)
setInterval(() => {
    sessions.forEach((session, sessionId) => {
        if (session.isRunning && session.clients.grvt && session.clients.var) {
            if (session.position.isOpen) {
                checkCloseOpportunity(sessionId);
            } else {
                checkOpenOpportunity(sessionId);
            }
        }
    });
}, 100);

// 检查订单超时 (每500ms)
setInterval(() => {
    const now = Date.now();

    sessions.forEach((session, sessionId) => {
        if (!session.isRunning) return;

        // 检查GRVT开仓订单超时
        const openGrvtOrder = session.orders.openGrvt;
        if (openGrvtOrder.status === OrderStatus.ACTIVE && openGrvtOrder.createdAt) {
            const elapsed = now - openGrvtOrder.createdAt;
            if (elapsed > session.config.orderTimeout) {
                log(`⏰ GRVT开仓订单超时(${(elapsed/1000).toFixed(1)}s)，取消重挂...`, 'warning', sessionId);
                sendTo(sessionId, 'grvt', { type: 'CANCEL_ORDER', orderType: 'open' });
                openGrvtOrder.status = OrderStatus.CANCELLED;
                // 延迟后重挂
                setTimeout(() => retryOpenGrvtOrder(sessionId), session.config.retryDelay);
            }
        }

        // 检查GRVT平仓订单超时
        const closeGrvtOrder = session.orders.closeGrvt;
        if (closeGrvtOrder.status === OrderStatus.ACTIVE && closeGrvtOrder.createdAt) {
            const elapsed = now - closeGrvtOrder.createdAt;
            if (elapsed > session.config.orderTimeout) {
                log(`⏰ GRVT平仓订单超时(${(elapsed/1000).toFixed(1)}s)，取消重挂...`, 'warning', sessionId);
                sendTo(sessionId, 'grvt', { type: 'CANCEL_ORDER', orderType: 'close' });
                closeGrvtOrder.status = OrderStatus.CANCELLED;
                // 延迟后重挂
                setTimeout(() => retryCloseGrvtOrder(sessionId), session.config.retryDelay);
            }
        }

        // 检查VAR开仓订单超时
        const openVarOrder = session.orders.openVar;
        if (openVarOrder.status === OrderStatus.ACTIVE && openVarOrder.createdAt) {
            const elapsed = now - openVarOrder.createdAt;
            if (elapsed > 3000) { // VAR订单3秒超时
                log(`⏰ VAR开仓订单超时(${(elapsed/1000).toFixed(1)}s)，重新发送...`, 'warning', sessionId);
                openVarOrder.status = OrderStatus.NONE;
                // 重新发送VAR开仓指令
                if (session.pendingVarOrder) {
                    sendTo(sessionId, 'var', {
                        type: 'PLACE_MARKET_ORDER',
                        ...session.pendingVarOrder,
                        urgent: true,
                        retry: true  // 标记为重试指令
                    });
                }
            }
        }

        // 检查VAR平仓订单超时
        const closeVarOrder = session.orders.closeVar;
        if (closeVarOrder.status === OrderStatus.ACTIVE && closeVarOrder.createdAt) {
            const elapsed = now - closeVarOrder.createdAt;
            if (elapsed > 3000) { // VAR订单3秒超时
                log(`⏰ VAR平仓订单超时(${(elapsed/1000).toFixed(1)}s)，重新发送...`, 'warning', sessionId);
                closeVarOrder.status = OrderStatus.NONE;
                // 重新发送VAR平仓指令
                const pos = session.position;
                if (pos.isOpen) {
                    sendTo(sessionId, 'var', {
                        type: 'PLACE_MARKET_ORDER',
                        side: pos.varSide === 'long' ? 'sell' : 'buy',
                        quantity: pos.quantity,
                        orderType: 'close',
                        urgent: true,
                        retry: true  // 标记为重试指令
                    });
                }
            }
        }
    });
}, 500);

// 定时广播状态 (每1秒)
setInterval(() => {
    sessions.forEach((session, sessionId) => {
        broadcastToSession(sessionId, {
            type: 'HEARTBEAT',
            sessionId,
            position: session.position,
            orders: {
                openGrvt: session.orders.openGrvt.status,
                closeGrvt: session.orders.closeGrvt.status
            },
            running: session.isRunning
        });
    });
}, 1000);

// 定时广播会话列表 (每2秒)
setInterval(() => {
    broadcastSessionList();
}, 2000);

// 定时检查所有会话的GRVT状态 (每10秒)
setInterval(() => {
    sessions.forEach((session, sessionId) => {
        if (session.clients.grvt && session.clients.grvt.readyState === WebSocket.OPEN) {
            sendTo(sessionId, 'grvt', { type: 'REPORT_STATUS' });
        }
    });
}, 10000);

log(`服务器已启动，等待客户端连接...`, 'success');
log(`策略: GRVT双限价单(双Maker返佣) + VAR市价单`, 'info');
log(`预计成本: ~0.048% (VAR点差 - GRVT双返佣)`, 'info');
log(`状态检查: 每10秒同步一次GRVT仓位和订单信息`, 'info');
