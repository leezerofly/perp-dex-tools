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
        
        // 仓位信息
        position: {
            isOpen: false,
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
    
    const msgStr = JSON.stringify(message);
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
        client.send(JSON.stringify(message));
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

// 检查开仓机会
function checkOpenOpportunity(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || !session.isRunning) return;
    
    // 如果已有仓位或正在开仓，跳过
    if (session.position.isOpen) return;
    if (session.orders.openGrvt.status === OrderStatus.PENDING || 
        session.orders.openGrvt.status === OrderStatus.ACTIVE) return;
    
    const { grvt, var: varPrices } = session.prices;
    if (!grvt.bid || !grvt.ask || !varPrices.bid || !varPrices.ask) return;
    
    // 计算两个方向的完整往返收益
    const profit1 = calcRoundTripProfit(session, 'grvt_buy_var_sell', session.config.orderSize);
    const profit2 = calcRoundTripProfit(session, 'grvt_sell_var_buy', session.config.orderSize);
    
    // 选择收益更高的策略
    const bestStrategy = profit1.netProfit > profit2.netProfit ? profit1 : profit2;
    
    // 判断是否满足开仓条件
    if (bestStrategy.profitPercent >= session.config.minProfitToOpen) {
        log(`🎯 发现套利机会!`, 'success', sessionId);
        log(`   方向: ${bestStrategy.side === 'grvt_buy_var_sell' ? 'GRVT买/VAR卖' : 'GRVT卖/VAR买'}`, 'info', sessionId);
        log(`   价差收益: $${bestStrategy.crossExchangeSpread.toFixed(4)}`, 'info', sessionId);
        log(`   GRVT双返佣: +$${bestStrategy.totalGrvtRebate.toFixed(4)}`, 'info', sessionId);
        log(`   VAR点差: -$${bestStrategy.totalVarSpreadCost.toFixed(4)}`, 'info', sessionId);
        log(`   净收益: $${bestStrategy.netProfit.toFixed(4)} (${(bestStrategy.profitPercent * 100).toFixed(4)}%)`, 'success', sessionId);
        
        executeOpenGrvtOrder(sessionId, bestStrategy);
    }
}

// 执行GRVT开仓限价单
function executeOpenGrvtOrder(sessionId, strategy) {
    const session = sessions.get(sessionId);
    if (!session) return;
    
    const grvtSide = strategy.side === 'grvt_buy_var_sell' ? 'buy' : 'sell';
    const varSide = strategy.side === 'grvt_buy_var_sell' ? 'sell' : 'buy';
    
    // 更新订单状态
    session.orders.openGrvt = {
        status: OrderStatus.PENDING,
        side: grvtSide,
        price: strategy.grvtEntryPrice,
        quantity: session.config.orderSize,
        createdAt: Date.now(),
        retryCount: 0
    };
    
    // 记录待执行的VAR订单
    session.pendingVarOrder = {
        side: varSide,
        quantity: session.config.orderSize
    };
    
    // 预设仓位信息
    session.position.grvtSide = grvtSide === 'buy' ? 'long' : 'short';
    session.position.varSide = varSide === 'buy' ? 'long' : 'short';
    session.position.quantity = session.config.orderSize;
    
    log(`📤 发送GRVT开仓限价单: ${grvtSide} @ ${strategy.grvtEntryPrice}`, 'trade', sessionId);
    
    sendTo(sessionId, 'grvt', {
        type: 'PLACE_LIMIT_ORDER',
        orderId: `open_${sessionId}_${Date.now()}`,
        side: grvtSide,
        price: strategy.grvtEntryPrice,
        quantity: session.config.orderSize
    });
}

// 重新挂GRVT开仓单 (超时或失败后调用)
function retryOpenGrvtOrder(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || !session.isRunning) return;
    
    const order = session.orders.openGrvt;
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
    session.position.grvtSide = null;
    session.position.varSide = null;
    session.position.quantity = 0;
}

// ============================================
// 平仓逻辑 (GRVT限价单)
// ============================================

// 检查平仓机会
function checkCloseOpportunity(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || !session.position.isOpen || !session.isRunning) return;
    
    // 如果正在平仓，跳过
    if (session.orders.closeGrvt.status === OrderStatus.PENDING || 
        session.orders.closeGrvt.status === OrderStatus.ACTIVE) return;
    
    const { grvt, var: varPrices } = session.prices;
    const pos = session.position;
    const varSpread = varPrices.spread || 0.0005;
    const { grvtMakerFee } = session.config;
    
    // 计算当前平仓价格 (GRVT用限价单，挂在bid/ask)
    let grvtClosePrice, varClosePrice;
    if (pos.grvtSide === 'long') {
        grvtClosePrice = grvt.ask;      // 限价卖出挂ask (maker)
        varClosePrice = varPrices.ask;   // 市价买入
    } else {
        grvtClosePrice = grvt.bid;       // 限价买入挂bid (maker)
        varClosePrice = varPrices.bid;   // 市价卖出
    }
    
    const grvtValue = pos.grvtEntryPrice * pos.quantity;
    
    // === GRVT双返佣 ===
    const grvtOpenRebate = grvtValue * Math.abs(grvtMakerFee);
    const grvtCloseRebate = grvtClosePrice * pos.quantity * Math.abs(grvtMakerFee);
    const totalGrvtRebate = grvtOpenRebate + grvtCloseRebate;
    
    // === VAR点差成本 ===
    const varOpenSpreadCost = pos.varEntryPrice * pos.quantity * (varSpread / 2);
    const varCloseSpreadCost = varClosePrice * pos.quantity * (varSpread / 2);
    const totalVarSpreadCost = varOpenSpreadCost + varCloseSpreadCost;
    
    // === 仓位盈亏 ===
    let grvtPnL, varPnL;
    if (pos.grvtSide === 'long') {
        grvtPnL = (grvtClosePrice - pos.grvtEntryPrice) * pos.quantity;
        varPnL = (pos.varEntryPrice - varClosePrice) * pos.quantity;
    } else {
        grvtPnL = (pos.grvtEntryPrice - grvtClosePrice) * pos.quantity;
        varPnL = (varClosePrice - pos.varEntryPrice) * pos.quantity;
    }
    
    // === 净收益 (双Maker策略) ===
    const totalCost = totalVarSpreadCost - totalGrvtRebate;
    const netProfit = grvtPnL + varPnL - totalCost;
    const profitPercent = netProfit / grvtValue;
    
    // 定期输出当前状态 (降低频率避免刷屏)
    if (Math.random() < 0.05) {
        log(`📊 仓位状态 - 净收益: $${netProfit.toFixed(4)} (${(profitPercent * 100).toFixed(4)}%)`, 'info', sessionId);
    }
    
    // 平仓条件：满足收益阈值
    if (profitPercent >= session.config.minProfitToClose) {
        log(`💰 达到平仓条件!`, 'success', sessionId);
        log(`   GRVT PnL: $${grvtPnL.toFixed(4)}`, 'info', sessionId);
        log(`   VAR PnL: $${varPnL.toFixed(4)}`, 'info', sessionId);
        log(`   GRVT双返佣: +$${totalGrvtRebate.toFixed(4)}`, 'info', sessionId);
        log(`   VAR点差: -$${totalVarSpreadCost.toFixed(4)}`, 'info', sessionId);
        log(`   净收益: $${netProfit.toFixed(4)} (${(profitPercent * 100).toFixed(4)}%)`, 'success', sessionId);
        executeCloseGrvtOrder(sessionId);
    }
}

// 执行GRVT平仓限价单
function executeCloseGrvtOrder(sessionId) {
    const session = sessions.get(sessionId);
    if (!session) return;
    
    const pos = session.position;
    const { grvt } = session.prices;
    const closeSide = pos.grvtSide === 'long' ? 'sell' : 'buy';
    const closePrice = pos.grvtSide === 'long' ? grvt.ask : grvt.bid;
    
    // 更新平仓订单状态
    session.orders.closeGrvt = {
        status: OrderStatus.PENDING,
        side: closeSide,
        price: closePrice,
        quantity: pos.quantity,
        createdAt: Date.now(),
        retryCount: 0
    };
    
    log(`📤 发送GRVT平仓限价单: ${closeSide} @ ${closePrice}`, 'trade', sessionId);
    
    sendTo(sessionId, 'grvt', {
        type: 'PLACE_LIMIT_ORDER',
        orderId: `close_${sessionId}_${Date.now()}`,
        side: closeSide,
        price: closePrice,
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

// 紧急平仓 (市价单)
function executeEmergencyClose(sessionId) {
    const session = sessions.get(sessionId);
    if (!session || !session.position.isOpen) return;
    
    log('🚨 执行紧急市价平仓...', 'warning', sessionId);
    const pos = session.position;
    
    // GRVT市价平仓
    sendTo(sessionId, 'grvt', {
        type: 'PLACE_MARKET_ORDER',
        side: pos.grvtSide === 'long' ? 'sell' : 'buy',
        quantity: pos.quantity
    });
    
    // VAR市价平仓
    sendTo(sessionId, 'var', {
        type: 'PLACE_MARKET_ORDER',
        side: pos.varSide === 'long' ? 'sell' : 'buy',
        quantity: pos.quantity
    });
    
    // 重置仓位和订单
    resetPosition(sessionId);
}

// 重置仓位状态
function resetPosition(sessionId) {
    const session = sessions.get(sessionId);
    if (!session) return;
    
    session.position = {
        isOpen: false,
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
                    log(`✓ GRVT开仓订单已挂出`, 'info', sessionId);
                } else if (msg.orderType === 'close') {
                    session.orders.closeGrvt.status = OrderStatus.ACTIVE;
                    log(`✓ GRVT平仓订单已挂出`, 'info', sessionId);
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
                    // 开仓订单成交
                    log(`✅ GRVT开仓成交 @ ${msg.price}`, 'success', sessionId);
                    session.orders.openGrvt.status = OrderStatus.FILLED;
                    session.position.grvtEntryPrice = msg.price;
                    
                    // 立即执行VAR开仓
                    if (session.pendingVarOrder) {
                        log(`📤 发送VAR开仓市价单...`, 'trade', sessionId);
                        sendTo(sessionId, 'var', {
                            type: 'PLACE_MARKET_ORDER',
                            ...session.pendingVarOrder,
                            urgent: true
                        });
                        session.pendingVarOrder = null;
                    }
                } else if (msg.orderType === 'close') {
                    // 平仓订单成交
                    log(`✅ GRVT平仓成交 @ ${msg.price}`, 'success', sessionId);
                    session.orders.closeGrvt.status = OrderStatus.FILLED;
                    
                    // 立即执行VAR平仓
                    const pos = session.position;
                    log(`📤 发送VAR平仓市价单...`, 'trade', sessionId);
                    sendTo(sessionId, 'var', {
                        type: 'PLACE_MARKET_ORDER',
                        side: pos.varSide === 'long' ? 'sell' : 'buy',
                        quantity: pos.quantity,
                        urgent: true
                    });
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
        
        // VAR订单成交
        case 'VAR_ORDER_FILLED':
            if (sessionId && sessions.has(sessionId)) {
                const session = sessions.get(sessionId);
                
                if (msg.orderType === 'open') {
                    log(`✅ VAR开仓成交 @ ${msg.price}`, 'success', sessionId);
                    session.orders.openVar.status = OrderStatus.FILLED;
                    session.position.varEntryPrice = msg.price;
                    session.position.isOpen = true;
                    
                    log(`🎉 套利仓位已建立!`, 'success', sessionId);
                    log(`   GRVT ${session.position.grvtSide} @ ${session.position.grvtEntryPrice}`, 'info', sessionId);
                    log(`   VAR ${session.position.varSide} @ ${session.position.varEntryPrice}`, 'info', sessionId);
                    
                    broadcastToSession(sessionId, { type: 'POSITION_OPENED', position: session.position, sessionId });
                    broadcastSessionList();
                } else if (msg.orderType === 'close') {
                    log(`✅ VAR平仓成交 @ ${msg.price}`, 'success', sessionId);
                    session.orders.closeVar.status = OrderStatus.FILLED;
                    
                    log(`🎉 套利仓位已平仓!`, 'success', sessionId);
                    resetPosition(sessionId);
                }
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
                if (session.position.isOpen) {
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
        
        // 检查开仓订单超时
        const openOrder = session.orders.openGrvt;
        if (openOrder.status === OrderStatus.ACTIVE && openOrder.createdAt) {
            const elapsed = now - openOrder.createdAt;
            if (elapsed > session.config.orderTimeout) {
                log(`⏰ GRVT开仓订单超时(${(elapsed/1000).toFixed(1)}s)，取消重挂...`, 'warning', sessionId);
                sendTo(sessionId, 'grvt', { type: 'CANCEL_ORDER', orderType: 'open' });
                openOrder.status = OrderStatus.CANCELLED;
                // 延迟后重挂
                setTimeout(() => retryOpenGrvtOrder(sessionId), session.config.retryDelay);
            }
        }
        
        // 检查平仓订单超时
        const closeOrder = session.orders.closeGrvt;
        if (closeOrder.status === OrderStatus.ACTIVE && closeOrder.createdAt) {
            const elapsed = now - closeOrder.createdAt;
            if (elapsed > session.config.orderTimeout) {
                log(`⏰ GRVT平仓订单超时(${(elapsed/1000).toFixed(1)}s)，取消重挂...`, 'warning', sessionId);
                sendTo(sessionId, 'grvt', { type: 'CANCEL_ORDER', orderType: 'close' });
                closeOrder.status = OrderStatus.CANCELLED;
                // 延迟后重挂
                setTimeout(() => retryCloseGrvtOrder(sessionId), session.config.retryDelay);
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

log(`服务器已启动，等待客户端连接...`, 'success');
log(`策略: GRVT双限价单(双Maker返佣) + VAR市价单`, 'info');
log(`预计成本: ~0.048% (VAR点差 - GRVT双返佣)`, 'info');
