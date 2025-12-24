// ========== VAR 标签页客户端脚本 (多会话版) ==========
// 在VAR (Variational) 交易页面的控制台运行此脚本
// 需要指定 SESSION_ID 与对应的GRVT客户端配对

(function() {
    'use strict';
    
    // ===== 配置区域 - 修改这里的SESSION_ID =====
    const SESSION_ID = 'session1';  // 🔴 修改此ID与GRVT客户端匹配
    // ==========================================
    
    const WS_URL = 'ws://localhost:8765';
    let ws = null;
    let reconnectTimer = null;
    
    function log(msg, type = 'info') {
        const prefix = {
            info: '📊',
            success: '✅',
            warning: '⚠️',
            error: '❌',
            trade: '💹',
            ws: '🔌'
        }[type] || '📊';
        const time = new Date().toLocaleTimeString('zh-CN', { hour12: false });
        console.log(`[VAR][${SESSION_ID}][${time}] ${prefix} ${msg}`);
    }
    
    function connect() {
        if (ws && ws.readyState === WebSocket.OPEN) return;

        ws = new WebSocket(WS_URL);
        
        ws.onopen = () => {
            log('已连接到套利服务器', 'success');
            ws.send(JSON.stringify({
                type: 'REGISTER',
                client: 'var',
                sessionId: SESSION_ID
            }));
            startPriceUpdates();
        };

        ws.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data);
                handleMessage(msg);
            } catch (e) {
                log(`消息解析错误: ${e.message}`, 'error');
            }
        };

        ws.onclose = () => {
            log('与服务器断开连接，5秒后重连...', 'warning');
            reconnectTimer = setTimeout(connect, 5000);
        };

        ws.onerror = () => {
            log('WebSocket连接错误', 'error');
        };
    }
    
    // 判断当前订单类型 (开仓或平仓)
    let currentOrderType = null;
    
    function handleMessage(msg) {
        // 只处理自己会话的消息
        if (msg.sessionId && msg.sessionId !== SESSION_ID) return;
        
        switch (msg.type) {
            case 'REGISTERED':
                log(`已注册到会话: ${SESSION_ID}`, 'success');
                break;

            case 'HEARTBEAT':
                // 服务器心跳，不需要响应
                break;
            
            case 'PAIR_READY':
                log('🎉 GRVT客户端已连接，可以开始套利!', 'success');
                break;
            
            case 'PLACE_MARKET_ORDER':
                // 根据上下文判断订单类型
                currentOrderType = msg.orderType || (msg.urgent ? 'open' : 'close');
                log(`🔥 收到${currentOrderType === 'open' ? '开仓' : '平仓'}市价单指令: ${msg.side}, 数量: ${msg.quantity}${msg.urgent ? ' [紧急]' : ''}`, 'trade');
                log(`消息详情: ${JSON.stringify(msg)}`, 'info');
                placeMarketOrder(msg.side, msg.quantity, currentOrderType);
                break;
            
            case 'STATUS':
                log(`策略状态: ${msg.running ? '运行中' : '已停止'}`, 'info');
                break;
            
            case 'SESSION_DELETED':
                log('会话已被删除', 'warning');
                if (ws) ws.close();
                break;
            
            case 'CLIENT_DISCONNECTED':
                if (msg.clientType === 'grvt') {
                    log('⚠️ GRVT客户端断开连接', 'warning');
                }
                break;
        }
    }
    
    function getPrices() {
        const askEl = document.querySelector('[data-testid="ask-price-display"]');
        const bidEl = document.querySelector('[data-testid="bid-price-display"]');
        const spreadEl = document.querySelector('[data-testid="percent-spread"]');
        
        const ask = askEl ? parseFloat(askEl.textContent.replace(/[^0-9.]/g, '')) : null;
        const bid = bidEl ? parseFloat(bidEl.textContent.replace(/[^0-9.]/g, '')) : null;
        const spreadText = spreadEl ? spreadEl.textContent : '0';
        const spread = parseFloat(spreadText.replace(/[^0-9.]/g, '')) / 100 || 0;
        
        return { ask, bid, spread };
    }
    
    let priceUpdateInterval = null;
    let connectionCheckInterval = null;

    function startPriceUpdates() {
        if (priceUpdateInterval) clearInterval(priceUpdateInterval);
        if (connectionCheckInterval) clearInterval(connectionCheckInterval);

        priceUpdateInterval = setInterval(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                const prices = getPrices();
                if (prices.bid && prices.ask) {
                    ws.send(JSON.stringify({
                        type: 'PRICE_UPDATE',
                        source: 'var',
                        sessionId: SESSION_ID,
                        bid: prices.bid,
                        ask: prices.ask,
                        spread: prices.spread
                    }));
                }
            }
        }, 200);

        // 每30秒发送一次连接状态确认
        connectionCheckInterval = setInterval(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    type: 'CLIENT_STATUS',
                    source: 'var',
                    sessionId: SESSION_ID,
                    status: 'connected',
                    readyState: ws.readyState
                }));
            } else {
                log('连接断开，尝试重连...', 'warning');
                connect();
            }
        }, 30000);
    }
    
    function simulateInput(input, value) {
        const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
        nativeInputValueSetter.call(input, value);
        input.dispatchEvent(new Event('input', { bubbles: true }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
    }
    
    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    
    // VAR订单状态跟踪
    let currentVarOrder = {
        orderId: null,
        orderType: null,
        side: null,
        quantity: 0,
        retryCount: 0,
        maxRetries: 5,  // VAR订单最大重试次数
        createdAt: null,
        status: 'none'
    };

    async function placeMarketOrder(side, quantity, orderType = 'open') {
        log(`执行${orderType === 'open' ? '开仓' : '平仓'}市价单: ${side}, 数量: ${quantity}`, 'trade');

        // 更新当前订单状态
        const orderId = `var_${orderType}_${Date.now()}`;
        currentVarOrder = {
            orderId: orderId,
            orderType: orderType,
            side: side,
            quantity: quantity,
            retryCount: currentVarOrder.retryCount || 0,
            createdAt: Date.now(),
            status: 'pending'
        };

        try {
            // 1. 确保是市价单模式
            const marketBtn = Array.from(document.querySelectorAll('[data-testid="toggle-select"] button'))
                .find(btn => btn.textContent.includes('市价'));
            if (marketBtn && !marketBtn.classList.contains('pointer-events-none')) {
                marketBtn.click();
                await sleep(100);
            }

            // 2. 选择买/卖方向
            const sideButtons = document.querySelectorAll('[role="switch"] button');
            let sideClicked = false;
            for (const btn of sideButtons) {
                const text = btn.textContent.trim();
                if ((side === 'buy' && text.startsWith('买')) ||
                    (side === 'sell' && text.startsWith('卖'))) {
                    btn.click();
                    sideClicked = true;
                    await sleep(100);
                    break;
                }
            }

            if (!sideClicked) {
                throw new Error('无法选择买卖方向');
            }

            // 3. 输入数量
            const qtyInput = document.querySelector('[data-testid="quantity-input"]');
            if (qtyInput) {
                qtyInput.focus();
                simulateInput(qtyInput, quantity.toString());
                await sleep(150);
            }

            // 4. 点击提交
            await sleep(100);
            const submitBtn = document.querySelector('[data-testid="submit-button"]');
            if (submitBtn && !submitBtn.disabled) {
                const currentPrices = getPrices();
                const fillPrice = side === 'buy' ? currentPrices.ask : currentPrices.bid;

                submitBtn.click();
                log(`🎉 ${orderType === 'open' ? '开仓' : '平仓'}${side === 'buy' ? '买入' : '卖出'}市价单已点击提交按钮`, 'success');
                currentVarOrder.status = 'submitted';

                // 通知服务器订单已提交
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        type: 'VAR_ORDER_SUBMITTED',
                        source: 'var',
                        sessionId: SESSION_ID,
                        orderId: orderId,
                        orderType: orderType,
                        side: side,
                        quantity: quantity,
                        expectedPrice: fillPrice
                    }));
                }

                // 等待成交确认
                await sleep(500);

                // 检查是否真的成交了（这里需要更准确的成交检测）
                const hasFilled = await checkOrderFilled(orderType);

                if (hasFilled) {
                    log(`✅ VAR订单已确认成交`, 'success');
                    currentVarOrder.status = 'filled';

                    // 通知服务器成交
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({
                            type: 'VAR_ORDER_FILLED',
                            source: 'var',
                            sessionId: SESSION_ID,
                            orderId: orderId,
                            orderType: orderType,
                            side: side,
                            price: fillPrice,
                            quantity: quantity
                        }));
                    }
                    return true;
                } else {
                    // 可能成交失败或延迟，通知服务器
                    log(`⚠️ VAR订单成交状态不确定`, 'warning');
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({
                            type: 'VAR_ORDER_UNCONFIRMED',
                            source: 'var',
                            sessionId: SESSION_ID,
                            orderId: orderId,
                            orderType: orderType,
                            side: side,
                            quantity: quantity
                        }));
                    }
                    return false;
                }

            } else {
                // 按钮不可用，尝试重试
                if (currentVarOrder.retryCount < currentVarOrder.maxRetries) {
                    log(`提交按钮不可用，第${currentVarOrder.retryCount + 1}次重试...`, 'warning');
                    currentVarOrder.retryCount++;
                    await sleep(500);
                    return await placeMarketOrder(side, quantity, orderType);
                } else {
                    throw new Error('提交按钮不可用，重试次数已达上限');
                }
            }
        } catch (e) {
            log(`下单失败: ${e.message}`, 'error');

            // 通知服务器订单失败
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    type: 'VAR_ORDER_FAILED',
                    source: 'var',
                    sessionId: SESSION_ID,
                    orderId: currentVarOrder.orderId,
                    orderType: orderType,
                    side: side,
                    quantity: quantity,
                    reason: e.message,
                    retryCount: currentVarOrder.retryCount
                }));
            }

            return false;
        }
    }

    // 检查订单是否成交
    async function checkOrderFilled(orderType) {
        // 记录检查开始时的仓位状态
        const initialPositions = getCurrentPositions();
        await sleep(500); // 等待交易执行

        let attempts = 0;
        const maxAttempts = 10; // 最多检查10次
        const checkInterval = 500; // 每500ms检查一次

        while (attempts < maxAttempts) {
            await sleep(checkInterval);
            const currentPositions = getCurrentPositions();

            // 检查是否有仓位变化
            if (orderType === 'open') {
                // 开仓：检查是否新增了仓位
                if (hasPositionOpened(initialPositions, currentPositions)) {
                    log(`✅ 检测到开仓成交，仓位已建立`, 'success');
                    return true;
                }
            } else if (orderType === 'close') {
                // 平仓：检查仓位是否减少或清零
                if (hasPositionClosed(initialPositions, currentPositions)) {
                    log(`✅ 检测到平仓成交，仓位已关闭`, 'success');
                    return true;
                }
            }

            attempts++;
        }

        log(`⚠️ 未检测到仓位变化，可能成交失败或延迟`, 'warning');
        return false;
    }

    // 获取当前仓位信息
    function getCurrentPositions() {
        const positions = [];

        // 查找仓位表格中的行
        const positionRows = document.querySelectorAll('[data-testid*="position-row"], tr, .position-item');

        // 如果没找到特定的选择器，尝试查找包含交易对信息的元素
        if (positionRows.length === 0) {
            // 查找所有包含"XRP"或其他交易对名称的元素
            const allElements = document.querySelectorAll('*');
            for (const el of allElements) {
                const text = el.textContent || '';
                if (text.includes('XRP') && text.match(/[\d.]+/)) {
                    // 尝试提取数量信息
                    const qtyMatch = text.match(/([\d.]+)\s*XRP/);
                    if (qtyMatch) {
                        positions.push({
                            symbol: 'XRP',
                            quantity: parseFloat(qtyMatch[1]),
                            element: el
                        });
                    }
                }
            }
        } else {
            // 解析表格行
            positionRows.forEach(row => {
                const cells = row.querySelectorAll('td, div');
                if (cells.length >= 2) {
                    const symbol = cells[0]?.textContent?.trim();
                    const qtyText = cells[1]?.textContent?.trim();
                    if (symbol && qtyText) {
                        const quantity = parseFloat(qtyText.replace(/[^\d.-]/g, ''));
                        if (!isNaN(quantity) && quantity !== 0) {
                            positions.push({
                                symbol: symbol.replace('/USDT', '').replace('PERP', ''),
                                quantity: Math.abs(quantity),
                                element: row
                            });
                        }
                    }
                }
            });
        }

        return positions;
    }

    // 检查是否开仓成功（新增了仓位）
    function hasPositionOpened(initialPositions, currentPositions) {
        // 如果初始时没有仓位，现在有了仓位
        if (initialPositions.length === 0 && currentPositions.length > 0) {
            return true;
        }

        // 如果初始仓位数量为0，现在有非零仓位
        const initialTotalQty = initialPositions.reduce((sum, pos) => sum + Math.abs(pos.quantity), 0);
        const currentTotalQty = currentPositions.reduce((sum, pos) => sum + Math.abs(pos.quantity), 0);

        return currentTotalQty > initialTotalQty;
    }

    // 检查是否平仓成功（仓位减少或清零）
    function hasPositionClosed(initialPositions, currentPositions) {
        const initialTotalQty = initialPositions.reduce((sum, pos) => sum + Math.abs(pos.quantity), 0);
        const currentTotalQty = currentPositions.reduce((sum, pos) => sum + Math.abs(pos.quantity), 0);

        // 仓位总量减少
        return currentTotalQty < initialTotalQty;
    }
    
    function init() {
        log(`VAR套利客户端已加载 (会话: ${SESSION_ID})`, 'success');
        log('正在连接套利服务器...', 'info');
        connect();
    }
    
    window.VAR_ARB = {
        SESSION_ID,
        connect,
        disconnect: () => { if (ws) ws.close(); },
        getStatus: () => ({
            sessionId: SESSION_ID,
            connected: ws && ws.readyState === WebSocket.OPEN,
            readyState: ws ? ws.readyState : -1
        }),
        getPrices,
        // 手动控制
        start: () => ws && ws.send(JSON.stringify({ type: 'START', sessionId: SESSION_ID })),
        stop: () => ws && ws.send(JSON.stringify({ type: 'STOP', sessionId: SESSION_ID })),
        // 调试功能
        ping: () => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'CLIENT_STATUS', source: 'var', sessionId: SESSION_ID, status: 'ping' }));
            }
        }
    };
    
    init();
})();
