// ========== GRVT 标签页客户端脚本 (多会话版) ==========
// 在GRVT交易页面的控制台运行此脚本
// 需要指定 SESSION_ID 与对应的VAR客户端配对

(function() {
    'use strict';
    
    // ===== 配置区域 - 修改这里的SESSION_ID =====
    const SESSION_ID = 'session1';  // 🔴 修改此ID与VAR客户端匹配
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
        console.log(`[GRVT][${SESSION_ID}][${time}] ${prefix} ${msg}`);
    }
    
    function connect() {
        if (ws && ws.readyState === WebSocket.OPEN) return;
        
        log(`连接套利服务器 ${WS_URL}...`, 'ws');
        
        ws = new WebSocket(WS_URL);
        
        ws.onopen = () => {
            log('已连接到套利服务器', 'success');
            ws.send(JSON.stringify({ 
                type: 'REGISTER', 
                client: 'grvt',
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
            log('WebSocket错误，请确保服务器已启动', 'error');
        };
    }
    
    // 当前订单状态
    let currentOrder = {
        orderId: null,
        orderType: null,  // 'open' or 'close'
        side: null,
        price: 0,
        quantity: 0,
        status: 'none'
    };
    
    function handleMessage(msg) {
        // 只处理自己会话的消息
        if (msg.sessionId && msg.sessionId !== SESSION_ID) return;
        
        switch (msg.type) {
            case 'REGISTERED':
                log(`已注册到会话: ${SESSION_ID}`, 'success');
                break;
            
            case 'PAIR_READY':
                log('🎉 VAR客户端已连接，可以开始套利!', 'success');
                break;
            
            case 'OPEN_POSITION':
                log(`收到开仓指令: ${msg.side}, 数量: ${msg.quantity}`, 'trade');
                currentOrder = {
                    orderId: msg.orderId,
                    orderType: 'open',
                    side: msg.side,
                    price: 0, // 由点击订单簿获取
                    quantity: msg.quantity,
                    status: 'pending'
                };
                openPosition(msg.side, msg.quantity);
                break;

            case 'CLOSE_POSITION':
                log(`收到平仓指令: ${msg.side}, 数量: ${msg.quantity}`, 'trade');
                currentOrder = {
                    orderId: msg.orderId,
                    orderType: 'close',
                    side: msg.side,
                    price: 0, // 由点击订单簿获取
                    quantity: msg.quantity,
                    status: 'pending'
                };
                closePosition(msg.side, msg.quantity);
                break;

            case 'PLACE_LIMIT_ORDER':
                log(`收到限价单指令: ${msg.side} @ ${msg.price}, 数量: ${msg.quantity}`, 'trade');
                currentOrder = {
                    orderId: msg.orderId,
                    orderType: msg.orderId?.startsWith('open') ? 'open' : 'close',
                    side: msg.side,
                    price: msg.price,
                    quantity: msg.quantity,
                    status: 'pending'
                };
                placeLimitOrder(msg.side, msg.price, msg.quantity, currentOrder.orderType);
                break;
            
            case 'PLACE_MARKET_ORDER':
                log(`收到市价单指令: ${msg.side}, 数量: ${msg.quantity}`, 'trade');
                placeMarketOrder(msg.side, msg.quantity);
                break;
            
            case 'CANCEL_ORDER':
                log(`收到取消订单指令: ${msg.orderType}`, 'warning');
                cancelCurrentOrder(msg.orderType);
                break;
            
            case 'STATUS':
                log(`策略状态: ${msg.running ? '运行中' : '已停止'}`, 'info');
                break;
            
            case 'SESSION_DELETED':
                log('会话已被删除', 'warning');
                if (ws) ws.close();
                break;
            
            case 'CLIENT_DISCONNECTED':
                if (msg.clientType === 'var') {
                    log('⚠️ VAR客户端断开连接', 'warning');
                }
                break;

            case 'REPORT_STATUS':
                // 报告当前GRVT状态 - 使用更稳定的检测逻辑
                const positionCount = getPositionCount();
                const pendingCount = getPendingOrderCount();
                let positionInfo = null;
                let hasPosition = false;

                // 多次尝试获取仓位信息，确保准确性
                for (let attempt = 0; attempt < 3; attempt++) {
                    positionInfo = getCurrentPosition();
                    if (positionInfo && positionInfo.size > 0 && positionInfo.entryPrice > 0) {
                        hasPosition = true;
                        break;
                    }
                    // 等待一小段时间再试
                    if (attempt < 2) {
                        sleep(200);
                    }
                }

                // 如果多次尝试仍未获取到有效仓位信息，回退到标签计数
                if (!hasPosition && positionCount > 0) {
                    log('仓位信息获取失败，使用标签计数作为备选', 'warning');
                    hasPosition = true;
                }

                const status = {
                    hasPosition: hasPosition,
                    hasPendingOrders: pendingCount > 0,
                    positionInfo: positionInfo
                };

                // 添加更详细的调试信息
                log(`状态检查详情: 仓位标签=${positionCount}, 订单标签=${pendingCount}, 解析大小=${positionInfo?.size || 0}, 解析价格=${positionInfo?.entryPrice || 0}`, 'info');

                ws.send(JSON.stringify({
                    type: 'GRVT_STATUS_REPORT',
                    sessionId: SESSION_ID,
                    source: 'grvt',
                    ...status
                }));
                log(`状态报告: 仓位=${status.hasPosition}, 未完成订单=${status.hasPendingOrders}`, 'info');
                break;
        }
    }
    
    // 取消当前订单
    async function cancelCurrentOrder(orderType) {
        log(`取消${orderType === 'open' ? '开仓' : '平仓'}订单...`, 'warning');
        
        try {
            // 点击未成交tab
            const uncompletedTab = document.querySelector('[data-text*="未成交订单"]');
            if (uncompletedTab) {
                uncompletedTab.click();
                await sleep(100);
            }
            
            // 点击取消所有订单按钮
            const cancelAllBtn = Array.from(document.querySelectorAll('button')).find(btn => btn.textContent.trim() === '取消所有订单');
            if (cancelAllBtn) {
                cancelAllBtn.click();
                await sleep(100);
            }
            currentOrder.status = 'cancelled';
            log('订单取消成功', 'success');
        } catch (e) {
            log(`取消订单失败: ${e.message}`, 'error');
        }
    }
    
    // 解析带逗号分隔符的数字，如 "89,356.12" -> 89356.12
    function parseNumber(str) {
        if (!str) return NaN;
        // 移除所有逗号后解析
        return parseFloat(str.replace(/,/g, ''));
    }
    
    function getBestPrices() {
        let bestBid = null;
        let bestAsk = null;
        
        // 方法1: 从订单簿行获取 (BackgroundProgress 组件)
        const orderBookRows = document.querySelectorAll('[data-sentry-component="BackgroundProgress"]');
        orderBookRows.forEach(row => {
            // 价格在第一个 fx-1 元素中
            const priceEl = row.querySelector('.fx-1.py-1');
            if (priceEl) {
                const price = parseNumber(priceEl.textContent);
                if (!isNaN(price)) {
                    // 判断是买单(绿色)还是卖单(红色)
                    if (priceEl.classList.contains('txt-feature-green')) {
                        // 买单 - 取最高价
                        if (!bestBid || price > bestBid) bestBid = price;
                    } else if (priceEl.classList.contains('txt-feature-red')) {
                        // 卖单 - 取最低价
                        if (!bestAsk || price < bestAsk) bestAsk = price;
                    }
                }
            }
        });
        
        // 方法2: 从中间价格区域获取 (heading-16 的绿色大数字)
        if (!bestBid || !bestAsk) {
            const midPriceEl = document.querySelector('.txt-feature-green .heading-16, .heading-16.txt-feature-green, .fx.txt-feature-green .heading-16');
            if (midPriceEl) {
                const midPrice = parseNumber(midPriceEl.textContent);
                if (!isNaN(midPrice)) {
                    // 用中间价估算 bid/ask
                    const tickSize = 0.0001;
                    if (!bestBid) bestBid = midPrice;
                    if (!bestAsk) bestAsk = midPrice + tickSize;
                }
            }
        }
        
        // 方法3: 从标题获取
        if (!bestBid || !bestAsk) {
            const titleMatch = document.title.match(/([\d.,]+)\s*\|/);
            if (titleMatch) {
                const midPrice = parseNumber(titleMatch[1]);
                const tickSize = 0.0001;
                bestBid = bestBid || midPrice - tickSize;
                bestAsk = bestAsk || midPrice + tickSize;
            }
        }
        
        // 调试日志 (首次获取时输出)
        if (bestBid && bestAsk && !window._grvtPriceLogged) {
            log(`价格获取成功: Bid=${bestBid}, Ask=${bestAsk}`, 'success');
            window._grvtPriceLogged = true;
        }
        
        return { bid: bestBid, ask: bestAsk };
    }
    
    let priceUpdateInterval = null;
    function startPriceUpdates() {
        if (priceUpdateInterval) clearInterval(priceUpdateInterval);
        
        priceUpdateInterval = setInterval(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                const prices = getBestPrices();
                if (prices.bid && prices.ask) {
                    ws.send(JSON.stringify({
                        type: 'PRICE_UPDATE',
                        source: 'grvt',
                        sessionId: SESSION_ID,
                        bid: prices.bid,
                        ask: prices.ask
                    }));
                }
            }
        }, 200);
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

    // 更稳定的"只做maker"复选框查找
    function findMakerOnlyCheckbox() {
        // 方法1: 通过文本内容查找包含"只做maker"的元素
        const makerLabels = Array.from(document.querySelectorAll('div')).filter(el =>
            el.textContent && el.textContent.includes('只做maker')
        );

        for (const label of makerLabels) {
            // 查找最近的checkbox input
            let parent = label;
            while (parent && parent !== document.body) {
                const checkbox = parent.querySelector('input[type="checkbox"]');
                if (checkbox) {
                    return { checkbox, container: parent };
                }
                parent = parent.parentElement;
            }
        }

        // 方法2: 通过data-sentry-component查找所有checkbox，然后检查文本
        const allCheckboxes = document.querySelectorAll('input[type="checkbox"]');
        for (const checkbox of allCheckboxes) {
            let parent = checkbox.parentElement;
            while (parent && parent !== document.body) {
                if (parent.textContent && parent.textContent.includes('只做maker')) {
                    return { checkbox, container: parent };
                }
                parent = parent.parentElement;
            }
        }

        return null;
    }

    function findReduceOnlyCheckbox() {
        // 方法1: 通过文本内容查找包含"只减仓"的元素
        const reduceLabels = Array.from(document.querySelectorAll('div')).filter(el =>
            el.textContent && el.textContent.includes('只减仓')
        );

        for (const label of reduceLabels) {
            // 查找最近的checkbox input
            let parent = label;
            while (parent && parent !== document.body) {
                const checkbox = parent.querySelector('input[type="checkbox"]');
                if (checkbox) {
                    return { checkbox, container: parent };
                }
                parent = parent.parentElement;
            }
        }

        // 方法2: 通过data-sentry-component查找所有checkbox，然后检查文本
        const allCheckboxes = document.querySelectorAll('input[type="checkbox"]');
        for (const checkbox of allCheckboxes) {
            let parent = checkbox.parentElement;
            while (parent && parent !== document.body) {
                if (parent.textContent && parent.textContent.includes('只减仓')) {
                    return { checkbox, container: parent };
                }
                parent = parent.parentElement;
            }
        }

        return null;
    }

    // 从订单簿获取最佳价格并点击对应行
    // Maker单逻辑：挂在自己方的最优价，等待对方来吃
    function clickOrderBookPrice(side) {
        const orderBookRows = document.querySelectorAll('[data-sentry-component="BackgroundProgress"]');
        let targetRow = null;
        let targetPrice = null;
        
        if (side === 'buy') {
            // 买入Maker：点击买单(绿色)的最高价，挂单等待卖方来成交
            let highestBid = 0;
            orderBookRows.forEach(row => {
                const priceEl = row.querySelector('.fx-1.py-1.txt-feature-green');
                if (priceEl) {
                    const price = parseNumber(priceEl.textContent);
                    if (!isNaN(price) && price > highestBid) {
                        highestBid = price;
                        targetRow = row;
                        targetPrice = price;
                    }
                }
            });
        } else {
            // 卖出Maker：点击卖单(红色)的最低价，挂单等待买方来成交
            let lowestAsk = Infinity;
            orderBookRows.forEach(row => {
                const priceEl = row.querySelector('.fx-1.py-1.txt-feature-red');
                if (priceEl) {
                    const price = parseNumber(priceEl.textContent);
                    if (!isNaN(price) && price < lowestAsk) {
                        lowestAsk = price;
                        targetRow = row;
                        targetPrice = price;
                    }
                }
            });
        }
        
        if (targetRow && targetPrice) {
            targetRow.click();
            log(`点击订单簿价格: ${targetPrice} (${side === 'buy' ? '买单最高价' : '卖单最低价'})`, 'info');
            return targetPrice;
        }
        
        return null;
    }
    
    // 开仓：点击订单簿限价单
    async function openPosition(side, quantity) {
        log(`执行开仓限价单: ${side}, 数量: ${quantity}`, 'trade');

        try {
            // 1. 切换到限价单模式
            const limitTab = document.querySelector('[data-text="限价"]');
            if (limitTab) {
                limitTab.click();
                await sleep(100);
            }

            // 2. 点击订单簿获取价格
            const actualPrice = clickOrderBookPrice(side);
            if (!actualPrice) {
                throw new Error('无法从订单簿获取价格');
            }

            // 3. 输入数量
            const qtyInput = document.querySelector('input[placeholder="数量"]');
            if (qtyInput) {
                qtyInput.focus();
                simulateInput(qtyInput, quantity.toString());
                await sleep(50);
            }

            // 4. 勾选只做maker
            const makerCheckbox = findMakerOnlyCheckbox();
            if (makerCheckbox && !makerCheckbox.checkbox.checked) {
                makerCheckbox.container.click();
                await sleep(30);
            }

            // 5. 点击下单按钮
            const buttons = document.querySelectorAll('[data-sentry-component="LoadingButton"]');
            let buttonClicked = false;
            for (const btn of buttons) {
                const text = btn.textContent.trim();
                if ((side === 'buy' && text.includes('买入')) ||
                    (side === 'sell' && text.includes('卖出'))) {
                    btn.click();
                    buttonClicked = true;
                    log(`开仓限价单已提交 @ ${actualPrice}`, 'success');

                    // 通知服务器订单已挂出
                    ws.send(JSON.stringify({
                        type: 'ORDER_PLACED',
                        sessionId: SESSION_ID,
                        source: 'grvt',
                        orderType: 'open',
                        side: side,
                        price: actualPrice,
                        quantity: quantity
                    }));

                    currentOrder.status = 'active';
                    currentOrder.price = actualPrice;

                    // 开始监控订单成交
                    monitorOrderFill(actualPrice, quantity, 'open');
                    return true;
                }
            }

            if (!buttonClicked) {
                throw new Error('未找到下单按钮');
            }
        } catch (e) {
            log(`开仓失败: ${e.message}`, 'error');
            ws.send(JSON.stringify({
                type: 'ORDER_FAILED',
                sessionId: SESSION_ID,
                source: 'grvt',
                orderType: 'open',
                reason: e.message
            }));
            return false;
        }
    }

    // 平仓：点击订单簿限价单
    async function closePosition(side, quantity) {
        log(`执行平仓限价单: ${side}, 数量: ${quantity}`, 'trade');

        try {
            // 1. 切换到限价单模式
            const limitTab = document.querySelector('[data-text="限价"]');
            if (limitTab) {
                limitTab.click();
                await sleep(100);
            }

            // 2. 点击订单簿获取价格
            const actualPrice = clickOrderBookPrice(side);
            if (!actualPrice) {
                throw new Error('无法从订单簿获取价格');
            }

            // 3. 输入数量
            const qtyInput = document.querySelector('input[placeholder="数量"]');
            if (qtyInput) {
                qtyInput.focus();
                simulateInput(qtyInput, quantity.toString());
                await sleep(50);
            }

            // 4. 勾选只做maker
            const makerCheckbox = findMakerOnlyCheckbox();
            if (makerCheckbox && !makerCheckbox.checkbox.checked) {
                makerCheckbox.container.click();
                await sleep(30);
            }

            // 4.1. 勾选只减仓
            const reduceOnlyCheckbox = findReduceOnlyCheckbox();
            if (reduceOnlyCheckbox && !reduceOnlyCheckbox.checkbox.checked) {
                reduceOnlyCheckbox.container.click();
                await sleep(30);
            }

            // 5. 点击下单按钮
            const buttons = document.querySelectorAll('[data-sentry-component="LoadingButton"]');
            let buttonClicked = false;
            for (const btn of buttons) {
                const text = btn.textContent.trim();
                if ((side === 'buy' && text.includes('买入')) ||
                    (side === 'sell' && text.includes('卖出'))) {
                    btn.click();
                    buttonClicked = true;
                    log(`平仓限价单已提交 @ ${actualPrice}`, 'success');

                    // 通知服务器订单已挂出
                    ws.send(JSON.stringify({
                        type: 'ORDER_PLACED',
                        sessionId: SESSION_ID,
                        source: 'grvt',
                        orderType: 'close',
                        side: side,
                        price: actualPrice,
                        quantity: quantity
                    }));

                    currentOrder.status = 'active';
                    currentOrder.price = actualPrice;

                    // 开始监控订单成交
                    monitorOrderFill(actualPrice, quantity, 'close');
                    return true;
                }
            }

            if (!buttonClicked) {
                throw new Error('未找到下单按钮');
            }
        } catch (e) {
            log(`平仓失败: ${e.message}`, 'error');
            ws.send(JSON.stringify({
                type: 'ORDER_FAILED',
                sessionId: SESSION_ID,
                source: 'grvt',
                orderType: 'close',
                reason: e.message
            }));
            return false;
        }
    }

    async function placeLimitOrder(side, price, quantity, orderType = 'open') {
        log(`执行限价单: ${side}, 数量: ${quantity}`, 'trade');

        try {
            // 1. 切换到限价单模式
            const limitTab = document.querySelector('[data-text="限价"]');
            if (limitTab) {
                limitTab.click();
                await sleep(100);
            }

            // 2. 点击订单簿获取价格（比输入更快更准确）
            const actualPrice = clickOrderBookPrice(side);
            if (!actualPrice) {
                log('无法从订单簿获取价格，使用传入价格', 'warning');
                // 降级：使用输入框输入价格
                const priceInput = document.querySelector('input[placeholder="价格"]');
                if (priceInput) {
                    priceInput.focus();
                    simulateInput(priceInput, price.toString());
                    await sleep(50);
                }
            }
            await sleep(50);

            // 使用实际价格（点击获取的或传入的）
            const finalPrice = actualPrice || price;

            // 3. 输入数量
            const qtyInput = document.querySelector('input[placeholder="数量"]');
            if (qtyInput) {
                qtyInput.focus();
                simulateInput(qtyInput, quantity.toString());
                await sleep(50);
            }

            // 4. 勾选只做maker
            const makerCheckbox = findMakerOnlyCheckbox();
            if (makerCheckbox && !makerCheckbox.checkbox.checked) {
                makerCheckbox.container.click();
                await sleep(30);
            }

            // 5. 点击下单按钮
            const buttons = document.querySelectorAll('[data-sentry-component="LoadingButton"]');
            let buttonClicked = false;
            for (const btn of buttons) {
                const text = btn.textContent.trim();
                if ((side === 'buy' && text.includes('买入')) ||
                    (side === 'sell' && text.includes('卖出'))) {
                    btn.click();
                    buttonClicked = true;
                    log(`${side === 'buy' ? '买入' : '卖出'}限价单已提交 @ ${finalPrice}`, 'success');

                    // 通知服务器订单已挂出（使用实际价格）
                    ws.send(JSON.stringify({
                        type: 'ORDER_PLACED',
                        sessionId: SESSION_ID,
                        source: 'grvt',
                        orderType: orderType,
                        side: side,
                        price: finalPrice,  // 使用实际点击的价格
                        quantity: quantity
                    }));

                    currentOrder.status = 'active';
                    currentOrder.price = finalPrice;

                    // 开始监控订单成交
                    monitorOrderFill(finalPrice, quantity, orderType);
                    return true;
                }
            }

            if (!buttonClicked) {
                log('未找到下单按钮', 'error');
                ws.send(JSON.stringify({
                    type: 'ORDER_FAILED',
                    sessionId: SESSION_ID,
                    source: 'grvt',
                    orderType: orderType,
                    reason: '未找到下单按钮'
                }));
                return false;
            }
        } catch (e) {
            log(`下单失败: ${e.message}`, 'error');
            ws.send(JSON.stringify({
                type: 'ORDER_FAILED',
                sessionId: SESSION_ID,
                source: 'grvt',
                orderType: orderType,
                reason: e.message
            }));
            return false;
        }
    }
    
    async function placeMarketOrder(side, quantity) {
        log(`执行市价单: ${side}, 数量: ${quantity}`, 'trade');
        
        try {
            const marketTab = document.querySelector('[data-text="市价"]');
            if (marketTab) {
                marketTab.click();
                await sleep(150);
            }
            
            const qtyInput = document.querySelector('input[placeholder="数量"]');
            if (qtyInput) {
                qtyInput.focus();
                simulateInput(qtyInput, quantity.toString());
                await sleep(100);
            }
            
            const buttons = document.querySelectorAll('[data-sentry-component="LoadingButton"]');
            for (const btn of buttons) {
                const text = btn.textContent.trim();
                if ((side === 'buy' && text.includes('买入')) ||
                    (side === 'sell' && text.includes('卖出'))) {
                    btn.click();
                    log(`${side === 'buy' ? '买入' : '卖出'}市价单已提交`, 'success');
                    return true;
                }
            }
            
            return false;
        } catch (e) {
            log(`市价单失败: ${e.message}`, 'error');
            return false;
        }
    }
    
    // ============================================
    // 订单和仓位检测
    // ============================================

    let orderMonitorInterval = null;
    function monitorOrderFill(targetPrice, quantity, orderType = 'open') {
        if (orderMonitorInterval) clearInterval(orderMonitorInterval);

        let lastPendingCount = getPendingOrderCount();
        let checkCount = 0;
        const startTime = Date.now();

        log(`开始监控${orderType === 'open' ? '开仓' : '平仓'}订单成交...`, 'info');

        orderMonitorInterval = setInterval(() => {
            checkCount++;

            // 如果订单已被取消，停止监控
            if (currentOrder.status === 'cancelled') {
                log('订单已取消，停止监控', 'warning');
                clearInterval(orderMonitorInterval);
                return;
            }

            const currentCount = getPendingOrderCount();

            // 检测订单成交 (待成交数量减少)
            if (currentCount < lastPendingCount) {
                log(`🎉 ${orderType === 'open' ? '开仓' : '平仓'}限价单已成交!`, 'success');
                clearInterval(orderMonitorInterval);

                currentOrder.status = 'filled';

                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        type: 'ORDER_FILLED',
                        source: 'grvt',
                        sessionId: SESSION_ID,
                        orderType: orderType,
                        price: targetPrice,
                        quantity: quantity
                    }));
                }
                return;
            }

            lastPendingCount = currentCount;

            // 每秒输出一次状态
            if (checkCount % 10 === 0) {
                const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
                log(`等待成交中... ${elapsed}s (待成交订单数: ${currentCount})`, 'info');
            }

            // 60秒超时(由服务器控制实际超时，这里只是监控上限)
            if (checkCount > 600) {
                log('订单监控超时(60s)', 'warning');
                clearInterval(orderMonitorInterval);
            }
        }, 100);
    }

    // 获取未成交订单数量
    function getPendingOrderCount() {
        // 从标签文本提取数字: "未成交订单（ 1 ）"
        const tab = document.querySelector('[data-text*="未成交订单"]');
        if (tab) {
            const match = tab.textContent.match(/（ (\d+) ）/);
            return match ? parseInt(match[1]) : 0;
        }
        return 0;
    }

    // 获取当前仓位数量
    function getPositionCount() {
        // 从标签文本提取数字: "仓位（ 1 ）"
        const tab = document.querySelector('[data-text*="仓位"]');
        if (tab) {
            const match = tab.textContent.match(/（ (\d+) ）/);
            return match ? parseInt(match[1]) : 0;
        }
        return 0;
    }

    // 获取当前仓位信息
    function getCurrentPosition() {
        const positionCount = getPositionCount();
        if (positionCount === 0) return null;

        try {
            // 点击仓位标签页切换到仓位视图
            const positionTab = document.querySelector('[data-text*="仓位"]');
            if (positionTab && !positionTab.classList.contains('style_active__ex4rC')) {
                positionTab.click();
                // 等待切换完成
                sleep(300);
            }

            // 使用正确的选择器查找仓位单元格
            const cells = document.querySelectorAll('[data-sentry-element="CellWrapper"]');
            if (cells.length === 0) {
                log('未找到仓位单元格', 'warning');
                return null;
            }

            log(`找到 ${cells.length} 个仓位单元格`, 'info');

            // 提取单元格文本
            const cellTexts = [];
            cells.forEach((cell, index) => {
                const text = cell.textContent.trim();
                cellTexts.push(text);
                log(`仓位单元格 ${index}: "${text}"`, 'info');
            });

            // GRVT仓位表格结构（基于实际DOM分析）:
            // 0: 交易对 (BTC)
            // 1: 杠杆信息 (Cross 50x) - 包含方向信息
            // 2: 仓位大小 (-0.003 BTC) 或 仓位价值 (263.99 USDT)
            // 3: 盈亏 (-268.57)
            // 4: 标记价 (89,571.1)
            // 5: 开仓价 (89,509.2)
            // 6: 强平价 (91,587.2)
            // 7+: 其他字段

            // 注意：第2列可能是"仓位大小"（合约数量）或"仓位价值"（美元价值）
            // 我们需要的是合约数量，不是美元价值

            let symbol = '';
            let side = 'unknown';
            let size = 0;
            let entryPrice = 0;
            let pnl = 0;

            // 解析交易对
            if (cellTexts[0]) {
                symbol = cellTexts[0];
            }

            // 解析方向 - 从杠杆信息或仓位大小判断
            if (cellTexts[1] && cellTexts[1].includes('Cross')) {
                // 杠杆信息通常不包含方向，需要从仓位大小判断
            }

            // 解析仓位大小 (格式如: -0.003 BTC 或 0.003 BTC)
            if (cellTexts[2]) {
                log(`解析仓位大小文本: "${cellTexts[2]}"`, 'info');
                // 匹配数字部分，忽略货币单位
                const sizeMatch = cellTexts[2].match(/([+-]?\d+\.?\d*)\s*[A-Z]*/);
                if (sizeMatch) {
                    size = parseFloat(sizeMatch[1]);
                    log(`解析得到仓位大小: ${size}`, 'info');

                    // 验证大小是否合理（合约数量不应该超过1）
                    if (size > 1) {
                        log(`仓位大小异常(${size})，可能解析到的是美元价值而不是合约数量`, 'warning');
                        // 如果大小太大，尝试查找真正的合约数量列
                        for (let i = 0; i < cellTexts.length; i++) {
                            const text = cellTexts[i];
                            const contractMatch = text.match(/([+-]?\d+\.?\d*)\s*BTC/);
                            if (contractMatch && Math.abs(parseFloat(contractMatch[1])) <= 1) {
                                size = Math.abs(parseFloat(contractMatch[1]));
                                log(`找到正确的合约数量: ${size}`, 'info');
                                break;
                            }
                        }
                    }

                    // 根据正负号判断多空方向
                    if (size > 0) {
                        side = 'long';
                    } else if (size < 0) {
                        side = 'short';
                        size = Math.abs(size); // 转为正数
                    }
                } else {
                    log(`无法解析仓位大小: ${cellTexts[2]}`, 'warning');
                }
            }

            // 解析盈亏
            if (cellTexts[3]) {
                const pnlMatch = cellTexts[3].match(/([+-]?\d+\.?\d*)/);
                if (pnlMatch) {
                    pnl = parseFloat(pnlMatch[1]);
                }
            }

            // 解析标记价
            if (cellTexts[4]) {
                const markMatch = cellTexts[4].match(/([\d,]+\.?\d*)/);
                if (markMatch) {
                    // 标记价暂时不需要，但可以用于验证
                }
            }

            // 解析开仓价
            if (cellTexts[5]) {
                const entryMatch = cellTexts[5].match(/([\d,]+\.?\d*)/);
                if (entryMatch) {
                    entryPrice = parseNumber(entryMatch[0]);
                }
            }

            // 如果还没确定方向，从其他字段判断
            if (side === 'unknown') {
                cells.forEach(cell => {
                    const text = cell.textContent.trim();
                    if (text.includes('做多') || text.includes('Long') || text.includes('Buy') || text.includes('买入')) {
                        side = 'long';
                    } else if (text.includes('做空') || text.includes('Short') || text.includes('Sell') || text.includes('卖出')) {
                        side = 'short';
                    }
                });
            }

            const result = {
                side,
                size,
                entryPrice,
                pnl,
                symbol
            };

            log(`解析仓位信息: 方向=${side}, 大小=${size}, 开仓价=${entryPrice}, 盈亏=${pnl}`, 'info');
            return result;

        } catch (e) {
            log(`获取仓位信息失败: ${e.message}`, 'warning');
        }

        // 如果无法获取详细信息，返回基本信息
        return {
            side: 'unknown',
            size: 0,
            entryPrice: 0,
            pnl: 0,
            symbol: ''
        };
    }

    // 检查是否有未完成的订单
    function hasPendingOrders() {
        return getPendingOrderCount() > 0;
    }

    // 检查是否有仓位
    function hasPosition() {
        return getPositionCount() > 0;
    }
    
    function init() {
        log(`GRVT套利客户端已加载 (会话: ${SESSION_ID})`, 'success');
        log('正在连接套利服务器...', 'info');
        connect();
    }
    
    window.GRVT_ARB = {
        SESSION_ID,
        connect,
        disconnect: () => { if (ws) ws.close(); },
        getStatus: () => ({ 
            sessionId: SESSION_ID,
            connected: ws && ws.readyState === WebSocket.OPEN 
        }),
        getPrices: getBestPrices,
        // 手动控制
        start: () => ws && ws.send(JSON.stringify({ type: 'START', sessionId: SESSION_ID })),
        stop: () => ws && ws.send(JSON.stringify({ type: 'STOP', sessionId: SESSION_ID }))
    };
    
    init();
})();
