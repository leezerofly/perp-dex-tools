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
        
        log(`连接套利服务器 ${WS_URL}...`, 'ws');
        
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
            log('WebSocket错误，请确保服务器已启动', 'error');
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
            
            case 'PAIR_READY':
                log('🎉 GRVT客户端已连接，可以开始套利!', 'success');
                break;
            
            case 'PLACE_MARKET_ORDER':
                // 根据上下文判断订单类型
                currentOrderType = msg.orderType || (msg.urgent ? 'open' : 'close');
                log(`收到${currentOrderType === 'open' ? '开仓' : '平仓'}市价单指令: ${msg.side}, 数量: ${msg.quantity}${msg.urgent ? ' [紧急]' : ''}`, 'trade');
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
    function startPriceUpdates() {
        if (priceUpdateInterval) clearInterval(priceUpdateInterval);
        
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
    
    async function placeMarketOrder(side, quantity, orderType = 'open') {
        log(`执行${orderType === 'open' ? '开仓' : '平仓'}市价单: ${side}, 数量: ${quantity}`, 'trade');
        
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
            for (const btn of sideButtons) {
                const text = btn.textContent.trim();
                if ((side === 'buy' && text.startsWith('买')) ||
                    (side === 'sell' && text.startsWith('卖'))) {
                    if (!btn.disabled && !btn.classList.contains('pointer-events-none')) {
                        btn.click();
                        await sleep(100);
                    }
                    break;
                }
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
                log(`🎉 ${orderType === 'open' ? '开仓' : '平仓'}${side === 'buy' ? '买入' : '卖出'}市价单已提交`, 'success');
                
                // 通知服务器成交
                await sleep(500);
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({
                        type: 'VAR_ORDER_FILLED',
                        source: 'var',
                        sessionId: SESSION_ID,
                        orderType: orderType,
                        side: side,
                        price: fillPrice,
                        quantity: quantity
                    }));
                }
                
                return true;
            } else {
                log('提交按钮不可用，等待重试...', 'warning');
                await sleep(300);
                return await placeMarketOrder(side, quantity, orderType);
            }
        } catch (e) {
            log(`下单失败: ${e.message}`, 'error');
            return false;
        }
    }
    
    function createStatusPanel() {
        const existing = document.getElementById('var-arb-panel');
        if (existing) existing.remove();
        
        const panel = document.createElement('div');
        panel.id = 'var-arb-panel';
        panel.innerHTML = `
            <style>
                #var-arb-panel {
                    position: fixed;
                    top: 60px;
                    left: 10px;
                    background: linear-gradient(135deg, #0d1421 0%, #182238 100%);
                    border: 2px solid #4C9AF8;
                    border-radius: 10px;
                    padding: 12px;
                    z-index: 10000;
                    font-family: monospace;
                    font-size: 11px;
                    color: #fff;
                    min-width: 200px;
                }
                #var-arb-panel h4 {
                    margin: 0 0 8px 0;
                    color: #4C9AF8;
                    font-size: 12px;
                }
                #var-arb-panel .session-id {
                    background: rgba(76, 154, 248, 0.2);
                    padding: 2px 6px;
                    border-radius: 4px;
                    font-size: 10px;
                    margin-left: 5px;
                }
                #var-arb-panel .status-line {
                    display: flex;
                    justify-content: space-between;
                    padding: 4px 0;
                    border-bottom: 1px solid rgba(255,255,255,0.1);
                }
                #var-arb-panel .connected { color: #00ff88; }
                #var-arb-panel .disconnected { color: #ff4466; }
            </style>
            <h4>🔵 VAR<span class="session-id">${SESSION_ID}</span></h4>
            <div class="status-line">
                <span>服务器</span>
                <span id="var-ws-status" class="disconnected">断开</span>
            </div>
            <div class="status-line">
                <span>Bid/Ask</span>
                <span id="var-prices">-</span>
            </div>
            <div class="status-line">
                <span>点差</span>
                <span id="var-spread">-</span>
            </div>
        `;
        document.body.appendChild(panel);
        
        setInterval(() => {
            const wsStatus = document.getElementById('var-ws-status');
            if (wsStatus) {
                if (ws && ws.readyState === WebSocket.OPEN) {
                    wsStatus.textContent = '已连接';
                    wsStatus.className = 'connected';
                } else {
                    wsStatus.textContent = '断开';
                    wsStatus.className = 'disconnected';
                }
            }
            
            const prices = getPrices();
            
            const pricesEl = document.getElementById('var-prices');
            if (pricesEl) {
                pricesEl.textContent = prices.bid && prices.ask ? 
                    `${prices.bid} / ${prices.ask}` : '-';
            }
            
            const spreadEl = document.getElementById('var-spread');
            if (spreadEl) {
                spreadEl.textContent = prices.spread ? 
                    `${(prices.spread * 100).toFixed(4)}%` : '-';
            }
        }, 500);
    }
    
    function init() {
        log(`VAR套利客户端已加载 (会话: ${SESSION_ID})`, 'success');
        log('正在连接套利服务器...', 'info');
        createStatusPanel();
        connect();
    }
    
    window.VAR_ARB = {
        SESSION_ID,
        connect,
        disconnect: () => { if (ws) ws.close(); },
        getStatus: () => ({ 
            sessionId: SESSION_ID,
            connected: ws && ws.readyState === WebSocket.OPEN 
        }),
        getPrices,
        // 手动控制
        start: () => ws && ws.send(JSON.stringify({ type: 'START', sessionId: SESSION_ID })),
        stop: () => ws && ws.send(JSON.stringify({ type: 'STOP', sessionId: SESSION_ID }))
    };
    
    init();
})();
