/*
╔══════════════════════════════════════════════════════════════════════════════╗
║                    GRVT-VAR 价差套利系统使用说明                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  系统架构:                                                                    ║
║  ┌─────────────┐      ┌─────────────────────┐      ┌─────────────┐          ║
║  │ GRVT 标签页 │◄────►│ WebSocket 中继服务器 │◄────►│ VAR 标签页  │          ║
║  │ (grvt.io)  │      │  (localhost:8765)   │      │(variational)│          ║
║  └─────────────┘      └─────────────────────┘      └─────────────┘          ║
║                                                                              ║
║  文件说明:                                                                    ║
║  • arbitrage_server.js - Node.js WebSocket服务器，协调两个交易所              ║
║  • grvt_client.js      - GRVT页面注入的客户端脚本                            ║
║  • var_client.js       - VAR页面注入的客户端脚本                             ║
║                                                                              ║
║  使用步骤:                                                                    ║
║  1. 安装依赖: npm install ws                                                  ║
║  2. 启动服务器: node arbitrage_server.js                                      ║
║  3. 打开GRVT交易页面，在控制台粘贴 grvt_client.js 内容                        ║
║  4. 打开VAR交易页面，在控制台粘贴 var_client.js 内容                          ║
║  5. 在任一客户端发送 START 命令启动策略                                       ║
║                                                                              ║
║  手续费设置:                                                                  ║
║  • GRVT maker: -0.001% (返佣)                                                 ║
║  • GRVT taker: 0.037%                                                         ║
║  • VAR: 0% (但有点差)                                                         ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

        // 测试GRVT状态检测功能
        function testGrvtStatus() {
            console.log('🧪 测试GRVT状态检测...');

            // 发送状态检查请求
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    type: 'CHECK_GRVT_STATUS',
                    sessionId: 'session1'
                }));
                console.log('✅ 已发送状态检查请求');
            } else {
                console.log('❌ WebSocket未连接');
            }
        }

        // 如果在本地HTML控制面板中运行，提供控制接口
        const ARBITRAGE_CONTROLLER = {
    ws: null,
    config: {
        wsUrl: 'ws://localhost:8765',
        symbol: 'XRP/USDT',
        orderSize: 100,
        minProfitToOpen: 0.0001,   // 开仓最小净收益率 (0.01%)
        minProfitToClose: 0.0002   // 平仓锁定利润阈值 (0.02%)
    },
    state: {
        connected: false,
        running: false,
        grvtConnected: false,
        varConnected: false,
        position: null,
        prices: {
            grvt: { bid: 0, ask: 0 },
            var: { bid: 0, ask: 0, spread: 0 }
        }
    },

    // 连接服务器
    connect() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            console.log('已连接');
            return;
        }

        console.log('连接套利服务器...');
        this.ws = new WebSocket(this.config.wsUrl);

        this.ws.onopen = () => {
            console.log('✅ 已连接到套利服务器');
            this.state.connected = true;
            this.ws.send(JSON.stringify({ type: 'REGISTER', client: 'controller' }));
            this.ws.send(JSON.stringify({ type: 'GET_STATUS' }));
        };

        this.ws.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            this.handleMessage(msg);
        };

        this.ws.onclose = () => {
            console.log('⚠️ 与服务器断开');
            this.state.connected = false;
        };
    },

    handleMessage(msg) {
        switch (msg.type) {
            case 'FULL_STATUS':
                this.state = { ...this.state, ...msg.state };
                this.state.grvtConnected = msg.state.clients?.grvt || false;
                this.state.varConnected = msg.state.clients?.var || false;
                console.log('状态更新:', this.state);
                break;
            case 'PRICES':
                this.state.prices = msg.prices;
                break;
            case 'POSITION_OPENED':
                console.log('💹 仓位已开:', msg.position);
                this.state.position = msg.position;
                break;
            case 'STATUS':
                this.state.running = msg.running;
                console.log(`策略: ${msg.running ? '运行中' : '已停止'}`);
                break;
            case 'HEARTBEAT':
                this.state.position = msg.position;
                this.state.running = msg.running;
                break;
        }
    },

    // 启动策略
    start() {
        if (!this.state.connected) {
            console.log('请先连接服务器');
            return;
        }
        if (!this.state.grvtConnected || !this.state.varConnected) {
            console.log('等待两个交易所客户端都连接...');
            console.log(`GRVT: ${this.state.grvtConnected ? '✅' : '❌'}, VAR: ${this.state.varConnected ? '✅' : '❌'}`);
            return;
        }
        this.ws.send(JSON.stringify({ type: 'START' }));
        console.log('🚀 套利策略已启动');
    },

    // 停止策略
    stop() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({ type: 'STOP' }));
            console.log('⏹ 套利策略已停止');
        }
    },

    // 紧急平仓
    emergencyClose() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({ type: 'EMERGENCY_CLOSE' }));
            console.log('🚨 紧急平仓指令已发送');
        }
    },

    // 更新配置
    updateConfig(newConfig) {
        Object.assign(this.config, newConfig);
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({ type: 'UPDATE_CONFIG', config: newConfig }));
            console.log('配置已更新:', newConfig);
        }
    },

    // 显示状态
    status() {
        console.log('\n========== 套利系统状态 ==========');
        console.log(`服务器连接: ${this.state.connected ? '✅' : '❌'}`);
        console.log(`GRVT客户端: ${this.state.grvtConnected ? '✅' : '❌'}`);
        console.log(`VAR客户端: ${this.state.varConnected ? '✅' : '❌'}`);
        console.log(`策略运行: ${this.state.running ? '🟢 运行中' : '🔴 已停止'}`);
        console.log('\n--- 当前价格 ---');
        console.log(`GRVT: ${this.state.prices.grvt.bid} / ${this.state.prices.grvt.ask}`);
        console.log(`VAR: ${this.state.prices.var.bid} / ${this.state.prices.var.ask}`);
        console.log(`VAR点差: ${(this.state.prices.var.spread * 100).toFixed(4)}%`);
        if (this.state.position?.isOpen) {
            console.log('\n--- 当前仓位 ---');
            console.log(`GRVT: ${this.state.position.grvtSide} @ ${this.state.position.grvtEntryPrice}`);
            console.log(`VAR: ${this.state.position.varSide} @ ${this.state.position.varEntryPrice}`);
        }
        console.log('==================================\n');
    },

    // 帮助
    help() {
        console.log(`
╔══════════════════════════════════════════════════════════════╗
║                    套利控制器命令                             ║
╠══════════════════════════════════════════════════════════════╣
║  ARBITRAGE_CONTROLLER.connect()     - 连接服务器              ║
║  ARBITRAGE_CONTROLLER.start()       - 启动策略                ║
║  ARBITRAGE_CONTROLLER.stop()        - 停止策略                ║
║  ARBITRAGE_CONTROLLER.emergencyClose() - 紧急平仓             ║
║  ARBITRAGE_CONTROLLER.status()      - 查看状态                ║
║  ARBITRAGE_CONTROLLER.updateConfig({orderSize: 200}) - 更新配置║
╚══════════════════════════════════════════════════════════════╝
        `);
    }
};

// 自动显示帮助
console.log('GRVT-VAR 套利控制器已加载，输入 ARBITRAGE_CONTROLLER.help() 查看命令');

// 暴露到全局
if (typeof window !== 'undefined') {
    window.ARBITRAGE_CONTROLLER = ARBITRAGE_CONTROLLER;
}

if (typeof module !== 'undefined') {
    module.exports = ARBITRAGE_CONTROLLER;
}
