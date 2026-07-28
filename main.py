import asyncio
import json
from datetime import datetime, time
from typing import Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
import uvicorn
import os

app = FastAPI()

class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
    
    async def broadcast(self, message: str):
        if not self.active_connections:
            return
        
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except WebSocketDisconnect:
                disconnected.append(connection)
            except:
                disconnected.append(connection)
        
        for conn in disconnected:
            self.disconnect(conn)

manager = ConnectionManager()
latest_data = None

def is_a_stock_trading_time() -> bool:
    """
    判断当前是否在A股开盘时间
    周一到周五：9:15-11:30, 13:00-15:00
    """
    now = datetime.now()
    current_time = now.time()
    current_weekday = now.weekday()
    
    # 周一到周五 (0=Monday, 4=Friday)
    if current_weekday >= 5:
        return False
    
    morning_start = time(9, 15)
    morning_end = time(11, 30)
    afternoon_start = time(13, 0)
    afternoon_end = time(15, 0)
    
    if morning_start <= current_time <= morning_end:
        return True
    if afternoon_start <= current_time <= afternoon_end:
        return True
    
    return False

def check_bot_token(request: Request) -> bool:
    """
    检查请求中是否包含正确的BOT_TOKEN
    """
    bot_token = os.environ.get("BOT_TOKEN", "")
    if not bot_token:
        return False
    
    # 从查询参数获取key
    key = request.query_params.get("key", "")
    return key == bot_token

@app.get("/redfox.ico")
async def get_icon():
    """提供图标文件"""
    if os.path.exists("redfox.ico"):
        return FileResponse("redfox.ico", media_type="image/x-icon")
    else:
        from fastapi.responses import Response
        return Response(status_code=204)

@app.get("/")
async def get(request: Request):
    """
    提供Web界面
    在A股开盘时间需要验证BOT_TOKEN
    """
    if is_a_stock_trading_time():
        if not check_bot_token(request):
            return HTMLResponse("""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Palmmicro - 访问受限</title>
                    <link rel="shortcut icon" href="/redfox.ico" type="image/x-icon">
                    <link rel="icon" href="/redfox.ico" type="image/x-icon">
                    <style>
                        body {
                            font-family: Arial, sans-serif;
                            display: flex;
                            justify-content: center;
                            align-items: center;
                            height: 100vh;
                            margin: 0;
                            background: #f5f5f5;
                        }
                        .container {
                            background: white;
                            padding: 40px;
                            border-radius: 10px;
                            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                            text-align: center;
                            max-width: 400px;
                        }
                        h1 {
                            color: #dc3545;
                            font-size: 24px;
                        }
                        p {
                            color: #666;
                            line-height: 1.6;
                        }
                        .hint {
                            background: #f8f9fa;
                            padding: 10px;
                            border-radius: 5px;
                            margin-top: 20px;
                            font-size: 14px;
                            color: #888;
                        }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>🔒 访问受限</h1>
                        <p>当前处于A股开盘时间，访问需要提供验证密钥。</p>
                        <p style="font-size: 14px; color: #666;">
                            请使用以下格式访问：<br>
                            <code style="background: #f5f5f5; padding: 4px 8px; border-radius: 3px;">
                                https://palmmicro.onrender.com?key=你的密钥
                            </code>
                        </p>
                        <div class="hint">
                            💡 提示：非开盘时间可无需密钥直接访问
                        </div>
                    </div>
                </body>
                </html>
            """, status_code=403)
    
    # 非开盘时间或验证通过，正常返回页面
    return get_main_html()

def get_main_html() -> HTMLResponse:
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Palmmicro</title>
        <link rel="shortcut icon" href="/redfox.ico" type="image/x-icon">
        <link rel="icon" href="/redfox.ico" type="image/x-icon">
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 10px;
                background: #f5f5f5;
            }
            .container {
                max-width: 1400px;
                margin: 0 auto;
                background: white;
                padding: 15px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            h1 {
                color: #333;
                font-size: 20px;
                margin: 0 0 10px 0;
                padding-bottom: 8px;
                border-bottom: 2px solid #4CAF50;
            }
            .top-bar {
                display: flex;
                align-items: center;
                gap: 20px;
                margin-bottom: 10px;
                flex-wrap: wrap;
            }
            #status {
                padding: 6px 15px;
                border-radius: 5px;
                background: #e7f3fe;
                color: #2196F3;
                font-weight: bold;
                font-size: 14px;
                white-space: nowrap;
                flex-shrink: 0;
            }
            #status.connected {
                background: #d4edda;
                color: #155724;
            }
            #status.disconnected {
                background: #f8d7da;
                color: #721c24;
            }
            #stats {
                display: flex;
                gap: 15px;
                flex-wrap: wrap;
                flex: 1;
            }
            .stat-box {
                background: #f8f9fa;
                padding: 5px 15px;
                border-radius: 5px;
                border-left: 3px solid #4CAF50;
                display: flex;
                align-items: baseline;
                gap: 8px;
            }
            .stat-box label {
                font-weight: bold;
                color: #666;
                font-size: 11px;
                text-transform: uppercase;
            }
            .stat-box .value {
                font-size: 16px;
                font-weight: bold;
                color: #333;
            }
            #data-container {
                margin-top: 5px;
                overflow-x: auto;
                max-height: calc(100vh - 200px);
                min-height: 400px;
                overflow-y: auto;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                font-size: 13px;
            }
            table th {
                background: #4CAF50;
                color: white;
                padding: 8px 10px;
                text-align: left;
                position: sticky;
                top: 0;
                z-index: 10;
                font-size: 13px;
            }
            table td {
                padding: 6px 10px;
                border-bottom: 1px solid #ddd;
                font-size: 13px;
            }
            table tr:hover {
                background: #f5f5f5;
            }
            .bool-true {
                color: #28a745;
                font-weight: bold;
                font-size: 16px;
            }
            .bool-false {
                color: #dc3545;
                font-weight: bold;
                font-size: 16px;
            }
            @media (max-width: 768px) {
                .top-bar {
                    flex-direction: column;
                    align-items: stretch;
                }
                #stats {
                    justify-content: space-between;
                }
                .stat-box {
                    flex: 1;
                    min-width: 100px;
                }
                #data-container {
                    max-height: calc(100vh - 300px);
                    min-height: 300px;
                }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>企业微信消息实时汇总</h1>
            
            <div class="top-bar">
                <div id="status">连接中...</div>
                
                <div id="stats">
                    <div class="stat-box">
                        <label>最后更新</label>
                        <div class="value" id="last-update">-</div>
                    </div>
                    <div class="stat-box">
                        <label>数据行数</label>
                        <div class="value" id="row-count">0</div>
                    </div>
                    <div class="stat-box">
                        <label>接收条数</label>
                        <div class="value" id="msg-count">0</div>
                    </div>
                </div>
            </div>
            
            <div id="data-container">
                <div style="text-align: center; padding: 40px; color: #999;">等待数据...</div>
            </div>
        </div>

        <script>
            let ws;
            let messageCount = 0;
            let reconnectAttempts = 0;
            
            function connect() {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const wsUrl = `${protocol}//${window.location.host}/ws`;
                
                ws = new WebSocket(wsUrl);
                
                ws.onopen = function() {
                    document.getElementById('status').textContent = '已连接';
                    document.getElementById('status').className = 'connected';
                    reconnectAttempts = 0;
                };
                
                ws.onmessage = function(event) {
                    try {
                        const data = JSON.parse(event.data);
                        messageCount++;
                        updateUI(data);
                    } catch (e) {
                        console.error('解析失败:', e);
                    }
                };
                
                ws.onclose = function() {
                    document.getElementById('status').textContent = '断开，重连中...';
                    document.getElementById('status').className = 'disconnected';
                    
                    if (reconnectAttempts < 5) {
                        reconnectAttempts++;
                        setTimeout(connect, 3000 * reconnectAttempts);
                    } else {
                        document.getElementById('status').textContent = '连接失败，请刷新';
                    }
                };
            }
            
            function updateUI(data) {
                document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
                document.getElementById('msg-count').textContent = messageCount;
                
                if (data.type === 'dataframe') {
                    const df = data.data;
                    
                    if (df.length > 0) {
                        document.getElementById('row-count').textContent = df.length;
                    }
                    
                    if (df.length === 0) {
                        document.getElementById('data-container').innerHTML = 
                            '<div style="text-align: center; padding: 40px; color: #999;">暂无数据</div>';
                        return;
                    }
                    
                    const columns = Object.keys(df[0]);
                    
                    let html = '<table><thead><tr>';
                    columns.forEach(col => {
                        html += `<th>${escapeHtml(col)}</th>`;
                    });
                    html += '</tr></thead><tbody>';
                    
                    df.forEach(row => {
                        html += '<tr>';
                        columns.forEach(col => {
                            let value = row[col];
                            let displayValue;
                            
                            if (typeof value === 'boolean') {
                                if (value === true) {
                                    displayValue = '<span class="bool-true">✓</span>';
                                } else {
                                    displayValue = '<span class="bool-false">✗</span>';
                                }
                            } else if (value === null || value === undefined) {
                                displayValue = '-';
                            } else if (typeof value === 'number') {
                                if (Number.isInteger(value)) {
                                    displayValue = value.toString();
                                } else {
                                    displayValue = value.toFixed(2);
                                }
                            } else if (typeof value === 'object') {
                                displayValue = JSON.stringify(value);
                            } else {
                                displayValue = escapeHtml(String(value));
                            }
                            
                            html += `<td>${displayValue}</td>`;
                        });
                        html += '</tr>';
                    });
                    html += '</tbody></table>';
                    
                    document.getElementById('data-container').innerHTML = html;
                }
            }
            
            function escapeHtml(text) {
                const div = document.createElement('div');
                div.textContent = text;
                return div.innerHTML;
            }
            
            connect();
        </script>
    </body>
    </html>
    """
    return HTMLResponse(html_content)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global latest_data
    
    await manager.connect(websocket)
    try:
        if latest_data is not None:
            await websocket.send_text(json.dumps({
                "type": "dataframe",
                "data": latest_data
            }))
        
        while True:
            data = await websocket.receive_text()
            try:
                parsed = json.loads(data)
                if parsed.get("type") == "dataframe":
                    latest_data = parsed.get("data", [])
                    await manager.broadcast(json.dumps({
                        "type": "dataframe",
                        "data": latest_data
                    }))
                else:
                    await websocket.send_text(f"Received: {data}")
            except:
                pass
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except:
        manager.disconnect(websocket)

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10000)