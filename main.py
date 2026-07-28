import asyncio
import json
from datetime import datetime
from typing import Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import os

app = FastAPI()

# 挂载静态文件目录（如果存在图标文件）
if os.path.exists("redfox.ico"):
	app.mount("/static", StaticFiles(directory="."), name="static")

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

@app.get("/")
async def get():
	"""提供Web界面"""
	html_content = """
	<!DOCTYPE html>
	<html>
	<head>
		<title>Palmmicro</title>
		<link rel="shortcut icon" href="/redfox.ico" type="image/x-icon">
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
							
							// 处理布尔值 - 显示带颜色的勾叉符号
							if (typeof value === 'boolean') {
								if (value === true) {
									displayValue = '<span class="bool-true">✓</span>';
								} else {
									displayValue = '<span class="bool-false">✗</span>';
								}
							} else if (value === null || value === undefined) {
								displayValue = '-';
							} else if (typeof value === 'number') {
								// 判断是否为整数
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