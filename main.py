import asyncio
import json
import logging
from datetime import datetime
from typing import Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# 存储所有活跃的WebSocket连接
class ConnectionManager:
	def __init__(self):
		self.active_connections: Set[WebSocket] = set()
	
	async def connect(self, websocket: WebSocket):
		await websocket.accept()
		self.active_connections.add(websocket)
		logger.info(f"New connection. Total: {len(self.active_connections)}")
	
	def disconnect(self, websocket: WebSocket):
		self.active_connections.discard(websocket)
		logger.info(f"Connection closed. Total: {len(self.active_connections)}")
	
	async def broadcast(self, message: str):
		"""广播消息给所有连接的客户端"""
		if not self.active_connections:
			return
		
		disconnected = []
		for connection in self.active_connections:
			try:
				await connection.send_text(message)
			except WebSocketDisconnect:
				disconnected.append(connection)
			except Exception as e:
				logger.error(f"Error sending message: {e}")
				disconnected.append(connection)
		
		# 清理断开的连接
		for conn in disconnected:
			self.disconnect(conn)

manager = ConnectionManager()

# 存储最新的数据（全局变量）
latest_data = None

@app.get("/")
async def get():
	"""提供Web界面"""
	html_content = """
	<!DOCTYPE html>
	<html>
	<head>
		<title>实时数据监控</title>
		<style>
			body {
				font-family: Arial, sans-serif;
				margin: 20px;
				background: #f5f5f5;
			}
			.container {
				max-width: 1200px;
				margin: 0 auto;
				background: white;
				padding: 20px;
				border-radius: 10px;
				box-shadow: 0 2px 10px rgba(0,0,0,0.1);
			}
			h1 {
				color: #333;
				border-bottom: 2px solid #4CAF50;
				padding-bottom: 10px;
			}
			#status {
				padding: 10px;
				margin: 10px 0;
				border-radius: 5px;
				background: #e7f3fe;
				color: #2196F3;
				font-weight: bold;
			}
			#status.connected {
				background: #d4edda;
				color: #155724;
			}
			#status.disconnected {
				background: #f8d7da;
				color: #721c24;
			}
			#data-container {
				margin-top: 20px;
				overflow-x: auto;
			}
			table {
				width: 100%;
				border-collapse: collapse;
				font-size: 14px;
			}
			table th {
				background: #4CAF50;
				color: white;
				padding: 12px;
				text-align: left;
				position: sticky;
				top: 0;
				z-index: 10;
			}
			table td {
				padding: 10px;
				border-bottom: 1px solid #ddd;
			}
			table tr:hover {
				background: #f5f5f5;
			}
			#stats {
				display: flex;
				gap: 20px;
				margin: 15px 0;
				flex-wrap: wrap;
			}
			.stat-box {
				background: #f8f9fa;
				padding: 10px 20px;
				border-radius: 5px;
				border-left: 4px solid #4CAF50;
			}
			.stat-box label {
				font-weight: bold;
				color: #666;
				font-size: 12px;
				text-transform: uppercase;
			}
			.stat-box .value {
				font-size: 20px;
				font-weight: bold;
				color: #333;
			}
			.error {
				color: #dc3545;
				padding: 10px;
				background: #f8d7da;
				border-radius: 5px;
				margin: 10px 0;
			}
		</style>
	</head>
	<body>
		<div class="container">
			<h1>📊 实时数据监控</h1>
			<div id="status">🔌 正在连接...</div>
			
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
					<label>数据列数</label>
					<div class="value" id="col-count">0</div>
				</div>
				<div class="stat-box">
					<label>接收条数</label>
					<div class="value" id="msg-count">0</div>
				</div>
			</div>
			
			<div id="data-container">
				<div style="text-align: center; padding: 40px; color: #999;">
					等待数据...
				</div>
			</div>
		</div>

		<script>
			let ws;
			let messageCount = 0;
			let reconnectAttempts = 0;
			const maxReconnectAttempts = 5;
			
			function connect() {
				const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
				const wsUrl = `${protocol}//${window.location.host}/ws`;
				
				ws = new WebSocket(wsUrl);
				
				ws.onopen = function() {
					console.log('WebSocket连接已建立');
					document.getElementById('status').textContent = '✅ 已连接';
					document.getElementById('status').className = 'connected';
					reconnectAttempts = 0;
				};
				
				ws.onmessage = function(event) {
					try {
						const data = JSON.parse(event.data);
						messageCount++;
						updateUI(data);
					} catch (e) {
						console.error('解析数据失败:', e);
					}
				};
				
				ws.onclose = function() {
					console.log('WebSocket连接已关闭');
					document.getElementById('status').textContent = '❌ 连接断开，尝试重连...';
					document.getElementById('status').className = 'disconnected';
					
					if (reconnectAttempts < maxReconnectAttempts) {
						reconnectAttempts++;
						setTimeout(connect, 3000 * reconnectAttempts);
					} else {
						document.getElementById('status').textContent = '❌ 连接失败，请刷新页面重试';
					}
				};
				
				ws.onerror = function(error) {
					console.error('WebSocket错误:', error);
				};
			}
			
			function updateUI(data) {
				document.getElementById('last-update').textContent = new Date().toLocaleTimeString();
				document.getElementById('msg-count').textContent = messageCount;
				
				if (data.type === 'dataframe') {
					const df = data.data;
					
					if (df.length > 0) {
						document.getElementById('row-count').textContent = df.length;
						document.getElementById('col-count').textContent = Object.keys(df[0]).length;
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
					
					const displayData = df.slice(-50);
					displayData.forEach(row => {
						html += '<tr>';
						columns.forEach(col => {
							let value = row[col];
							if (value === null || value === undefined) {
								value = '-';
							} else if (typeof value === 'number') {
								value = value.toFixed(2);
							} else if (typeof value === 'object') {
								value = JSON.stringify(value);
							}
							html += `<td>${escapeHtml(String(value))}</td>`;
						});
						html += '</tr>';
					});
					html += '</tbody></table>';
					
					if (df.length > 50) {
						html += `<div style="margin-top: 10px; color: #666; font-size: 12px;">
							显示最近50行（共${df.length}行）
						</div>`;
					}
					
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
	await manager.connect(websocket)
	try:
		# 如果有历史数据，立即发送给新连接的客户端
		global latest_data
		if latest_data is not None:
			await websocket.send_text(json.dumps({
				"type": "dataframe",
				"data": latest_data
			}))
		
		while True:
			# 接收来自客户端的消息
			data = await websocket.receive_text()
			try:
				parsed = json.loads(data)
				
				# 如果是数据帧更新
				if parsed.get("type") == "dataframe":
					# 更新全局变量
					global latest_data
					latest_data = parsed.get("data", [])
					# 广播给所有连接的浏览器客户端
					await manager.broadcast(json.dumps({
						"type": "dataframe",
						"data": latest_data
					}))
				else:
					# 其他类型的消息
					await websocket.send_text(f"Received: {data}")
					
			except json.JSONDecodeError:
				logger.error(f"Invalid JSON received: {data}")
				await websocket.send_text("Error: Invalid JSON format")
				
	except WebSocketDisconnect:
		manager.disconnect(websocket)
	except Exception as e:
		logger.error(f"WebSocket error: {e}")
		manager.disconnect(websocket)

# 健康检查端点
@app.get("/health")
async def health():
	return {"status": "healthy", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
	uvicorn.run(app, host="0.0.0.0", port=10000)