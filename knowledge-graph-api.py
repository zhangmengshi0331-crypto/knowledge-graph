"""
产业链知识图谱 API 服务
支持 Agent 访问和 HTML 前端访问
"""

from flask import Flask, request, jsonify, g
from flask_cors import CORS
import sqlite3
import json
import os
from datetime import datetime

app = Flask(__name__)
CORS(app)  # 允许跨域请求
app.config['DATABASE'] = 'knowledge_graph.db'

# ============================================================
# 数据库初始化
# ============================================================

def get_db_path():
    """获取数据库文件路径"""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), app.config['DATABASE'])

def init_db():
    """初始化数据库（直接连接，不依赖请求上下文）"""
    db = sqlite3.connect(get_db_path())
    db.row_factory = sqlite3.Row

    # 创建节点表
    db.execute('''
        CREATE TABLE IF NOT EXISTS nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            category TEXT,
            node_type TEXT DEFAULT 'product',
            description TEXT,
            meta TEXT,
            upstream TEXT,
            downstream TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 创建字段配置表（支持动态字段）
    db.execute('''
        CREATE TABLE IF NOT EXISTS fields (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            label TEXT NOT NULL,
            field_type TEXT DEFAULT 'text',
            required INTEGER DEFAULT 0,
            visible INTEGER DEFAULT 1,
            editable INTEGER DEFAULT 1,
            options TEXT,
            sort_order INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 创建搜索日志表
    db.execute('''
        CREATE TABLE IF NOT EXISTS search_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL,
            result_count INTEGER DEFAULT 0,
            found INTEGER DEFAULT 0,
            searched_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 初始化默认字段
    default_fields = [
        ('category', '行业分类', 'text', 1, 1, 1, None, 1),
        ('name', '名称', 'text', 1, 1, 1, None, 2),
        ('node_type', '类型', 'select', 1, 1, 1, '["行业","产品","材料","事件"]', 3),
        ('description', '描述', 'textarea', 0, 1, 1, None, 4),
        ('meta', '关键信息', 'tags', 0, 1, 1, None, 5),
        ('upstream', '上游', 'tags', 0, 1, 1, None, 6),
        ('downstream', '下游', 'tags', 0, 1, 1, None, 7),
    ]

    cursor = db.execute("SELECT COUNT(*) FROM fields")
    if cursor.fetchone()[0] == 0:
        for field in default_fields:
            db.execute('''
                INSERT INTO fields (name, label, field_type, required, visible, editable, options, sort_order)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', field)

    db.commit()

    # 导入默认数据
    import_default_data(db)
    db.close()

def import_default_data(db):
    """导入默认产业链数据"""
    cursor = db.execute("SELECT COUNT(*) FROM nodes")
    if cursor.fetchone()[0] > 0:
        return

    json_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'industry-data.json')
    if not os.path.exists(json_file):
        print(f"[警告] 未找到 industry-data.json，跳过默认数据导入")
        return

    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    for name, node in data.items():
        db.execute('''
            INSERT OR IGNORE INTO nodes (name, category, node_type, description, meta, upstream, downstream)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            node.get('name', name),
            node.get('category', ''),
            node.get('type', 'product'),
            node.get('description', ''),
            json.dumps(node.get('meta', []), ensure_ascii=False),
            json.dumps(node.get('upstream', []), ensure_ascii=False),
            json.dumps(node.get('downstream', []), ensure_ascii=False),
        ))

    db.commit()
    print(f"[初始化] 已导入 {len(data)} 条默认数据")

def get_db():
    """获取请求级别的数据库连接（仅在请求上下文中使用）"""
    if 'db' not in g:
        g.db = sqlite3.connect(get_db_path())
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(exception):
    """关闭数据库连接"""
    db = g.pop('db', None)
    if db is not None:
        db.close()

# ============================================================
# API 路由
# ============================================================

@app.route('/')
def index():
    """首页"""
    return jsonify({
        'name': '产业链知识图谱 API',
        'version': '2.0',
        'endpoints': {
            'GET /api/nodes': '获取所有节点或搜索',
            'GET /api/nodes/<id>': '获取单个节点',
            'POST /api/nodes': '创建新节点',
            'PUT /api/nodes/<id>': '更新节点',
            'DELETE /api/nodes/<id>': '删除节点',
            'GET /api/fields': '获取字段配置',
            'POST /api/fields': '添加字段',
            'PUT /api/fields/<id>': '更新字段',
            'DELETE /api/fields/<id>': '删除字段',
            'GET /api/graph': '获取图谱数据',
            'GET /api/stats': '获取统计信息',
            'GET /api/logs': '查看搜索日志',
        }
    })

# -------------------- 节点管理 --------------------

@app.route('/api/nodes', methods=['GET'])
def get_nodes():
    """获取节点列表或搜索"""
    db = get_db()
    keyword = request.args.get('keyword', '')
    category = request.args.get('category', '')
    node_type = request.args.get('type', '')
    
    query = "SELECT * FROM nodes WHERE 1=1"
    params = []
    
    if keyword:
        query += " AND (name LIKE ? OR description LIKE ? OR category LIKE ?)"
        params.extend([f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'])
    
    if category:
        query += " AND category = ?"
        params.append(category)
    
    if node_type:
        query += " AND node_type = ?"
        params.append(node_type)
    
    query += " ORDER BY category, name"
    
    cursor = db.execute(query, params)
    nodes = []
    for row in cursor.fetchall():
        node = dict(row)
        node['meta'] = json.loads(node['meta'] or '[]')
        node['upstream'] = json.loads(node['upstream'] or '[]')
        node['downstream'] = json.loads(node['downstream'] or '[]')
        nodes.append(node)
    
    # 记录搜索日志
    log_search(keyword, len(nodes), len(nodes) > 0)
    
    return jsonify({
        'success': True,
        'count': len(nodes),
        'data': nodes
    })

@app.route('/api/nodes/<int:node_id>', methods=['GET'])
def get_node(node_id):
    """获取单个节点"""
    db = get_db()
    cursor = db.execute("SELECT * FROM nodes WHERE id = ?", (node_id,))
    row = cursor.fetchone()
    
    if not row:
        return jsonify({'success': False, 'error': '节点不存在'}), 404
    
    node = dict(row)
    node['meta'] = json.loads(node['meta'] or '[]')
    node['upstream'] = json.loads(node['upstream'] or '[]')
    node['downstream'] = json.loads(node['downstream'] or '[]')
    
    return jsonify({'success': True, 'data': node})

@app.route('/api/nodes', methods=['POST'])
def create_node():
    """创建新节点"""
    db = get_db()
    data = request.get_json()
    
    if not data.get('name'):
        return jsonify({'success': False, 'error': '名称不能为空'}), 400
    
    try:
        cursor = db.execute('''
            INSERT INTO nodes (name, category, node_type, description, meta, upstream, downstream)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('name'),
            data.get('category', ''),
            data.get('node_type', 'product'),
            data.get('description', ''),
            json.dumps(data.get('meta', []), ensure_ascii=False),
            json.dumps(data.get('upstream', []), ensure_ascii=False),
            json.dumps(data.get('downstream', []), ensure_ascii=False),
        ))
        db.commit()
        
        return jsonify({
            'success': True,
            'data': {'id': cursor.lastrowid, 'name': data.get('name')}
        })
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': '节点已存在'}), 400

@app.route('/api/nodes/<int:node_id>', methods=['PUT'])
def update_node(node_id):
    """更新节点"""
    db = get_db()
    data = request.get_json()
    
    cursor = db.execute("SELECT id FROM nodes WHERE id = ?", (node_id,))
    if not cursor.fetchone():
        return jsonify({'success': False, 'error': '节点不存在'}), 404
    
    db.execute('''
        UPDATE nodes SET
            name = COALESCE(?, name),
            category = COALESCE(?, category),
            node_type = COALESCE(?, node_type),
            description = COALESCE(?, description),
            meta = COALESCE(?, meta),
            upstream = COALESCE(?, upstream),
            downstream = COALESCE(?, downstream),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (
        data.get('name'),
        data.get('category'),
        data.get('node_type'),
        data.get('description'),
        json.dumps(data.get('meta', []), ensure_ascii=False) if 'meta' in data else None,
        json.dumps(data.get('upstream', []), ensure_ascii=False) if 'upstream' in data else None,
        json.dumps(data.get('downstream', []), ensure_ascii=False) if 'downstream' in data else None,
        node_id
    ))
    db.commit()
    
    return jsonify({'success': True, 'message': '更新成功'})

@app.route('/api/nodes/<int:node_id>', methods=['DELETE'])
def delete_node(node_id):
    """删除节点"""
    db = get_db()
    
    cursor = db.execute("SELECT id FROM nodes WHERE id = ?", (node_id,))
    if not cursor.fetchone():
        return jsonify({'success': False, 'error': '节点不存在'}), 404
    
    db.execute("DELETE FROM nodes WHERE id = ?", (node_id,))
    db.commit()
    
    return jsonify({'success': True, 'message': '删除成功'})

# -------------------- 字段管理 --------------------

@app.route('/api/fields', methods=['GET'])
def get_fields():
    """获取字段配置"""
    db = get_db()
    cursor = db.execute("SELECT * FROM fields ORDER BY sort_order")
    fields = []
    for row in cursor.fetchall():
        field = dict(row)
        field['options'] = json.loads(field['options'] or '[]')
        fields.append(field)
    return jsonify({'success': True, 'data': fields})

@app.route('/api/fields', methods=['POST'])
def create_field():
    """添加字段"""
    db = get_db()
    data = request.get_json()
    
    if not data.get('name') or not data.get('label'):
        return jsonify({'success': False, 'error': '字段名和标签不能为空'}), 400
    
    # 检查是否已存在
    cursor = db.execute("SELECT id FROM fields WHERE name = ?", (data['name'],))
    if cursor.fetchone():
        return jsonify({'success': False, 'error': '字段已存在'}), 400
    
    cursor = db.execute('''
        INSERT INTO fields (name, label, field_type, required, visible, editable, options, sort_order)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data['name'],
        data['label'],
        data.get('field_type', 'text'),
        data.get('required', 0),
        data.get('visible', 1),
        data.get('editable', 1),
        json.dumps(data.get('options', []), ensure_ascii=False),
        data.get('sort_order', 10),
    ))
    db.commit()
    
    # 同时添加数据库字段
    field_type = data.get('field_type', 'text')
    if field_type == 'text':
        col_type = 'TEXT'
    elif field_type in ['select', 'tags']:
        col_type = 'TEXT'
    elif field_type == 'number':
        col_type = 'INTEGER'
    else:
        col_type = 'TEXT'
    
    try:
        db.execute(f'ALTER TABLE nodes ADD COLUMN {data["name"]} {col_type}')
        db.commit()
    except sqlite3.OperationalError:
        pass  # 字段可能已存在
    
    return jsonify({'success': True, 'data': {'id': cursor.lastrowid}})

@app.route('/api/fields/<int:field_id>', methods=['PUT'])
def update_field(field_id):
    """更新字段配置"""
    db = get_db()
    data = request.get_json()
    
    db.execute('''
        UPDATE fields SET
            label = COALESCE(?, label),
            field_type = COALESCE(?, field_type),
            required = COALESCE(?, required),
            visible = COALESCE(?, visible),
            editable = COALESCE(?, editable),
            options = COALESCE(?, options),
            sort_order = COALESCE(?, sort_order)
        WHERE id = ?
    ''', (
        data.get('label'),
        data.get('field_type'),
        data.get('required'),
        data.get('visible'),
        data.get('editable'),
        json.dumps(data.get('options', []), ensure_ascii=False) if 'options' in data else None,
        data.get('sort_order'),
        field_id
    ))
    db.commit()
    
    return jsonify({'success': True, 'message': '更新成功'})

@app.route('/api/fields/<int:field_id>', methods=['DELETE'])
def delete_field(field_id):
    """删除字段"""
    db = get_db()
    
    cursor = db.execute("SELECT name FROM fields WHERE id = ?", (field_id,))
    row = cursor.fetchone()
    if not row:
        return jsonify({'success': False, 'error': '字段不存在'}), 404
    
    field_name = row['name']
    
    # 不允许删除核心字段
    core_fields = ['name', 'category', 'node_type', 'description', 'meta', 'upstream', 'downstream']
    if field_name in core_fields:
        return jsonify({'success': False, 'error': '核心字段不能删除'}), 400
    
    db.execute("DELETE FROM fields WHERE id = ?", (field_id,))
    db.commit()
    
    return jsonify({'success': True, 'message': '删除成功'})

# -------------------- 图谱数据 --------------------

@app.route('/api/graph', methods=['GET'])
def get_graph():
    """获取图谱数据（用于前端展示）"""
    db = get_db()
    keyword = request.args.get('keyword', '')
    depth = int(request.args.get('depth', 5))  # 默认上下各5层
    
    if not keyword:
        return jsonify({'success': False, 'error': '请提供关键词'}), 400
    
    # 模糊搜索中心节点
    cursor = db.execute('''
        SELECT * FROM nodes 
        WHERE name LIKE ? OR description LIKE ?
        LIMIT 1
    ''', (f'%{keyword}%', f'%{keyword}%'))
    center_row = cursor.fetchone()
    
    if not center_row:
        # 记录未找到的搜索
        log_search(keyword, 0, False)
        return jsonify({
            'success': True,
            'found': False,
            'keyword': keyword,
            'message': '暂无相关信息'
        })
    
    center = dict(center_row)
    center['meta'] = json.loads(center['meta'] or '[]')
    center['upstream'] = json.loads(center['upstream'] or '[]')
    center['downstream'] = json.loads(center['downstream'] or '[]')
    
    # 记录搜索
    log_search(keyword, 1, True)
    
    # 构建图谱数据
    nodes = []
    links = []
    visited = set()
    
    def add_node(name, direction, level):
        if not name or name in visited or level > depth:
            return
        
        visited.add(name)
        
        # 查找或创建节点
        cursor = db.execute("SELECT * FROM nodes WHERE name = ?", (name,))
        row = cursor.fetchone()
        if not row:
            # 未知节点，作为占位符
            nodes.append({
                'id': name,
                'name': name,
                'category': '',
                'node_type': 'unknown',
                'description': '（待补充）',
                'meta': [],
                'upstream': [],
                'downstream': [],
                'level': level,
                'direction': direction,
                'isPlaceholder': True
            })
            return
        
        node = dict(row)
        node['meta'] = json.loads(node['meta'] or '[]')
        node['upstream'] = json.loads(node['upstream'] or '[]')
        node['downstream'] = json.loads(node['downstream'] or '[]')
        node['level'] = level
        node['direction'] = direction
        node['isCenter'] = False
        nodes.append(node)
        
        # 添加关系
        if direction == 'center':
            # 从中心向上
            for up in node['upstream']:
                links.append({
                    'source': up,
                    'target': name,
                    'type': '依赖'
                })
                add_node(up, 'upstream', level + 1)
            # 从中心向下
            for down in node['downstream']:
                links.append({
                    'source': name,
                    'target': down,
                    'type': '产出'
                })
                add_node(down, 'downstream', level + 1)
        elif direction == 'upstream':
            # 向上继续向上
            for up in node['upstream']:
                links.append({
                    'source': up,
                    'target': name,
                    'type': '依赖'
                })
                add_node(up, 'upstream', level + 1)
        elif direction == 'downstream':
            # 向下继续向下
            for down in node['downstream']:
                links.append({
                    'source': name,
                    'target': down,
                    'type': '产出'
                })
                add_node(down, 'downstream', level + 1)
    
    # 添加中心节点
    center['isCenter'] = True
    nodes.append(center)
    visited.add(center['name'])
    
    # 展开上下游
    for up in center['upstream']:
        links.append({'source': up, 'target': center['name'], 'type': '依赖'})
        add_node(up, 'upstream', 1)
    
    for down in center['downstream']:
        links.append({'source': center['name'], 'target': down, 'type': '产出'})
        add_node(down, 'downstream', 1)
    
    return jsonify({
        'success': True,
        'found': True,
        'center': center,
        'data': {
            'nodes': nodes,
            'links': links
        }
    })

# -------------------- 统计 --------------------

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """获取统计信息"""
    db = get_db()
    
    cursor = db.execute("SELECT COUNT(*) as count FROM nodes")
    node_count = cursor.fetchone()['count']
    
    cursor = db.execute("SELECT COUNT(*) as count FROM fields")
    field_count = cursor.fetchone()['count']
    
    cursor = db.execute("SELECT COUNT(DISTINCT category) as count FROM nodes")
    category_count = cursor.fetchone()['count']
    
    cursor = db.execute("SELECT COUNT(*) as count FROM search_logs WHERE found = 0")
    not_found_count = cursor.fetchone()['count']
    
    return jsonify({
        'success': True,
        'data': {
            'node_count': node_count,
            'field_count': field_count,
            'category_count': category_count,
            'not_found_count': not_found_count
        }
    })

# -------------------- 搜索日志 --------------------

@app.route('/api/logs', methods=['GET'])
def get_logs():
    """获取搜索日志"""
    db = get_db()
    limit = int(request.args.get('limit', 50))
    
    cursor = db.execute('''
        SELECT * FROM search_logs 
        ORDER BY searched_at DESC 
        LIMIT ?
    ''', (limit,))
    
    logs = [dict(row) for row in cursor.fetchall()]
    return jsonify({'success': True, 'data': logs})

def log_search(keyword, result_count, found):
    """记录搜索"""
    db = get_db()
    db.execute('''
        INSERT INTO search_logs (keyword, result_count, found)
        VALUES (?, ?, ?)
    ''', (keyword, result_count, 1 if found else 0))
    db.commit()

# -------------------- 通知功能 --------------------

@app.route('/api/notify', methods=['POST'])
def send_notification():
    """发送通知（供前端调用）"""
    data = request.get_json()
    message = data.get('message', '')
    
    if not message:
        return jsonify({'success': False, 'error': '消息内容不能为空'}), 400
    
    # 这里可以集成各种通知渠道
    # 1. 写入通知文件供 OpenClaw 读取
    notify_file = os.path.join(os.path.dirname(__file__), '.notifications.json')
    notifications = []
    if os.path.exists(notify_file):
        with open(notify_file, 'r') as f:
            notifications = json.load(f)
    
    notifications.append({
        'message': message,
        'time': datetime.now().isoformat(),
        'read': False
    })
    
    with open(notify_file, 'w') as f:
        json.dump(notifications, f, ensure_ascii=False, indent=2)
    
    # 2. 尝试通过 HTTP 调用 OpenClaw 发送微信消息
    try:
        import requests
        # 假设 OpenClaw Gateway 运行在 18789 端口
        gateway_url = 'http://localhost:18789'
        requests.post(f'{gateway_url}/api/notify', json={'message': message}, timeout=5)
    except:
        pass  # 如果 OpenClaw 未运行，忽略
    
    return jsonify({'success': True, 'message': '通知已发送'})

@app.route('/api/notifications', methods=['GET'])
def get_notifications():
    """获取未读通知"""
    notify_file = os.path.join(os.path.dirname(__file__), '.notifications.json')
    if not os.path.exists(notify_file):
        return jsonify({'success': True, 'data': []})
    
    with open(notify_file, 'r') as f:
        notifications = json.load(f)
    
    unread = [n for n in notifications if not n.get('read', False)]
    return jsonify({'success': True, 'data': unread, 'total': len(notifications)})

@app.route('/api/notifications/mark-read', methods=['POST'])
def mark_notifications_read():
    """标记通知为已读"""
    notify_file = os.path.join(os.path.dirname(__file__), '.notifications.json')
    if not os.path.exists(notify_file):
        return jsonify({'success': True})
    
    with open(notify_file, 'r') as f:
        notifications = json.load(f)
    
    for n in notifications:
        n['read'] = True
    
    with open(notify_file, 'w') as f:
        json.dump(notifications, f, ensure_ascii=False, indent=2)
    
    return jsonify({'success': True})

# ============================================================
# 启动
# ============================================================

if __name__ == '__main__':
    init_db()
    print("=" * 50)
    print("产业链知识图谱 API 服务")
    print("=" * 50)
    print("API 地址: http://localhost:5001")
    print("管理后台: 请直接打开 admin.html")
    print("=" * 50)
    app.run(host='0.0.0.0', port=5001, debug=True)
