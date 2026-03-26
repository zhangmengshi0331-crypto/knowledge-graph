"""
产业链知识图谱 API 服务
"""

from flask import Flask, request, jsonify, g, send_from_directory
from flask_cors import CORS
import sqlite3
import json
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=BASE_DIR)
app.config['DATABASE'] = 'knowledge_graph.db'
CORS(app)

# ============================================================
# 数据库
# ============================================================

def get_db_path():
    return os.path.join(BASE_DIR, app.config['DATABASE'])

def init_db():
    db = sqlite3.connect(get_db_path())
    db.row_factory = sqlite3.Row
    
    db.execute('''CREATE TABLE IF NOT EXISTS nodes (
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
    )''')
    
    db.execute('''CREATE TABLE IF NOT EXISTS fields (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        label TEXT NOT NULL,
        field_type TEXT DEFAULT 'text',
        required INTEGER DEFAULT 0,
        visible INTEGER DEFAULT 1,
        editable INTEGER DEFAULT 1,
        options TEXT,
        sort_order INTEGER DEFAULT 0
    )''')
    
    db.execute('''CREATE TABLE IF NOT EXISTS search_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword TEXT NOT NULL,
        result_count INTEGER DEFAULT 0,
        found INTEGER DEFAULT 0,
        searched_at TEXT DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # 默认字段
    defaults = [
        ('category', '行业分类', 'text', 1, 1, 1, None, 1),
        ('name', '名称', 'text', 1, 1, 1, None, 2),
        ('node_type', '类型', 'select', 1, 1, 1, '["行业","产品","材料","事件"]', 3),
        ('description', '描述', 'textarea', 0, 1, 1, None, 4),
        ('meta', '关键信息', 'tags', 0, 1, 1, None, 5),
        ('upstream', '上游', 'tags', 0, 1, 1, None, 6),
        ('downstream', '下游', 'tags', 0, 1, 1, None, 7),
    ]
    
    cur = db.execute("SELECT COUNT(*) FROM fields")
    if cur.fetchone()[0] == 0:
        for f in defaults:
            db.execute('INSERT INTO fields (name,label,field_type,required,visible,editable,options,sort_order) VALUES (?,?,?,?,?,?,?,?)', f)
    
    db.commit()
    import_data(db)
    db.close()

def import_data(db):
    cur = db.execute("SELECT COUNT(*) FROM nodes")
    if cur.fetchone()[0] > 0:
        return
    
    jf = os.path.join(BASE_DIR, 'industry-data.json')
    if not os.path.exists(jf):
        print("[警告] 未找到 industry-data.json")
        return
    
    with open(jf, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    for name, node in data.items():
        db.execute('''INSERT OR IGNORE INTO nodes (name,category,node_type,description,meta,upstream,downstream) 
            VALUES (?,?,?,?,?,?,?)''', (
            node.get('name', name),
            node.get('category', ''),
            node.get('type', 'product'),
            node.get('description', ''),
            json.dumps(node.get('meta', []), ensure_ascii=False),
            json.dumps(node.get('upstream', []), ensure_ascii=False),
            json.dumps(node.get('downstream', []), ensure_ascii=False),
        ))
    db.commit()
    print(f"[初始化] 导入 {len(data)} 条数据")

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(get_db_path())
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(e):
    db = g.pop('db', None)
    if db: db.close()

# ============================================================
# 页面路由
# ============================================================

@app.route('/')
def home():
    return send_from_directory(BASE_DIR, 'index.html')

@app.route('/admin.html')
def admin():
    return send_from_directory(BASE_DIR, 'admin.html')

# ============================================================
# API
# ============================================================

@app.route('/api')
def api_info():
    return jsonify({'name': '产业链知识图谱 API', 'version': '2.0'})

@app.route('/api/stats')
def stats():
    db = get_db()
    return jsonify({'success': True, 'data': {
        'node_count': db.execute("SELECT COUNT(*) FROM nodes").fetchone()[0],
        'field_count': db.execute("SELECT COUNT(*) FROM fields").fetchone()[0],
        'category_count': db.execute("SELECT COUNT(DISTINCT category) FROM nodes").fetchone()[0],
        'not_found_count': db.execute("SELECT COUNT(*) FROM search_logs WHERE found=0").fetchone()[0]
    }})

@app.route('/api/nodes')
def get_nodes():
    db = get_db()
    kw = request.args.get('keyword', '')
    q = "SELECT * FROM nodes WHERE 1=1"
    p = []
    if kw:
        q += " AND (name LIKE ? OR description LIKE ?)"
        p = [f'%{kw}%', f'%{kw}%']
    cur = db.execute(q + " ORDER BY category,name", p)
    nodes = []
    for r in cur.fetchall():
        n = dict(r)
        n['meta'] = json.loads(n['meta'] or '[]')
        n['upstream'] = json.loads(n['upstream'] or '[]')
        n['downstream'] = json.loads(n['downstream'] or '[]')
        nodes.append(n)
    return jsonify({'success': True, 'count': len(nodes), 'data': nodes})

@app.route('/api/nodes/<int:nid>')
def get_node(nid):
    db = get_db()
    r = db.execute("SELECT * FROM nodes WHERE id=?", (nid,)).fetchone()
    if not r:
        return jsonify({'success': False, 'error': '不存在'}), 404
    n = dict(r)
    n['meta'] = json.loads(n['meta'] or '[]')
    n['upstream'] = json.loads(n['upstream'] or '[]')
    n['downstream'] = json.loads(n['downstream'] or '[]')
    return jsonify({'success': True, 'data': n})

@app.route('/api/nodes', methods=['POST'])
def create_node():
    db = get_db()
    d = request.get_json()
    if not d.get('name'):
        return jsonify({'success': False, 'error': '名称必填'}), 400
    try:
        cur = db.execute('''INSERT INTO nodes (name,category,node_type,description,meta,upstream,downstream) 
            VALUES (?,?,?,?,?,?,?)''', (
            d['name'], d.get('category',''), d.get('node_type','product'), d.get('description',''),
            json.dumps(d.get('meta',[]), ensure_ascii=False),
            json.dumps(d.get('upstream',[]), ensure_ascii=False),
            json.dumps(d.get('downstream',[]), ensure_ascii=False)
        ))
        db.commit()
        return jsonify({'success': True, 'data': {'id': cur.lastrowid}})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': '已存在'}), 400

@app.route('/api/nodes/<int:nid>', methods=['PUT'])
def update_node(nid):
    db = get_db()
    d = request.get_json()
    if not db.execute("SELECT id FROM nodes WHERE id=?", (nid,)).fetchone():
        return jsonify({'success': False, 'error': '不存在'}), 404
    db.execute('''UPDATE nodes SET name=COALESCE(?,name),category=COALESCE(?,category),
        node_type=COALESCE(?,node_type),description=COALESCE(?,description),
        meta=COALESCE(?,meta),upstream=COALESCE(?,upstream),downstream=COALESCE(?,downstream),
        updated_at=CURRENT_TIMESTAMP WHERE id=?''', (
        d.get('name'), d.get('category'), d.get('node_type'), d.get('description'),
        json.dumps(d.get('meta',[]), ensure_ascii=False) if 'meta' in d else None,
        json.dumps(d.get('upstream',[]), ensure_ascii=False) if 'upstream' in d else None,
        json.dumps(d.get('downstream',[]), ensure_ascii=False) if 'downstream' in d else None,
        nid
    ))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/nodes/<int:nid>', methods=['DELETE'])
def delete_node(nid):
    db = get_db()
    if not db.execute("SELECT id FROM nodes WHERE id=?", (nid,)).fetchone():
        return jsonify({'success': False, 'error': '不存在'}), 404
    db.execute("DELETE FROM nodes WHERE id=?", (nid,))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/graph')
def get_graph():
    db = get_db()
    kw = request.args.get('keyword', '')
    depth = int(request.args.get('depth', 0))
    if depth <= 0:
        depth = 999  # 0 或不传表示追溯全部
    
    if not kw:
        return jsonify({'success': False, 'error': '请提供关键词'}), 400
    
    # 搜索中心节点：优先精确匹配名称，再模糊匹配
    r = db.execute("SELECT * FROM nodes WHERE name=? LIMIT 1", (kw,)).fetchone()
    if not r:
        r = db.execute("SELECT * FROM nodes WHERE name LIKE ? LIMIT 1", (f'%{kw}%',)).fetchone()
    if not r:
        r = db.execute("SELECT * FROM nodes WHERE description LIKE ? LIMIT 1", (f'%{kw}%',)).fetchone()
    
    if not r:
        db.execute("INSERT INTO search_logs (keyword,result_count,found) VALUES (?,?,0)", (kw, 0))
        db.commit()
        return jsonify({'success': True, 'found': False, 'keyword': kw, 'message': '暂无相关信息'})
    
    center = dict(r)
    center['meta'] = json.loads(center['meta'] or '[]')
    center['upstream'] = json.loads(center['upstream'] or '[]')
    center['downstream'] = json.loads(center['downstream'] or '[]')
    
    db.execute("INSERT INTO search_logs (keyword,result_count,found) VALUES (?,?,1)", (kw, 1))
    db.commit()
    
    nodes, links, visited = [], [], set()
    
    def add_node(name, direction, level):
        if not name or name in visited or level > depth:
            return None
        visited.add(name)
        r = db.execute("SELECT * FROM nodes WHERE name=?", (name,)).fetchone()
        if not r:
            node = {'id': name, 'name': name, 'category': '', 'node_type': 'unknown',
                'description': '（待补充）', 'meta': [], 'upstream': [], 'downstream': [],
                'level': level, 'direction': direction, 'isPlaceholder': True}
            nodes.append(node)
            return node
        n = dict(r)
        n['meta'] = json.loads(n['meta'] or '[]')
        n['upstream'] = json.loads(n['upstream'] or '[]')
        n['downstream'] = json.loads(n['downstream'] or '[]')
        n['level'], n['direction'], n['isCenter'] = level, direction, False
        nodes.append(n)
        return n
    
    def build_links(node, direction, level):
        if not node or level >= depth:
            return
        if direction == 'upstream':
            for up in node.get('upstream', []):
                if up and up not in visited:
                    child = add_node(up, 'upstream', level + 1)
                    if child:
                        links.append({'source': up, 'target': node['name'], 'type': '依赖'})
                        build_links(child, 'upstream', level + 1)
        else:
            for down in node.get('downstream', []):
                if down and down not in visited:
                    child = add_node(down, 'downstream', level + 1)
                    if child:
                        links.append({'source': node['name'], 'target': down, 'type': '产出'})
                        build_links(child, 'downstream', level + 1)
    
    center['isCenter'] = True
    nodes.append(center)
    visited.add(center['name'])
    
    # 处理上游：先添加节点，再添加链接
    for up in center['upstream']:
        if up and up not in visited:
            child = add_node(up, 'upstream', 1)
            if child:
                links.append({'source': up, 'target': center['name'], 'type': '依赖'})
                build_links(child, 'upstream', 1)
    
    # 处理下游：先添加节点，再添加链接
    for down in center['downstream']:
        if down and down not in visited:
            child = add_node(down, 'downstream', 1)
            if child:
                links.append({'source': center['name'], 'target': down, 'type': '产出'})
                build_links(child, 'downstream', 1)
    
    return jsonify({'success': True, 'found': True, 'center': center, 'data': {'nodes': nodes, 'links': links}})

@app.route('/api/fields')
def get_fields():
    db = get_db()
    fields = []
    for r in db.execute("SELECT * FROM fields ORDER BY sort_order"):
        f = dict(r)
        f['options'] = json.loads(f['options'] or '[]')
        fields.append(f)
    return jsonify({'success': True, 'data': fields})

@app.route('/api/logs')
def get_logs():
    db = get_db()
    limit = int(request.args.get('limit', 50))
    logs = [dict(r) for r in db.execute("SELECT * FROM search_logs ORDER BY searched_at DESC LIMIT ?", (limit,))]
    return jsonify({'success': True, 'data': logs})

@app.route('/api/notify', methods=['POST'])
def notify():
    d = request.get_json()
    msg = d.get('message', '')
    if not msg:
        return jsonify({'success': False, 'error': '消息为空'}), 400
    
    # 记录到本地文件
    nf = os.path.join(BASE_DIR, '.notifications.json')
    notifs = json.load(open(nf)) if os.path.exists(nf) else []
    notifs.append({'message': msg, 'time': datetime.now().isoformat(), 'read': False})
    with open(nf, 'w') as f:
        json.dump(notifs, f, ensure_ascii=False, indent=2)
    
    # 通过写入通知文件 + 触发 cron 发送微信
    try:
        # 写入待发送通知文件，由 cron 任务读取并发送
        pending_file = os.path.join(BASE_DIR, '.pending_notify.json')
        pending = json.load(open(pending_file)) if os.path.exists(pending_file) else []
        pending.append({'message': msg, 'time': datetime.now().isoformat()})
        with open(pending_file, 'w') as f:
            json.dump(pending, f, ensure_ascii=False, indent=2)
        print(f"[通知] 已写入待发送通知: {msg[:30]}...")
    except Exception as e:
        print(f"[通知] 写入通知失败: {e}")
    
    return jsonify({'success': True})

if __name__ == '__main__':
    init_db()
    print("\n" + "=" * 60)
    print("🏭 产业链知识图谱 API 服务已启动")
    print("=" * 60)
    print("\n📖 知识图谱页面: http://localhost:5001")
    print("🔧 管理后台页面: http://localhost:5001/admin.html\n")
    print("=" * 60 + "\n")
    app.run(host='0.0.0.0', port=5001, debug=True)
