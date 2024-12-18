from flask import Flask, request, jsonify, render_template
from sql_parser import SQLLexer, SQLParser

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('a.html')

# 首页路由
@app.route('/')
def home():
    return '''
    <h1>SQL解析器API</h1>
    <p>使用POST请求访问 /execute 端点来执行SQL查询</p>
    <p>请求体格式示例：{"sql": "SELECT * FROM table_name"}</p>
    '''

# SQL执行接口
@app.route('/execute', methods=['POST'])
def execute_sql():
    # 获取SQL语句
    sql = request.json.get('sql', '')
    print(f"收到SQL语句: {sql}")
    
    try:
        # 创建词法分析器和解析器实例
        lexer = SQLLexer()
        parser = SQLParser()
        
        try:
            # 词法分析
            tokens = list(lexer.tokenize(sql))
        except Exception as e:
            return jsonify({
                'success': False,
                'result': f"词法分析错误: {str(e)}"
            })
            
        # 执行语法分析和SQL操作
        result = parser.parse(iter(tokens))
        
        if result is None:
            return jsonify({
                'success': False,
                'result': "语法错误"
            })

        # 检查结果是否为错误消息
        if isinstance(result, str) and (
            '错误' in result or 
            result.startswith('错误:') or 
            '不存在' in result or  # 添加"不存在"的检查
            '已存在' in result     # 添加"已存在"的检查
        ):
            return jsonify({
                'success': False,
                'result': result
            })
            
        return jsonify({
            'success': True,
            'result': result
        })
            
    except Exception as e:
        return jsonify({
            'success': False,
            'result': f"执行错误: {str(e)}"
        })

if __name__ == '__main__':
    app.run(debug=True) 