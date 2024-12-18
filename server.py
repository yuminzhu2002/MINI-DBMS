from flask import Flask, request, jsonify, render_template
from sql_parser import SQLLexer, SQLParser, SQLError
from sql_executor import SQLExecutor
import os
import re

app = Flask(__name__)

def split_sql_statements(sql: str) -> list:
    """
    分割SQL语句，同时保持语句的完整性
    支持可选的分号结尾
    """
    # 移除所有换行符，替换为空格
    sql = sql.replace('\n', ' ').replace('\r', ' ')
    
    # 使用正则表达式分割语句，保持分号
    statements = []
    current_stmt = ''
    in_quotes = False
    quote_char = None
    
    for char in sql:
        if char in ["'", '"']:
            if not in_quotes:
                in_quotes = True
                quote_char = char
            elif quote_char == char:
                in_quotes = False
            current_stmt += char
        elif char == ';' and not in_quotes:
            current_stmt = current_stmt.strip()
            if current_stmt:
                statements.append(current_stmt)
            current_stmt = ''
        else:
            current_stmt += char
    
    # 添加最后一个语句（如果有的话）
    current_stmt = current_stmt.strip()
    if current_stmt:
        statements.append(current_stmt)
    
    # 如果没有语句，返回原始SQL作为单个语句
    if not statements and sql.strip():
        statements.append(sql.strip())
    
    return statements

@app.route('/')
def index():
    return render_template('a.html')

@app.route('/execute', methods=['POST'])
def execute_sql():
    sql = request.json.get('sql', '')
    print(f"\n收到SQL语句: {sql}")
    
    try:
        # 分割SQL语句
        statements = split_sql_statements(sql)
        if not statements:
            return jsonify({
                'success': False,
                'result': "错误: 没有找到有效的SQL语句"
            })
        
        # 创建词法分析器和解析器实例
        lexer = SQLLexer()
        parser = SQLParser()
        
        # 创建SQL执行器（使用默认数据目录）
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(current_dir, 'data')
        executor = SQLExecutor(data_dir)
        
        # 存储所有语句的执行结果
        results = []
        
        # 解析并执行每个语句，遇到错误立即停止
        for stmt in statements:
            try:
                # 词法分析
                tokens = list(lexer.tokenize(stmt))
                print(f"\n语句 '{stmt}' 的词法分析结果:")
                for token in tokens:
                    print(f"Token: {token.type}, Value: {token.value}")
                
                # 语法分析
                parsed_stmt = parser.parse(iter(tokens))
                if parsed_stmt is None:
                    results.append({
                        'statement': stmt,
                        'success': False,
                        'result': "语法错误"
                    })
                    # 遇到错误立即返回所有结果
                    return jsonify({
                        'success': False,
                        'result': results
                    })
                
                # 执行语句
                try:
                    result = executor.execute([parsed_stmt])
                    results.append({
                        'statement': stmt,
                        'success': True,
                        'result': result[0]['result'] if result else None
                    })
                except Exception as e:
                    results.append({
                        'statement': stmt,
                        'success': False,
                        'result': str(e)
                    })
                    # 遇到执行错误立即返回所有结果
                    return jsonify({
                        'success': False,
                        'result': results
                    })
                    
            except Exception as e:
                results.append({
                    'statement': stmt,
                    'success': False,
                    'result': f"语法分析错误: {str(e)}"
                })
                # 遇到解析错误立即返回所有结果
                return jsonify({
                    'success': False,
                    'result': results
                })
        
        # 所有语句执行成功
        return jsonify({
            'success': True,
            'result': results
        })
            
    except Exception as e:
        print(f"\n执行过程发生异常: {str(e)}")
        import traceback
        print("\n详细错误信息:")
        print(traceback.format_exc())
        return jsonify({
            'success': False,
            'result': f"执行错误: {str(e)}"
        })

if __name__ == '__main__':
    app.run(debug=True) 
