from sly import Lexer, Parser
import os
import csv

class SQLLexer(Lexer):
    # 定义tokens
    tokens = {
        'CREATE', 'TABLE', 'INSERT', 'INTO', 'VALUES',
        'ID', 'STRING', 'INTEGER', 'FLOAT_NUM', 'COMMA', 'LPAREN', 'RPAREN',
        'CHAR', 'INT', 'FLOAT', 'UPDATE', 'SET', 'EQUALS', 'WHERE', 'DELETE', 'FROM',
        'SELECT', 'STAR', 'AND'
    }
    
    # 移除空格忽略规则，改为显式处理
    ignore = '\t\n'  # 只忽略制表符和换行
    
    # 定义词法规则
    CREATE = r'CREATE'
    TABLE = r'TABLE'
    INSERT = r'INSERT'
    INTO = r'INTO'
    VALUES = r'VALUES'
    CHAR = r'CHAR'
    INT = r'INT'
    FLOAT = r'FLOAT'
    UPDATE = r'UPDATE'
    SET = r'SET'
    EQUALS = r'='
    WHERE = r'WHERE'
    DELETE = r'DELETE'
    FROM = r'FROM'
    SELECT = r'SELECT'
    STAR = r'\*'
    AND = r'AND'
    
    COMMA = r','
    LPAREN = r'\('
    RPAREN = r'\)'
    
    # 添加空格token规则（但不保存）
    @_(r'[ ]+') # type: ignore
    def ignore_space(self, t):
        pass

    # 先捕获以数字开头的SQL命令
    @_(r'[ ]*\d+[ ]*(?:UPDATE|CREATE|INSERT|SELECT|DELETE)')  # type: ignore # 只匹配以数字开头的SQL关键字
    def error_id(self, t):
        value = t.value.strip()
        raise Exception(f"错误: SQL命令不能以数字开头 '{value}'")

    # 处理数字字面量
    @_(r'\d+\.\d+') # type: ignore
    def FLOAT_NUM(self, t):
        t.value = float(t.value)
        return t

    @_(r'\d+') # type: ignore
    def INTEGER(self, t):
        t.value = int(t.value)
        return t

    # 最后处理普通标识符
    @_(r'[a-zA-Z_][a-zA-Z0-9_]*') # type: ignore
    def ID(self, t):
        if t.value.upper() in {'CREATE', 'TABLE', 'INSERT', 'INTO', 'VALUES', 
                              'CHAR', 'INT', 'FLOAT', 'UPDATE', 'SET', 'WHERE', 
                              'DELETE', 'FROM', 'SELECT'}:
            t.type = t.value.upper()
            t.value = t.value.upper()
        else:
            t.type = 'ID'
            t.value = t.value
        return t

    @_(r'\'[^\']*\'|\"[^\"]*\"') # type: ignore
    def STRING(self, t):
        # 简单地去掉引号，不打调试信息
        t.value = t.value[1:-1]
        return t

class SQLParser(Parser):
    tokens = SQLLexer.tokens

    def __init__(self):
        # 设置数据目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(current_dir, 'data')
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    @_('create_table', # type: ignore
       'insert_statement', 
       'update_statement', 
       'delete_statement', 
       'select_statement') 
    def statement(self, p):
        return p[0]

    @_('CREATE TABLE ID LPAREN typed_column_list RPAREN') # type: ignore
    def create_table(self, p):
        try:
            table_name = p.ID
            table_dir = os.path.join(self.data_dir, table_name)
            if os.path.exists(table_dir):
                return f"表 {table_name} 已存在"
            os.makedirs(table_dir)
            
            # 保持列名的原始大小写
            columns = [col[0] for col in p.typed_column_list]  # 移除.lower()
            
            # 保存schema时也保持原始大小写
            schema_file = os.path.join(table_dir, f'{table_name}_schema.csv')
            with open(schema_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['column_name', 'data_type'])
                for col, type_ in p.typed_column_list:
                    writer.writerow([col, type_])  # 移除.lower()
            
            # 创建数据文件
            data_file = os.path.join(table_dir, f'{table_name}_data.csv')
            with open(data_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
            
            return f"表 {table_name} 创建成功"
        except Exception as e:
            return f"错误: {str(e)}"

    @_('typed_column') # type: ignore
    def typed_column_list(self, p):
        return [p.typed_column]

    @_('typed_column COMMA typed_column_list') # type: ignore
    def typed_column_list(self, p):
        return [p.typed_column] + p.typed_column_list

    @_('ID type') # type: ignore
    def typed_column(self, p):
        return (p.ID, p.type)

    @_('CHAR', 'INT', 'FLOAT') # type: ignore
    def type(self, p):
        return p[0]

    @_('INSERT INTO ID VALUES LPAREN value_list RPAREN') # type: ignore 
    def insert_statement(self, p):
        try:
            # 获取表名
            table_name = p.ID
            # 获取表目录
            table_dir = os.path.join(self.data_dir, table_name)
            if not os.path.exists(table_dir):
                return f"表 {table_name} 不存在"

            # 读取schema
            schema_file = os.path.join(table_dir, f'{table_name}_schema.csv')
            with open(schema_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                schema = {row['column_name']: row['data_type'] for row in reader}

            # 重读取schema文件以获取列名顺序
            with open(schema_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                columns = [row['column_name'] for row in reader]

            # 检查列数是否匹配
            if len(columns) != len(p.value_list):
                return f"错误: 列数不匹配，需要 {len(columns)} 列，但提供了 {len(p.value_list)} 列"

            # 类型检查
            processed_values = []
            for i, value in enumerate(p.value_list):
                col_name = columns[i]
                col_type = schema[col_name]
                
                # 检查数据类型是否匹配
                if col_type == 'INT':
                    if not isinstance(value, int):
                        # 如果是字符串，显示带引号的值
                        display_value = f"'{value}'" if isinstance(value, str) else str(value)
                        return f"错误: 列 {col_name} 需要整数类型（不带引号），但提供了 {display_value}"
                    processed_values.append(str(value))
                elif col_type == 'FLOAT':
                    if not isinstance(value, float):
                        # 如果是字符串，显示带引号的值
                        display_value = f"'{value}'" if isinstance(value, str) else str(value)
                        return f"错误: 列 {col_name} 需要浮点数类型（带小数点），但提供了 {display_value}"
                    processed_values.append(str(value))
                else:  # CHAR类型
                    if not isinstance(value, str):
                        # 对于非字符串值，显示原始值
                        return f"错误: 列 {col_name} 需要字符串类型（带引号），但提供了 {value}"
                    processed_values.append(value)

            # 写入数据
            data_file = os.path.join(table_dir, f'{table_name}_data.csv')
            with open(data_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(processed_values)

            return "插入成功"
        except Exception as e:
            return f"错误: {str(e)}"

    @_('value') # type: ignore
    def value_list(self, p):
        return [p.value]

    @_('value COMMA value_list') # type: ignore
    def value_list(self, p):
        return [p.value] + p.value_list

    @_('STRING', 'INTEGER', 'FLOAT_NUM') # type: ignore
    def value(self, p):
        return p[0]

    @_('UPDATE ID SET ID EQUALS value WHERE ID EQUALS value') # type: ignore
    def update_statement(self, p):
        try:
            table_name = p.ID0  # 表名
            table_dir = os.path.join(self.data_dir, table_name)
            if not os.path.exists(table_dir):
                return f"表 {table_name} 不存在"

            # 读取schema
            schema_file = os.path.join(table_dir, f'{table_name}_schema.csv')
            with open(schema_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                schema = {row['column_name']: row['data_type'] for row in reader}
                columns = list(schema.keys())

            update_col = p.ID1  # 要更新的列
            where_col = p.ID2   # WHERE条件的列
            
            # 检查列是否存在
            if update_col not in schema:
                return f"错误: 列 {update_col} 不存在"
            if where_col not in schema:
                return f"错误: 列 {where_col} 不存在"

            # 类型检查
            new_value = p.value0
            where_value = p.value1

            # 检查更新值的类型
            if schema[update_col] == 'INT':
                if not isinstance(new_value, int):
                    return f"错误: 列 {update_col} 需要整数类型（不带''号），但提供了 {new_value}"
            elif schema[update_col] == 'FLOAT':
                if not isinstance(new_value, float):
                    return f"错误: 列 {update_col} 需要浮点数类型（带小数点），但提供了 {new_value}"
            elif schema[update_col] == 'CHAR':
                if not isinstance(new_value, str):
                    return f"错误: 列 {update_col} 需要字符串类型（带引号），但提供了 {new_value}"

            # 检查WHERE条件值的类型
            if schema[where_col] == 'INT':
                if not isinstance(where_value, int):
                    return f"错误: WHERE条件的值需要整数类型（不带引号），但提供了 {where_value}"
            elif schema[where_col] == 'FLOAT':
                if not isinstance(where_value, float):
                    return f"错误: WHERE条件的值需要浮点数类型（带小数点），但提供了 {where_value}"
            elif schema[where_col] == 'CHAR':
                if not isinstance(where_value, str):
                    return f"错误: WHERE条件的值需要字符串类型（带引号），但提供了 {where_value}"

            # 读取数据文件
            data_file = os.path.join(table_dir, f'{table_name}_data.csv')
            rows = []
            update_count = 0
            with open(data_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row[where_col] == str(where_value):
                        # 检查新值是否与当前值相同
                        if row[update_col] == str(new_value):
                            rows.append(row)  # 保留原记录
                            continue  # 跳过新
                        row[update_col] = str(new_value)
                        update_count += 1
                    rows.append(row)

            # 只有在有实际更新时才写入文件
            if update_count > 0:
                with open(data_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=columns)
                    writer.writeheader()
                    writer.writerows(rows)
                return f"更新了 {update_count} 条记录"
            else:
                return "没有记录需要更新"

        except Exception as e:
            return f"错误: {str(e)}"

    @_('DELETE FROM ID WHERE ID EQUALS value') # type: ignore
    def delete_statement(self, p):
        try:
            table_name = p.ID0  # 表名
            table_dir = os.path.join(self.data_dir, table_name)
            if not os.path.exists(table_dir):
                return f"表 {table_name} 不存在"

            # 读取schema
            schema_file = os.path.join(table_dir, f'{table_name}_schema.csv')
            with open(schema_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                schema = {row['column_name']: row['data_type'] for row in reader}
                columns = list(schema.keys())

            where_col = p.ID1   # WHERE条件的列
            where_value = p.value
            
            # 检查列是否存在
            if where_col not in schema:
                return f"错误: 列 {where_col} 不存在"

            # 检查WHERE条件值的类型
            if schema[where_col] == 'INT':
                if not isinstance(where_value, int):
                    return f"错误: WHERE条件的值需要整数类型（不带引号），但提供了 {where_value}"
            elif schema[where_col] == 'FLOAT':
                if not isinstance(where_value, float):
                    return f"错误: WHERE条件的值需要浮点数类型（带小数点），但提供了 {where_value}"
            elif schema[where_col] == 'CHAR':
                if not isinstance(where_value, str):
                    return f"错误: WHERE条件的值需要字符串类型（带��号），但提供了 {where_value}"

            # 读取数据文件
            data_file = os.path.join(table_dir, f'{table_name}_data.csv')
            rows = []
            delete_count = 0
            with open(data_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row[where_col] == str(where_value):
                        delete_count += 1
                        continue  # 跳过要删除的行
                    rows.append(row)

            # 只有在有实际删除时才写入文件
            if delete_count > 0:
                with open(data_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=columns)
                    writer.writeheader()
                    writer.writerows(rows)
                return f"删除了 {delete_count} 条记录"
            else:
                return "没有记录需要删除"

        except Exception as e:
            return f"错误: {str(e)}"

    @_('SELECT select_columns FROM ID WHERE ID EQUALS value AND ID EQUALS value') # type: ignore
    def select_statement(self, p):
        try:
            table_name = p.ID0  # 第一个ID是表名
            table_dir = os.path.join(self.data_dir, table_name)
            if not os.path.exists(table_dir):
                return f"表 {table_name} 不存在"

            # 读取schema，保持原始顺序
            schema_file = os.path.join(table_dir, f'{table_name}_schema.csv')
            schema = {}
            original_columns = []  # 保存原始列顺序
            with open(schema_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    col_name = row['column_name']
                    schema[col_name] = row['data_type']
                    original_columns.append(col_name)

            # 获取要查询的列
            if isinstance(p.select_columns, str) and p.select_columns == '*':
                columns = original_columns  # 使用原始顺序
            else:
                columns = p.select_columns  # 使用SELECT中指定的顺序
                # 检查列是否存在
                for col in columns:
                    if col not in schema:
                        return f"错误: 列 {col} 不存在"

            # 检查第一个WHERE条件列是否存在
            where_col1 = p.ID1
            if where_col1 not in schema:
                return f"错误: 列 {where_col1} 不存在"

            # 检查第二个WHERE条件列是否存在
            where_col2 = p.ID2
            if where_col2 not in schema:
                return f"错误: 列 {where_col2} 不存在"

            # 检查第一个WHERE条件值的类型
            where_value1 = p.value0
            if schema[where_col1] == 'INT':
                if not isinstance(where_value1, int):
                    return f"错误: WHERE条件的值需要整数类型（不带引号），但提供了 {where_value1}"
            elif schema[where_col1] == 'FLOAT':
                if not isinstance(where_value1, float):
                    return f"错误: WHERE条件的值需要浮点数类型（带小数点），但提供了 {where_value1}"
            elif schema[where_col1] == 'CHAR':
                if not isinstance(where_value1, str):
                    return f"错误: WHERE条件的值需要字符串类型（带引号），但提供了 {where_value1}"

            # 检查第二个WHERE条件值的类型
            where_value2 = p.value1
            if schema[where_col2] == 'INT':
                if not isinstance(where_value2, int):
                    return f"错误: WHERE条件的值需要整数类型（不带引号），但提供了 {where_value2}"
            elif schema[where_col2] == 'FLOAT':
                if not isinstance(where_value2, float):
                    return f"错误: WHERE条件的值需要浮点数类型（带小数点），但提供了 {where_value2}"
            elif schema[where_col2] == 'CHAR':
                if not isinstance(where_value2, str):
                    return f"错误: WHERE条件的值需要字符串类型（带引号），但提供了 {where_value2}"

            # 读取数据并应用WHERE条件
            data_file = os.path.join(table_dir, f'{table_name}_data.csv')
            result = []
            with open(data_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # 检查两个WHERE条件
                    if (row[where_col1] == str(where_value1) and 
                        row[where_col2] == str(where_value2)):
                        # 使用列表来保持顺序
                        ordered_row = []
                        for col in columns:  # 按照指定的列顺序创建结果
                            ordered_row.append([col, row[col]])
                        result.append(ordered_row)

            return result

        except Exception as e:
            return f"错误: {str(e)}"

    @_('SELECT select_columns FROM ID') # type: ignore
    def select_statement(self, p):
        try:
            table_name = p.ID
            table_dir = os.path.join(self.data_dir, table_name)
            if not os.path.exists(table_dir):
                return f"表 {table_name} 不存在"

            # 读取schema，保持原始顺序
            schema_file = os.path.join(table_dir, f'{table_name}_schema.csv')
            schema = {}
            original_columns = []  # 保存原始列顺序
            with open(schema_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    col_name = row['column_name']
                    schema[col_name] = row['data_type']
                    original_columns.append(col_name)

            # 获取要查询的列
            if isinstance(p.select_columns, str) and p.select_columns == '*':
                columns = original_columns  # 使用原始顺序
            else:
                columns = p.select_columns  # 使用SELECT中指定的顺序
                # 检查列是否存在
                for col in columns:
                    if col not in schema:
                        return f"错误: 列 {col} 不存在"

            # 读取数据
            data_file = os.path.join(table_dir, f'{table_name}_data.csv')
            result = []
            with open(data_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # 使用列表来保持顺序
                    ordered_row = []
                    for col in columns:  # 按照指定的列顺序创建结果
                        ordered_row.append([col, row[col]])
                    result.append(ordered_row)

            return result

        except Exception as e:
            return f"错误: {str(e)}"

    @_('STAR')  # type: ignore
    def select_columns(self, p):
        return '*'

    @_('column_list')  # type: ignore
    def select_columns(self, p):
        return p.column_list

    @_('ID')  # type: ignore
    def column_list(self, p):
        return [p.ID]

    @_('ID COMMA column_list')  # type: ignore
    def column_list(self, p):
        return [p.ID] + p.column_list