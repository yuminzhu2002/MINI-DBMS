from sly import Lexer, Parser
import os
import csv
from typing import List, Tuple, Any, Optional
from dataclasses import dataclass
from enum import Enum, auto

# 自定义异常类
class SQLError(Exception):
    """SQL错误的基类"""
    pass

class SQLSyntaxError(SQLError):
    """SQL语法错误"""
    pass

class SQLTypeError(SQLError):
    """SQL类型错误"""
    pass

class SQLExecutionError(SQLError):
    """SQL执行错误"""
    pass

# 数据类型枚举
class DataType(Enum):
    CHAR = auto()
    INT = auto()
    FLOAT = auto()

    @staticmethod
    def from_string(type_str: str) -> 'DataType':
        type_map = {
            'CHAR': DataType.CHAR,
            'INT': DataType.INT,
            'FLOAT': DataType.FLOAT
        }
        if type_str.upper() not in type_map:
            raise SQLTypeError(f"不支持的数据类型: {type_str}")
        return type_map[type_str.upper()]

# 数据结构定义
@dataclass
class Column:
    name: str
    data_type: DataType

@dataclass
class Table:
    name: str
    columns: List[Column]

@dataclass
class Condition:
    column: str
    operator: str
    value: Any
    logic_op: str = 'AND'  # 默认为AND

@dataclass
class SQLStatement:
    """SQL语句的基类"""
    pass

@dataclass
class CreateTableStatement(SQLStatement):
    table: Table

@dataclass
class InsertStatement(SQLStatement):
    table_name: str
    values: List[Any]

@dataclass
class SelectStatement(SQLStatement):
    tables: List[str]  # 改为支持多个表
    columns: List[Tuple[str, str]]  # (table_name, column_name)
    conditions: Optional[List[Condition]] = None

@dataclass
class UpdateValue:
    """更新值的数据结构"""
    column: Optional[str] = None  # 用于column + value形式
    operator: Optional[str] = None  # 运算符：+, -, *, /
    value: Any = None  # 具体的值

@dataclass
class UpdateStatement(SQLStatement):
    table_name: str
    column: str
    value: Any
    conditions: Optional[List[Condition]] = None

@dataclass
class DeleteStatement(SQLStatement):
    table_name: str
    conditions: List[Condition]

class SQLLexer(Lexer):
    tokens = {
        'IDENTIFIER',
        'STRING',
        'FLOAT',
        'INT',
        'CREATE',
        'TABLE',
        'INSERT',
        'INTO',
        'VALUES',
        'SELECT',
        'FROM',
        'WHERE',
        'UPDATE',
        'SET',
        'DELETE',
        'AND',
        'OR',
        'CHAR',
        'FLOAT_TYPE',
        'INT_TYPE',
        'STAR',
        'EQUALS',
        'LT',
        'GT',
        'LE',
        'GE',
        'NE',
        'SEMI',
        'COMMA',
        'LPAREN',
        'RPAREN',
        'MINUS',
        'TIMES',     # 乘号
        'DIVIDE',    # 除号
        'DOT',       # 添加 DOT token
        'PLUS',      # 添加 PLUS token
    }
    
    # 字符规则（支持单引号和双引号）
    STRING = r'(\'[^\']*\'|\"[^\"]*\")'
    
    # 比较运算符（注意顺序：先匹配较长的模式）
    LE = r'<='
    GE = r'>='
    NE = r'!='
    LT = r'<'
    GT = r'>'
    EQUALS = r'='
    
    # 特殊字符
    SEMI = r';'
    COMMA = r','
    LPAREN = r'\('
    RPAREN = r'\)'
    DOT = r'\.'
    
    # 算术运算符
    PLUS = r'\+'
    DIVIDE = r'/'
    
    # 数字（支持负数）
    FLOAT = r'\d*\.\d+'
    INT = r'\d+'
    
    # 标识符
    IDENTIFIER = r'[a-zA-Z_][a-zA-Z0-9_]*'
    
    # 忽略空白字符和换行符
    ignore = ' \t\n\r'
    
    # 忽略注释
    ignore_comment = r'\#.*'
    
    # 关键字映射（不区分大小写）
    keywords = {
        'create': 'CREATE',
        'table': 'TABLE',
        'insert': 'INSERT',
        'into': 'INTO',
        'values': 'VALUES',
        'select': 'SELECT',
        'from': 'FROM',
        'where': 'WHERE',
        'update': 'UPDATE',
        'set': 'SET',
        'delete': 'DELETE',
        'and': 'AND',
        'or': 'OR',
        'char': 'CHAR',
        'int': 'INT_TYPE',
        'float': 'FLOAT_TYPE',
    }
    
    # 修改 STAR 和 TIMES 的定义
    @_(r'\*')
    def STAR(self, t):
        # 如果前面是 SELECT，则是 STAR
        # 否则是 TIMES（乘法运算符）
        if hasattr(self, 'last_token') and self.last_token == 'SELECT':
            return t
        t.type = 'TIMES'
        return t
    
    # 记录上一个 token
    def tokenize(self, text):
        self.last_token = None
        for tok in super().tokenize(text):
            self.last_token = tok.type
            yield tok

    def IDENTIFIER(self, t):
        # 将标识符转换为关键字
        t.type = self.keywords.get(t.value.lower(), 'IDENTIFIER')
        return t

    # 添加减号规则
    MINUS = r'-'

class SQLParser(Parser):
    tokens = SQLLexer.tokens
    
    def __init__(self):
        self.names = {}
        # 确保数据目录存在
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(current_dir, 'data')
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        
    def validate_value_type(self, value, expected_type):
        """验证值的类型是否匹配"""
        if expected_type == DataType.CHAR:
            if not isinstance(value, str) or not value.startswith("'") or not value.endswith("'"):
                raise SQLTypeError(f"类型错误：期望CHAR类型（带引号的字符串），实际为 {value}")
        elif expected_type == DataType.INT:
            try:
                int(value)
                if isinstance(value, str) and ('.' in value):
                    raise SQLTypeError(f"类型错误：期望INT类型，但值 {value} 包含小数点")
            except ValueError:
                raise SQLTypeError(f"类型错误：期望INT类型实际为 {value}")
        elif expected_type == DataType.FLOAT:
            try:
                float_val = float(value)
                if isinstance(value, str):
                    # 检查是否只有一个小数点
                    if value.count('.') != 1:
                        if '.' not in value:
                            raise SQLTypeError(f"类型错误：FLOAT类型必须包含小数点，如 {float_val}")
                        else:
                            raise SQLTypeError(f"类型错误：FLOAT类型只能包含一个小数点")
            except ValueError:
                raise SQLTypeError(f"类型错误：期望FLOAT类型，实际为 {value}")
        else:
            raise SQLTypeError(f"未知的数据类型：{expected_type}")

    def get_table_schema(self, table_name):
        """获取表的结构信息"""
        # 使用实例变量中的数据目录路径
        table_dir = os.path.join(self.data_dir, table_name)
        schema_file = os.path.join(table_dir, 'schema.csv')
        
        if not os.path.exists(table_dir):
            raise SQLError(f"表 {table_name} 不存在")
            
        try:
            with open(schema_file, 'r', encoding='utf-8', newline='') as f:
                schema = []
                reader = csv.DictReader(f)
                for row in reader:
                    schema.append({
                        'name': row['column_name'],
                        'type': DataType.from_string(row['data_type'])
                    })
                if not schema:
                    raise SQLError(f"表 {table_name} 的结构为空")
                return schema
        except FileNotFoundError:
            raise SQLError(f"无法读取表 {table_name} 的结构文件")
        except Exception as e:
            raise SQLError(f"读取表 {table_name} 结构时出错: {str(e)}")

    @_('statement')
    def statements(self, p):
        return p.statement

    @_('statement SEMI')
    def statement(self, p):
        return p.statement

    @_('create_table_stmt',
       'insert_stmt',
       'select_stmt',
       'update_stmt',
       'delete_stmt')
    def statement(self, p):
        return p[0]

    @_('CREATE TABLE IDENTIFIER LPAREN column_defs RPAREN')
    def create_table_stmt(self, p):
        return CreateTableStatement(
            Table(p.IDENTIFIER, p.column_defs)
        )

    @_('column_def')
    def column_defs(self, p):
        return [p.column_def]

    @_('column_def COMMA column_defs')
    def column_defs(self, p):
        return [p.column_def] + p.column_defs

    @_('IDENTIFIER type')
    def column_def(self, p):
        return Column(p.IDENTIFIER, DataType.from_string(p.type))

    @_('CHAR', 'INT_TYPE', 'FLOAT_TYPE')
    def type(self, p):
        return p[0]

    @_('INSERT INTO IDENTIFIER VALUES LPAREN value_list RPAREN')
    def insert_stmt(self, p):
        # 获取表结构
        schema = self.get_table_schema(p.IDENTIFIER)
        values = p.value_list
        
        # 检查值的数量是否匹配
        if len(values) != len(schema):
            raise SQLError(f"列数不匹配：期望 {len(schema)} 列，实际提供 {len(values)} 列")
        
        # 验证每个值的类型
        for i, (value, column) in enumerate(zip(values, schema)):
            try:
                self.validate_value_type(value, column['type'])
            except SQLTypeError as e:
                raise SQLTypeError(f"第 {i+1} 列 '{column['name']}' {str(e)}")
        
        return InsertStatement(p.IDENTIFIER, values)

    @_('value')
    def value_list(self, p):
        return [p.value]

    @_('value COMMA value_list')
    def value_list(self, p):
        return [p.value] + p.value_list

    @_('STRING', 'INT', 'FLOAT', 'MINUS INT', 'MINUS FLOAT', 'qualified_column')
    def value(self, p):
        """解析值（字符串、整数、浮点数、负数或列名）"""
        if len(p) == 2:  # 处理负数
            return f"-{p[1]}"
        if p[0] is None:
            return None
        return p[0]

    @_('SELECT select_cols FROM table_list where_clause')
    def select_stmt(self, p):
        return SelectStatement(p.table_list, p.select_cols, p.where_clause)

    @_('SELECT select_cols FROM table_list')
    def select_stmt(self, p):
        return SelectStatement(p.table_list, p.select_cols, None)

    @_('IDENTIFIER')
    def table_list(self, p):
        return [p.IDENTIFIER]

    @_('IDENTIFIER COMMA table_list')
    def table_list(self, p):
        return [p.IDENTIFIER] + p.table_list

    @_('STAR')
    def select_cols(self, p):
        return [('*', '*')]

    @_('column_list')
    def select_cols(self, p):
        return p.column_list

    @_('qualified_column')
    def column_list(self, p):
        return [p.qualified_column]

    @_('qualified_column COMMA column_list')
    def column_list(self, p):
        return [p.qualified_column] + p.column_list

    @_('IDENTIFIER DOT IDENTIFIER')
    def qualified_column(self, p):
        """解析带表名限定的列名"""
        return (p.IDENTIFIER0, p.IDENTIFIER1)

    @_('IDENTIFIER')
    def qualified_column(self, p):
        """解析不带表名限定的列名"""
        return ('', p.IDENTIFIER)

    @_('WHERE conditions')
    def where_clause(self, p):
        return p.conditions

    @_('condition')
    def conditions(self, p):
        return [p.condition]

    @_('condition AND conditions')
    def conditions(self, p):
        # 设置所有条件的逻辑运算符为AND
        p.condition.logic_op = 'AND'
        for cond in p.conditions:
            cond.logic_op = 'AND'
        return [p.condition] + p.conditions

    @_('condition OR conditions')
    def conditions(self, p):
        # 设置所有条件的逻辑运算符为OR
        p.condition.logic_op = 'OR'
        for cond in p.conditions:
            cond.logic_op = 'OR'
        return [p.condition] + p.conditions

    @_('qualified_column comparison_op value')
    def condition(self, p):
        """解析普通条件表达式"""
        if isinstance(p.qualified_column, tuple):
            table_name, col_name = p.qualified_column
            column = f"{table_name}.{col_name}" if table_name else col_name
        else:
            column = p.qualified_column
        return Condition(column, p.comparison_op, p.value)

    @_('qualified_column EQUALS qualified_column')
    def condition(self, p):
        """解析表连接条件"""
        # 处理左边的列名
        if isinstance(p.qualified_column0, tuple):
            table_name0, col_name0 = p.qualified_column0
            column = f"{table_name0}.{col_name0}"
        else:
            column = p.qualified_column0
        
        # 处理右边的列名
        if isinstance(p.qualified_column1, tuple):
            table_name1, col_name1 = p.qualified_column1
            value = f"{table_name1}.{col_name1}"
        else:
            value = p.qualified_column1
        
        return Condition(column, '=', value)

    @_('qualified_column comparison_op qualified_column')
    def condition(self, p):
        """解析其他比较条件"""
        # 处理左边的列名
        if isinstance(p.qualified_column0, tuple):
            table_name0, col_name0 = p.qualified_column0
            column = f"{table_name0}.{col_name0}"
        else:
            column = p.qualified_column0
        
        # 处理右边的列名
        if isinstance(p.qualified_column1, tuple):
            table_name1, col_name1 = p.qualified_column1
            value = f"{table_name1}.{col_name1}"
        else:
            value = p.qualified_column1
        
        return Condition(column, p.comparison_op, value)

    @_('EQUALS', 'LT', 'GT', 'LE', 'GE', 'NE')
    def comparison_op(self, p):
        """转换比较运算符token为实际的运算符"""
        return p[0]  # 直接返回token的值，不需要映射

    @_('UPDATE IDENTIFIER SET update_list where_clause')
    def update_stmt(self, p):
        return UpdateStatement(p.IDENTIFIER, p.update_list[0], p.update_list[1], p.where_clause)
    
    @_('UPDATE IDENTIFIER SET update_list')
    def update_stmt(self, p):
        return UpdateStatement(p.IDENTIFIER, p.update_list[0], p.update_list[1], None)
    
    @_('IDENTIFIER EQUALS value')
    def update_list(self, p):
        return (p.IDENTIFIER, p.value)
    
    @_('IDENTIFIER EQUALS IDENTIFIER arithmetic_op value')
    def update_list(self, p):
        return (p.IDENTIFIER0, UpdateValue(p.IDENTIFIER1, p.arithmetic_op, p.value))
    
    @_('PLUS')
    def arithmetic_op(self, p):
        return '+'
    
    @_('MINUS')
    def arithmetic_op(self, p):
        return '-'
    
    @_('TIMES')
    def arithmetic_op(self, p):
        return '*'
    
    @_('DIVIDE')
    def arithmetic_op(self, p):
        return '/'

    def error(self, token):
        if token:
            raise SQLSyntaxError(f"语法错误: 在 '{token.value}' 附近")
        else:
            raise SQLSyntaxError("语法错误: 在输入结尾处")

    @_('DELETE FROM IDENTIFIER where_clause')
    def delete_stmt(self, p):
        return DeleteStatement(
            table_name=p.IDENTIFIER,
            conditions=p.where_clause
        )
    
    @_('DELETE FROM IDENTIFIER')
    def delete_stmt(self, p):
        return DeleteStatement(
            table_name=p.IDENTIFIER,
            conditions=[]
        )
