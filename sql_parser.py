from sly import Lexer, Parser
import os
import csv
from typing import List, Tuple, Any, Dict, Optional
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
    table_name: str
    columns: List[str]
    conditions: Optional[List[Condition]] = None

@dataclass
class UpdateStatement(SQLStatement):
    table_name: str
    column: str
    value: Any
    conditions: List[Condition]

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
        'MINUS'
    }
    
    # 字符串规则（支持单引号和双引号）
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
    STAR = r'\*'
    
    # 数字（支持负数）
    FLOAT = r'-?\d*\.\d+'
    INT = r'-?\d+'
    
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
        'char': 'CHAR',
        'int': 'INT_TYPE',
        'float': 'FLOAT_TYPE'
    }
    
    def IDENTIFIER(self, t):
        t.type = self.keywords.get(t.value.lower(), 'IDENTIFIER')
        return t
    
    def error(self, t):
        raise SQLError(f"非法字符 '{t.value[0]}'")

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
                raise SQLTypeError(f"类型错误：期望INT类型，实际为 {value}")
        elif expected_type == DataType.FLOAT:
            try:
                float_val = float(value)
                if isinstance(value, str):
                    # 检查是否只有一个小数点
                    if value.count('.') != 1:
                        if '.' not in value:
                            raise SQLTypeError(f"类型错误：FLOAT类型必须包含小数点，如 {float_val}.0")
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

    @_('STRING', 'INT', 'FLOAT', 'MINUS INT', 'MINUS FLOAT')
    def value(self, p):
        """解析值（字符串、整数或浮点数，包括负数）"""
        if len(p) == 2:  # 处理负数
            return f"-{p[1]}"
        if p[0] is None:
            return None
        return p[0]

    @_('SELECT select_cols FROM IDENTIFIER')
    def select_stmt(self, p):
        return SelectStatement(p.IDENTIFIER, p.select_cols, None)

    @_('SELECT select_cols FROM IDENTIFIER where_clause')
    def select_stmt(self, p):
        return SelectStatement(p.IDENTIFIER, p.select_cols, p.where_clause)

    @_('STAR')
    def select_cols(self, p):
        return ['*']

    @_('column_list')
    def select_cols(self, p):
        return p.column_list

    @_('IDENTIFIER')
    def column_list(self, p):
        return [p.IDENTIFIER]

    @_('IDENTIFIER COMMA column_list')
    def column_list(self, p):
        return [p.IDENTIFIER] + p.column_list

    @_('WHERE condition')
    def where_clause(self, p):
        return [p.condition]

    @_('WHERE condition AND condition')
    def where_clause(self, p):
        return [p.condition0, p.condition1]

    @_('IDENTIFIER comparison_op value')
    def condition(self, p):
        """解析条件表达式"""
        return Condition(p.IDENTIFIER, p.comparison_op, p.value)

    @_('EQUALS', 'LT', 'GT', 'LE', 'GE', 'NE')
    def comparison_op(self, p):
        """转换比较运算符token为实际的运算符"""
        return p[0]  # 直接返回token的值，不需要映射

    @_('UPDATE IDENTIFIER SET IDENTIFIER EQUALS value where_clause')
    def update_stmt(self, p):
        return UpdateStatement(
            table_name=p.IDENTIFIER0,
            column=p.IDENTIFIER1,
            value=p.value,
            conditions=p.where_clause
        )

    @_('DELETE FROM IDENTIFIER where_clause')
    def delete_stmt(self, p):
        return DeleteStatement(
            table_name=p.IDENTIFIER,
            conditions=p.where_clause
        )

    def error(self, token):
        if token:
            raise SQLSyntaxError(f"语法错误: 在 '{token.value}' 附近")
        else:
            raise SQLSyntaxError("语法错误: 在输入结尾处")

    def _parse_create_table(self):
        """解析CREATE TABLE语句"""
        columns = []
        
        # 跳过CREATE TABLE
        self._expect('CREATE')
        self._expect('TABLE')
        
        # 获取表名
        table_name = self._expect('IDENTIFIER').value
        
        # 解析列定义
        self._expect('(')
        
        while True:
            # 获取列名
            column_name = self._expect('IDENTIFIER').value
            
            # 获数据类型
            token = self._advance()
            if token.type == 'CHAR':
                column_type = 'CHAR'
            elif token.type == 'INT_TYPE':
                column_type = 'INT'
            elif token.type == 'FLOAT_TYPE':
                column_type = 'FLOAT'
            else:
                raise SQLError(f"无效的数据类型: {token.type}")
                
            columns.append({'name': column_name, 'type': column_type})
            
            # 检查是否还有更多列
            if self._match(')'):
                break
            self._expect(',')
        
        return CreateTableStatement(table_name, columns)

    def _parse_insert(self):
        """解析INSERT语句"""
        # 跳过INSERT INTO
        self._expect('INSERT')
        self._expect('INTO')
        
        # 获取表名
        table_name = self._expect('IDENTIFIER').value
        
        # 跳过VALUES
        self._expect('VALUES')
        
        # 解析值列表
        self._expect('(')
        values = []
        
        while True:
            if self._match('STRING'):
                values.append(self.current_token.value[1:-1])  # 移除引号
            elif self._match('FLOAT'):
                values.append(float(self.current_token.value))
            elif self._match('INT'):
                values.append(int(self.current_token.value))
            else:
                raise SQLError(f"预期是值，实际是 {self.current_token.type}")
                
            self._advance()
            
            if self._match(')'):
                break
            self._expect(',')
        
        self._advance()  # 跳过右括号
        return InsertStatement(table_name, values)
