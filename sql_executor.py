import os
import csv
from typing import List, Dict, Any, Optional, Tuple
from sql_parser import (
    SQLError, SQLTypeError, DataType, Column, Table,
    CreateTableStatement, InsertStatement, SelectStatement,
    UpdateStatement, DeleteStatement, Condition
)

class SQLExecutor:
    """SQL执行器"""
    def __init__(self, data_dir=None):
        # 如果没有提供数据目录，使用默认路径
        if data_dir is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            self.data_dir = os.path.join(current_dir, 'data')
        else:
            self.data_dir = data_dir
            
        # 确保数据目录存在
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            
    def get_table_dir(self, table_name: str) -> str:
        """获取表的目录路径"""
        return os.path.join(self.data_dir, table_name)
        
    def get_table_file(self, table_name: str) -> str:
        """获取表数据文件的路径"""
        return os.path.join(self.get_table_dir(table_name), 'data.csv')
        
    def get_schema_file(self, table_name: str) -> str:
        """获取表结构文件的路径"""
        return os.path.join(self.get_table_dir(table_name), 'schema.csv')
        
    def execute(self, statements: List[Any]) -> List[Dict[str, Any]]:
        """执行SQL语句"""
        results = []
        
        for stmt in statements:
            if isinstance(stmt, CreateTableStatement):
                result = self._execute_create_table(stmt)
            elif isinstance(stmt, InsertStatement):
                result = self._execute_insert(stmt)
            elif isinstance(stmt, SelectStatement):
                result = self._execute_select(stmt)
            elif isinstance(stmt, UpdateStatement):
                result = self._execute_update(stmt)
            elif isinstance(stmt, DeleteStatement):
                result = self._execute_delete(stmt)
            else:
                raise SQLError(f"不支持的SQL语句类型: {type(stmt)}")
                
            results.append({
                'success': True,
                'result': result
            })
            
        return results
        
    def _execute_create_table(self, stmt: CreateTableStatement) -> str:
        """执行CREATE TABLE语句"""
        table_name = stmt.table.name
        table_dir = self.get_table_dir(table_name)
        schema_file = self.get_schema_file(table_name)
        data_file = self.get_table_file(table_name)
        
        # 检查表是否已存在
        if os.path.exists(table_dir):
            raise SQLError(f"表 {table_name} 已存在")
            
        try:
            # 创建表目录
            os.makedirs(table_dir)
            
            # 写入表结构
            with open(schema_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                # 写入列名行
                writer.writerow(['column_name', 'data_type'])
                # 写入列定义
                for col in stmt.table.columns:
                    writer.writerow([col.name, col.data_type.name])
                    
            # 创建数据文件，写入列名
            with open(data_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([col.name for col in stmt.table.columns])
                
            return f"表 {table_name} 创建成功"
            
        except Exception as e:
            # 如果创建过程中出错，清理已创建的目录
            if os.path.exists(table_dir):
                import shutil
                shutil.rmtree(table_dir)
            raise SQLError(f"创建表时出错: {str(e)}")
            
    def _execute_insert(self, stmt: InsertStatement) -> str:
        """执行INSERT语句"""
        table_name = stmt.table_name
        table_dir = self.get_table_dir(table_name)
        schema_file = self.get_schema_file(table_name)
        data_file = self.get_table_file(table_name)
        
        # 检查表是否存在
        if not os.path.exists(table_dir):
            raise SQLError(f"表 {table_name} 不存在")
            
        try:
            # 读取表结构
            schema = []
            with open(schema_file, 'r', encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    schema.append({
                        'name': row['column_name'],
                        'type': DataType[row['data_type']]
                    })
                        
            # 检查值的数量是否匹配
            if len(stmt.values) != len(schema):
                raise SQLError(f"列数不匹配：期望 {len(schema)} 列，实际提供 {len(stmt.values)} 列")
                
            # 验证每个值的类型
            for i, (value, column) in enumerate(zip(stmt.values, schema)):
                is_valid, error_msg = self.validate_data_type(value, column['type'].name)
                if not is_valid:
                    raise SQLError(f"第 {i+1} 列 '{column['name']}' {error_msg}")
                    
            # 写入数据
            with open(data_file, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(stmt.values)
                
            return "插入成功"
            
        except Exception as e:
            raise SQLError(f"插入数据时出错: {str(e)}")

    def validate_data_type(self, value, column_type):
        """验证数据类型是否匹配"""
        try:
            if column_type == 'INT':
                # 确保是整数
                if isinstance(value, str):
                    if '.' in value:
                        return False, "INT类型不能包含小数点"
                if not isinstance(value, (int, str)):
                    return False, f"类型错误：期望INT类型，实际为{type(value)}"
                try:
                    int(value)
                    return True, None
                except ValueError:
                    return False, f"无法将'{value}'转换为INT类型"
                    
            elif column_type == 'FLOAT':
                # 确保是浮点数
                if isinstance(value, (int, str)):
                    try:
                        float_val = float(value)
                        # 如果输入的是整数（如5），转换为浮点数字符串（如"5.0"）
                        if isinstance(value, (int, str)) and '.' not in str(value):
                            return False, f"FLOAT类型必须包含小数点，请使用'{float_val}.0'"
                        return True, None
                    except ValueError:
                        return False, f"无法将'{value}'转换为FLOAT类型"
                return False, f"类型错误：期望FLOAT类型，实际为{type(value)}"
                
            elif column_type == 'CHAR':
                # 字符串类型
                if not isinstance(value, str):
                    return False, f"类型错误：期望CHAR类型，实际为{type(value)}"
                return True, None
                
            return False, f"未知的数据类型：{column_type}"
        except Exception as e:
            return False, f"类型验证错误：{str(e)}"

    def _execute_select(self, stmt: SelectStatement) -> List[List[Tuple[str, str]]]:
        """执行SELECT语句"""
        table_name = stmt.table_name
        table_dir = self.get_table_dir(table_name)
        schema_file = self.get_schema_file(table_name)
        data_file = self.get_table_file(table_name)
        
        # 检查表是否存在
        if not os.path.exists(table_dir):
            raise SQLError(f"表 {table_name} 不存在")
        
        try:
            # 读取表结构
            schema = []
            with open(schema_file, 'r', encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    schema.append({
                        'name': row['column_name'],
                        'type': DataType[row['data_type']]
                    })
                
            # 确定要查询的列
            if stmt.columns == ['*']:
                columns = [col['name'] for col in schema]
            else:
                # 验证列是否存在
                schema_cols = [col['name'] for col in schema]
                for col in stmt.columns:
                    if col not in schema_cols:
                        raise SQLError(f"列 {col} 不存在")
                columns = stmt.columns
                
            # 读取数据
            result = []
            with open(data_file, 'r', encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # 检查WHERE条件
                    if stmt.conditions and not self._check_conditions(row, stmt.conditions, schema):
                        continue
                        
                    # 添加符合条件的行
                    result_row = []
                    for col in columns:
                        result_row.append([col, row[col]])
                    result.append(result_row)
                    
            return result
            
        except Exception as e:
            raise SQLError(f"查询数据时出错: {str(e)}")

    def _check_conditions(self, row: Dict[str, str], conditions: List[Condition], schema: List[Dict[str, Any]]) -> bool:
        """检查行是否满足WHERE条件"""
        for condition in conditions:
            # 获取列的类型
            col_type = None
            for col in schema:
                if col['name'] == condition.column:
                    col_type = col['type']
                    break
            if col_type is None:
                raise SQLError(f"列 {condition.column} 不存在")
                
            # 获取行中的值和条件值
            row_value = row[condition.column]
            cond_value = condition.value
            
            # 根据类型转换值
            try:
                if col_type == DataType.INT:
                    row_value = int(row_value)
                    cond_value = int(cond_value)
                elif col_type == DataType.FLOAT:
                    row_value = float(row_value)
                    cond_value = float(cond_value)
                # CHAR类型不需要转换
            except ValueError:
                raise SQLError(f"值类型转换错误：列 {condition.column}")
                
            # 比较值
            if not self._compare_values(row_value, condition.operator, cond_value):
                return False
                
        return True

    def _compare_values(self, val1: Any, op: str, val2: Any) -> bool:
        """比较两个值"""
        if op == '=':
            return val1 == val2
        elif op == '<':
            return val1 < val2
        elif op == '>':
            return val1 > val2
        elif op == '<=':
            return val1 <= val2
        elif op == '>=':
            return val1 >= val2
        else:
            raise SQLError(f"不支持的比较运算符: {op}")
