import os
import csv
from typing import List, Dict, Any,  Tuple
from sql_parser import (
    SQLError, DataType, 
    CreateTableStatement, InsertStatement, SelectStatement,
    UpdateStatement, DeleteStatement, Condition, UpdateValue
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
                        float_val = value
                        # 如果输入的是整数（如5），转换为浮点数字符串（如"5.0"）
                        if isinstance(value, (int, str)) and '.' not in str(value):
                            return False, f"FLOAT类型必须包含小数点，请使用'{float_val}'"
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
        try:
            if len(stmt.tables) == 1:
                # 单表查询
                table_name = stmt.tables[0]
                
                # 验证表名的大小写
                actual_table_name = None
                for dir_name in os.listdir(self.data_dir):
                    if dir_name == table_name:  # 严格匹配，区分大小写
                        actual_table_name = dir_name
                        break
                
                if actual_table_name is None:
                    raise SQLError(f"表 {table_name} 不存在")
                
                data_file = self.get_table_file(actual_table_name)
                
                # 读取数据
                with open(data_file, 'r', encoding='utf-8', newline='') as f:
                    reader = csv.reader(f)
                    headers = next(reader)
                    rows = list(reader)
                
                # 过滤数据
                filtered_rows = []
                for row in rows:
                    # 构建行字典用于条件检查
                    row_dict = {}
                    for i, col in enumerate(headers):
                        value = row[i]
                        # 移除字符串值的引号
                        if isinstance(value, str) and value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]  # 移除首尾的引号
                        row_dict[col] = value
                    
                    # 如果没有条件，添加所有行
                    if not stmt.conditions:
                        filtered_rows.append(row)
                        continue
                    
                    # 检查条件
                    conditions_met = True  # 对于 AND 条件，初始值为 True
                    last_logic_op = None
                    
                    for condition in stmt.conditions:
                        # 获取列名（去掉表名前缀）
                        if '.' in condition.column:
                            _, col_name = condition.column.split('.')
                        else:
                            col_name = condition.column
                        
                        # 验证列名大小写
                        if col_name not in headers:
                            raise SQLError(f"列名大小写不匹配: {col_name}")
                        
                        col_val = row_dict[col_name]
                        condition_value = condition.value
                        
                        # 去除引号并转换类型
                        if isinstance(col_val, str):
                            col_val = col_val.strip("'")
                        if isinstance(condition_value, str):
                            condition_value = condition_value.strip("'")
                        
                        # 尝试转换为数字
                        try:
                            if col_val.isdigit():
                                col_val = int(col_val)
                            elif '.' in col_val:
                                col_val = float(col_val)
                        
                            if isinstance(condition_value, str):
                                if condition_value.isdigit():
                                    condition_value = int(condition_value)
                                elif '.' in condition_value:
                                    condition_value = float(condition_value)
                        except (ValueError, AttributeError):
                            pass
                        
                        # 计算当前条件的结果
                        current_result = self._compare_values(col_val, condition.operator, condition_value)
                        
                        # 处理逻辑运算符
                        if last_logic_op is None:
                            conditions_met = current_result
                        elif last_logic_op == 'AND':
                            conditions_met = conditions_met and current_result
                        elif last_logic_op == 'OR':
                            conditions_met = conditions_met or current_result
                        
                        last_logic_op = condition.logic_op
                    
                    if conditions_met:
                        filtered_rows.append(row)
                
                # 构建结果
                result = []
                for row in filtered_rows:
                    row_data = []
                    if stmt.columns[0] == ('*', '*'):
                        # 添加所有列
                        for i, header in enumerate(headers):
                            row_data.append((header, row[i]))
                    else:
                        # 添加指定列
                        for table_name, col_name in stmt.columns:
                            if col_name not in headers:
                                raise SQLError(f"列名大小写不匹配: {col_name}")
                            col_index = headers.index(col_name)
                            row_data.append((col_name, row[col_index]))
                    result.append(row_data)
                
                return result
                
            else:
                # 多表连接查询
                tables_data = {}
                tables_headers = {}
                
                # 首先验证表名是否存在（区分大小写）
                for table_name in stmt.tables:
                    actual_table_name = None
                    for dir_name in os.listdir(self.data_dir):
                        if dir_name == table_name:
                            actual_table_name = dir_name
                            break
                    
                    if actual_table_name is None:
                        raise SQLError(f"表 {table_name} 不存在或大小写不匹配")
                    
                    file_path = self.get_table_file(actual_table_name)
                    with open(file_path, 'r', encoding='utf-8', newline='') as f:
                        reader = csv.reader(f)
                        headers = next(reader)
                        tables_headers[actual_table_name] = headers
                        tables_data[actual_table_name] = list(reader)
                    print(f"读取表 {table_name} 的数据:")  # 调试输出
                    print(f"表头: {headers}")
                    print(f"数据: {tables_data[actual_table_name]}")

                # 找到所有连接条件和过滤条件
                join_conditions = []
                filter_conditions = []
                for condition in stmt.conditions:
                    if (isinstance(condition.value, str) and 
                        '.' in condition.value and 
                        any(table in condition.value for table in stmt.tables)):
                        # 这是一个连接条件（值中包含表名）
                        join_conditions.append(condition)
                    else:
                        # 这是一个过滤条件
                        if '.' in condition.column:
                            table_name, col_name = condition.column.split('.')
                            if table_name not in tables_data:
                                raise SQLError(f"表名大小写不匹配: {table_name}")
                            if col_name not in tables_headers[table_name]:
                                raise SQLError(f"列名大小写不匹配: {table_name}.{col_name}")
                        filter_conditions.append(condition)

                print(f"连接条件: {join_conditions}")  # 调试输出
                print(f"过滤条件: {filter_conditions}")  # 调试输出

                if not join_conditions:
                    raise SQLError("未找到有效的连接条件")

                # 执行连接
                # 从第一个表开始，逐步与其他表连接
                result_rows = []
                first_table = stmt.tables[0]
                
                # 初始化结果集
                for row in tables_data[first_table]:
                    # 处理带引号的值
                    processed_row = []
                    for value in row:
                        if isinstance(value, str) and value.startswith("'") and value.endswith("'"):
                            processed_row.append(value[1:-1])  # 移除引号
                        else:
                            # 尝试转换数值
                            try:
                                if isinstance(value, str):
                                    if '.' in value:
                                        processed_row.append(float(value))
                                    elif value.isdigit():
                                        processed_row.append(int(value))
                                    else:
                                        processed_row.append(value)
                                else:
                                    processed_row.append(value)
                            except ValueError:
                                processed_row.append(value)
                    
                    row_dict = {f"{first_table}.{header}": value 
                               for header, value in zip(tables_headers[first_table], processed_row)}
                    result_rows.append(row_dict)

                # 与其他表逐个连接
                for i in range(1, len(stmt.tables)):
                    current_table = stmt.tables[i]
                    new_result_rows = []

                    # 找到与当前表相关的连接条件
                    current_join_conditions = []
                    for cond in join_conditions:
                        # 检查条件是否涉及当前表
                        if (current_table in cond.column or 
                            (isinstance(cond.value, str) and current_table in cond.value)):
                            # 确这是一个连接条件（两个表之间的等值条件）
                            if ('.' in cond.column and 
                                isinstance(cond.value, str) and 
                                '.' in cond.value and 
                                cond.operator == '='):  # 只处理等值连接
                                current_join_conditions.append(cond)

                    # 对每个现有的结果行，尝试与当前表的行连接
                    for result_row in result_rows:
                        for current_row in tables_data[current_table]:
                            # 处理带引号的值
                            processed_row = []
                            for value in current_row:
                                if isinstance(value, str) and value.startswith("'") and value.endswith("'"):
                                    processed_row.append(value[1:-1])
                                else:
                                    try:
                                        if isinstance(value, str):
                                            if '.' in value:
                                                processed_row.append(float(value))
                                            elif value.isdigit():
                                                processed_row.append(int(value))
                                            else:
                                                processed_row.append(value)
                                        else:
                                            processed_row.append(value)
                                    except ValueError:
                                        processed_row.append(value)
                            
                            current_dict = {f"{current_table}.{header}": value 
                                           for header, value in zip(tables_headers[current_table], processed_row)}
                            
                            # 检查连接条件
                            conditions_met = True
                            for join_condition in current_join_conditions:
                                val1 = None
                                val2 = None
                                
                                # 获取连接条件的值
                                if current_table in join_condition.column:
                                    val1 = current_dict.get(join_condition.column)
                                    val2 = result_row.get(join_condition.value)
                                else:
                                    val1 = result_row.get(join_condition.column)
                                    val2 = current_dict.get(join_condition.value)
                                
                                # 确保两个值都是相同类型
                                if isinstance(val1, str):
                                    val1 = val1.strip("'")
                                if isinstance(val2, str):
                                    val2 = val2.strip("'")
                                
                                # 如果是数字，转换为同类型
                                try:
                                    if str(val1).isdigit() and str(val2).isdigit():
                                        val1 = int(val1)
                                        val2 = int(val2)
                                except (ValueError, AttributeError):
                                    pass
                                
                                print(f"连接条件比较: {join_condition.column} = {join_condition.value}")
                                print(f"值比较: {val1} ({type(val1)}) == {val2} ({type(val2)})")
                                
                                if val1 != val2:
                                    conditions_met = False
                                    break
                            
                            if conditions_met:
                                new_row = result_row.copy()
                                new_row.update(current_dict)
                                new_result_rows.append(new_row)
                                print(f"添加新行: {new_row}")  # 调试输出

                    result_rows = new_result_rows

                # 在过滤条件处理之前
                print(f"过滤前的行数: {len(result_rows)}")  # 调试输出
                for row in result_rows:
                    print(f"过滤前的行: {row}")

                # 应用过滤条件
                if filter_conditions:
                    filtered_rows = []
                    for row in result_rows:
                        conditions_met = True
                        for condition in filter_conditions:
                            val1 = row.get(condition.column)
                            val2 = condition.value
                            
                            # 处理值
                            if isinstance(val1, str):
                                if val1.startswith("'") and val1.endswith("'"):
                                    val1 = val1[1:-1]
                                # 尝试转换为数字
                                try:
                                    if '.' in val1:
                                        val1 = float(val1)
                                    elif val1.isdigit():
                                        val1 = int(val1)
                                except ValueError:
                                    pass

                            # 处理条件值
                            try:
                                if isinstance(val2, str):
                                    if val2.startswith("'") and val2.endswith("'"):
                                        val2 = val2[1:-1]
                                    if '.' in str(val2):
                                        val2 = float(val2)
                                    elif str(val2).isdigit():
                                        val2 = int(val2)
                                elif isinstance(val2, (int, float)):
                                    # 如果是数值类型，直接使用
                                    val2 = float(val2)
                            except ValueError:
                                pass

                            print(f"过滤条件: {condition.column} {condition.operator} {condition.value}")
                            print(f"比较值: {val1} ({type(val1)}) {condition.operator} {val2} ({type(val2)})")
                            
                            # 确保数值比较时类型一致
                            if isinstance(val1, (int, float)) or isinstance(val2, (int, float)):
                                try:
                                    val1 = float(val1)
                                    val2 = float(val2)
                                except (ValueError, TypeError):
                                    pass
                            
                            compare_result = self._compare_values(val1, condition.operator, val2)
                            print(f"比较结果: {compare_result}")
                            
                            if not compare_result:
                                conditions_met = False
                                break
                        
                        if conditions_met:
                            filtered_rows.append(row)
                            print(f"添加过滤后的行: {row}")
                    
                    result_rows = filtered_rows

                print(f"最终结果行数: {len(result_rows)}")  # 调试输出

                # 构建最终结果
                result = []
                for row in result_rows:
                    row_data = []
                    for table_name, col_name in stmt.columns:
                        # 验证表名的大小写
                        if table_name not in tables_data:
                            raise SQLError(f"表名大小写不匹配: {table_name}")
                        
                        # 验证列名的大小写
                        if col_name not in tables_headers[table_name]:
                            raise SQLError(f"列名大小写不匹配: {table_name}.{col_name}")
                        
                        val = row.get(f"{table_name}.{col_name}")
                        if isinstance(val, str):
                            val = val.strip("'")
                        row_data.append((col_name, val))
                    result.append(row_data)

                return result

        except Exception as e:
            if str(e):
                raise SQLError(f"查询数据时出错: {str(e)}")
            else:
                raise SQLError("查询数据时出错: 未知错误")

    def _perform_join(self, tables_data: Dict[str, List[Dict]], conditions: List[Condition]) -> List[Dict]:
        """执行连接操作"""
        if not tables_data:
            return []
        
        # 从第一个表开始
        first_table = list(tables_data.keys())[0]
        result = [{f"{first_table}.{k}": v for k, v in row.items()} 
                  for row in tables_data[first_table]]
        
        # 依次与其他表连接
        for table_name in list(tables_data.keys())[1:]:
            new_result = []
            for row1 in result:
                for row2 in tables_data[table_name]:
                    # 创建新的连接行
                    joined_row = row1.copy()
                    joined_row.update({f"{table_name}.{k}": v for k, v in row2.items()})
                    
                    # 检查所有条件
                    all_conditions_met = True
                    for condition in conditions:
                        # 只检查涉及到前已连接表的条件
                        current_tables = set(key.split('.')[0] for key in joined_row.keys())
                        
                        if isinstance(condition.value, tuple):
                            # 连接条件
                            table1 = condition.column.split('.')[0]
                            table2 = condition.value[0]
                            if table1 in current_tables and table2 in current_tables:
                                val1 = joined_row.get(condition.column)
                                val2 = joined_row.get(f"{condition.value[0]}.{condition.value[1]}")
                                if isinstance(val1, str):
                                    val1 = val1.strip("'")
                                if isinstance(val2, str):
                                    val2 = val2.strip("'")
                                if val1 != val2:
                                    all_conditions_met = False
                                    break
                        else:
                            # 过滤条件
                            table = condition.column.split('.')[0]
                            if table in current_tables:
                                val = joined_row.get(condition.column)
                                if isinstance(val, str):
                                    val = val.strip("'")
                                if isinstance(condition.value, str):
                                    if condition.value.startswith("'"):
                                        condition_value = condition.value.strip("'")
                                    elif '.' in condition.value:
                                        condition_value = float(condition.value)
                                    else:
                                        condition_value = int(condition.value)
                                else:
                                    condition_value = condition.value
                                
                                if not self._compare_values(val, condition.operator, condition_value):
                                    all_conditions_met = False
                                    break
                    
                    if all_conditions_met:
                        new_result.append(joined_row)
            result = new_result
        
        return result

    def _compare_values(self, val1, operator: str, val2) -> bool:
        """比较两个值"""
        try:
            # 确保数值比较时类型一致
            if isinstance(val1, (int, float)) or isinstance(val2, (int, float)):
                try:
                    # 统一转换为浮点数进行比较
                    val1 = float(str(val1))  # 使用字符串转换避免精度问题
                    val2 = float(str(val2))
                    
                    # 对于浮点数比较，使用近似相等
                    if operator == '=':
                        return abs(val1 - val2) < 1e-10
                    elif operator == '>':
                        return val1 > val2
                    elif operator == '<':
                        return val1 < val2
                    elif operator == '>=':
                        return val1 >= val2 or abs(val1 - val2) < 1e-10
                    elif operator == '<=':
                        return val1 <= val2 or abs(val1 - val2) < 1e-10
                    elif operator == '<>':
                        return abs(val1 - val2) >= 1e-10
                except (ValueError, TypeError):
                    return False

            # 非数值比较
            if operator == '=':
                return val1 == val2
            elif operator == '>':
                return val1 > val2
            elif operator == '<':
                return val1 < val2
            elif operator == '>=':
                return val1 >= val2
            elif operator == '<=':
                return val1 <= val2
            elif operator == '<>':
                return val1 != val2
            else:
                raise SQLError(f"不支持的操作符: {operator}")
        except TypeError:
            # 如果比较失败，返回False
            return False

    def _execute_update(self, stmt: UpdateStatement) -> str:
        try:
            table_name = stmt.table_name
            
            # 验证表名的大小写
            actual_table_name = None
            for dir_name in os.listdir(self.data_dir):
                if dir_name == table_name:  # 严格匹配，区分大小写
                    actual_table_name = dir_name
                    break
            
            if actual_table_name is None:
                raise SQLError(f"表 {table_name} 不存在")
            
            # 获取文件路径
            schema_file = self.get_schema_file(actual_table_name)
            data_file = self.get_table_file(actual_table_name)
            
            # 读取表结构
            schema = []
            with open(schema_file, 'r', encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    schema.append({
                        'name': row['column_name'],
                        'type': DataType[row['data_type']]
                    })
            
            # 验证要更新的列是否存在
            col_schema = next((col for col in schema if col['name'] == stmt.column), None)
            if not col_schema:
                raise SQLError(f"列 {stmt.column} 不存在")
            
            # 如果是算术表达式，验证操作数列是否存在
            if isinstance(stmt.value, UpdateValue) and stmt.value.column:
                update_col_schema = next((col for col in schema if col['name'] == stmt.value.column), None)
                if not update_col_schema:
                    raise SQLError(f"列 {stmt.value.column} 不存在")
                # 验证操作数的类型
                if update_col_schema['type'] not in (DataType.INT, DataType.FLOAT):
                    raise SQLError(f"列 {stmt.value.column} 不是数值类型")
            
            # 读取数据
            with open(data_file, 'r', encoding='utf-8', newline='') as f:
                reader = csv.reader(f)
                headers = next(reader)
                rows = list(reader)
            
            # 找到要更新的列引
            try:
                col_index = headers.index(stmt.column)
            except ValueError:
                raise SQLError(f"列 {stmt.column} 不存在")
            
            # 更新数据
            update_count = 0
            updated_rows = []  # 存储更新的行信息
            
            for row in rows:
                # 构建行字典用于件查
                row_dict = {headers[i]: val for i, val in enumerate(row)}
                
                # 检查条件
                conditions_met = True
                if stmt.conditions:
                    for condition in stmt.conditions:
                        # 获取列名（去掉表名前缀）
                        if '.' in condition.column:
                            _, col_name = condition.column.split('.')
                        else:
                            col_name = condition.column
                        
                        # 验证列名大小写
                        if col_name not in headers:
                            raise SQLError(f"列名大小写不匹配: {col_name}")
                        
                        val = row_dict[col_name]
                        condition_value = condition.value
                        
                        # 去除引号并转换类型
                        if isinstance(val, str):
                            val = val.strip("'")
                        if isinstance(condition_value, str):
                            condition_value = condition_value.strip("'")
                        
                        # 尝试转换为数字
                        try:
                            if isinstance(val, str):
                                if val.isdigit():
                                    val = int(val)
                                elif '.' in val:
                                    val = float(val)
                            if isinstance(condition_value, str):
                                if condition_value.isdigit():
                                    condition_value = int(condition_value)
                                elif '.' in condition_value:
                                    condition_value = float(condition_value)
                        except (ValueError, AttributeError):
                            pass
                        
                        if not self._compare_values(val, condition.operator, condition_value):
                            conditions_met = False
                            break
                
                # 如果满足条件更新值
                if conditions_met:
                    old_value = row[col_index]
                    
                    if isinstance(stmt.value, UpdateValue):
                        # 获取当前列的值
                        current_val = row_dict[stmt.value.column]
                        if isinstance(current_val, str):
                            current_val = current_val.strip("'")
                        
                        # 转为数字
                        if '.' in current_val:
                            current_val = float(current_val)
                        else:
                            current_val = int(current_val)
                        
                        # 执行算术运算
                        update_val = float(stmt.value.value)
                        if stmt.value.operator == '+':
                            result = current_val + update_val
                        elif stmt.value.operator == '-':
                            result = current_val - update_val
                        elif stmt.value.operator == '*':
                            result = current_val * update_val
                        elif stmt.value.operator == '/':
                            result = current_val / update_val
                        
                        # 格式化结果
                        if col_schema['type'] == DataType.INT:
                            formatted_value = str(int(result))
                        else:
                            formatted_value = str(float(result))
                    else:
                        # 处理普通值
                        formatted_value = str(stmt.value)
                    
                    # 只有当新值与旧值不同时才更新
                    if formatted_value != old_value:
                        # 保存更新信息
                        updated_rows.append({
                            'row': dict(zip(headers, row)),
                            'column': stmt.column,
                            'old_value': old_value,
                            'new_value': formatted_value
                        })
                        
                        row[col_index] = formatted_value
                        update_count += 1
            
            # 写回文件
            with open(data_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(rows)
            
            # 构建更新结果消息
            result_msg = f"更新了 {update_count} 行数据\n"
            if updated_rows:
                result_msg += "\n更新详情:\n"
                result_msg += "=" * 50 + "\n"  # 添加分隔线
                
                for i, update_info in enumerate(updated_rows, 1):
                    # 格式化每个字段
                    formatted_fields = []
                    for k, v in update_info['row'].items():
                        if isinstance(v, str) and v.startswith("'") and v.endswith("'"):
                            v = v[1:-1]  # 移除引号
                        formatted_fields.append(f"{k}: {v}")
                    
                    row_str = "  " + "\n  ".join(formatted_fields)  # 每个字段一行，添加缩进
                    
                    result_msg += f"行 {i}:\n{row_str}\n"
                    result_msg += f"更新: {update_info['column']} = {update_info['old_value']} -> {update_info['new_value']}\n"
                    
                    if i < len(updated_rows):
                        result_msg += "-" * 50 + "\n"  # 在行之间添加分隔线
                
                result_msg += "=" * 50  # 添加结束分隔线
            
            return result_msg
            
        except Exception as e:
            raise SQLError(f"更新数据时出错: {str(e)}")

    def _execute_delete(self, stmt: DeleteStatement) -> str:
        """执行DELETE语句"""
        try:
            table_name = stmt.table_name
            
            # 验证表名的大小写
            actual_table_name = None
            for dir_name in os.listdir(self.data_dir):
                if dir_name == table_name:  # 严格匹配，区分大小写
                    actual_table_name = dir_name
                    break
            
            if actual_table_name is None:
                raise SQLError(f"表 {table_name} 不存在")
            
            # 获取文件路径
            data_file = self.get_table_file(actual_table_name)
            
            # 读取数据
            with open(data_file, 'r', encoding='utf-8', newline='') as f:
                reader = csv.reader(f)
                headers = next(reader)
                rows = list(reader)
            
            # 存储删除的行信息
            deleted_rows = []
            remaining_rows = []
            
            # 处理每一行
            for row in rows:
                # 构建行字典用于条件检查
                row_dict = {headers[i]: val for i, val in enumerate(row)}
                
                # 检查条件
                conditions_met = True
                if stmt.conditions:
                    for condition in stmt.conditions:
                        # 获取列名（去掉表名前缀）
                        if '.' in condition.column:
                            _, col_name = condition.column.split('.')
                        else:
                            col_name = condition.column
                        
                        # 验证列名大小写
                        if col_name not in headers:
                            raise SQLError(f"列名大小写不匹配: {col_name}")
                        
                        val = row_dict[col_name]
                        condition_value = condition.value
                        
                        # 去除引号并转换类型
                        if isinstance(val, str):
                            val = val.strip("'")
                        if isinstance(condition_value, str):
                            condition_value = condition_value.strip("'")
                        
                        # 尝试转换为数字
                        try:
                            if isinstance(val, str):
                                if val.isdigit():
                                    val = int(val)
                                elif '.' in val:
                                    val = float(val)
                            if isinstance(condition_value, str):
                                if condition_value.isdigit():
                                    condition_value = int(condition_value)
                                elif '.' in condition_value:
                                    condition_value = float(condition_value)
                        except (ValueError, AttributeError):
                            pass
                        
                        if not self._compare_values(val, condition.operator, condition_value):
                            conditions_met = False
                            break
                
                # 如果满足条件，添加到删除列表；否则保留
                if conditions_met:
                    deleted_rows.append(row_dict)
                else:
                    remaining_rows.append(row)
            
            # 写回文件
            with open(data_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(remaining_rows)
            
            # 构建删除结果消息
            result_msg = f"删除了 {len(deleted_rows)} 行数据\n"
            if deleted_rows:
                result_msg += "\n删除的行:\n"
                result_msg += "=" * 50 + "\n"  # 添加分隔线
                
                for i, row_dict in enumerate(deleted_rows, 1):
                    # 格式化每个字段
                    formatted_fields = []
                    for k, v in row_dict.items():
                        if isinstance(v, str) and v.startswith("'") and v.endswith("'"):
                            v = v[1:-1]  # 移除引号
                        formatted_fields.append(f"{k}: {v}")
                    
                    row_str = "  " + "\n  ".join(formatted_fields)  # 每个字段一行，添加缩进
                    result_msg += f"行 {i}:\n{row_str}\n"
                    
                    if i < len(deleted_rows):
                        result_msg += "-" * 50 + "\n"  # 在行之间添加分隔线
                
                result_msg += "=" * 50  # 添加结束分隔线
            
            return result_msg
            
        except Exception as e:
            raise SQLError(f"删除数据时出错: {str(e)}")
