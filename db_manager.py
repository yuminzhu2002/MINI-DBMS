import os
import csv
import shutil
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from threading import Lock
from sql_parser import SQLError, SQLTypeError, DataType, Column, Table

class DBError(SQLError):
    """数据库操作错误"""
    pass

@dataclass
class TableOperation:
    """表操作记录"""
    operation_type: str  # 'create', 'modify', 'delete'
    table_name: str
    backup_path: Optional[str] = None
    is_new: bool = False

class LockManager:
    """锁管理器"""
    def __init__(self):
        self._locks: Dict[str, Lock] = {}
        self._global_lock = Lock()
        
    def acquire_lock(self, table_name: str) -> Lock:
        """获取表锁"""
        with self._global_lock:
            if table_name not in self._locks:
                self._locks[table_name] = Lock()
            return self._locks[table_name]
            
    def release_lock(self, table_name: str):
        """释放表锁"""
        if table_name in self._locks:
            self._locks[table_name].release()

class Transaction:
    """事务管理"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.operations: List[TableOperation] = []
        self.active = True
        
    def add_operation(self, operation: TableOperation):
        """添加操作到事务"""
        if not self.active:
            raise DBError("事务已结束")
        self.operations.append(operation)
        
    def commit(self):
        """提交事务"""
        if not self.active:
            raise DBError("事务已结束")
            
        try:
            # 清理所有备份
            for op in self.operations:
                if op.backup_path and os.path.exists(op.backup_path):
                    os.remove(op.backup_path)
        finally:
            self.active = False
            
    def rollback(self):
        """回滚事务"""
        if not self.active:
            raise DBError("事务已结束")
            
        try:
            # 反向处理操作
            for op in reversed(self.operations):
                if op.operation_type == 'create' and op.is_new:
                    # 删除新创建的表
                    table_path = os.path.join(self.db_path, op.table_name)
                    if os.path.exists(table_path):
                        shutil.rmtree(table_path)
                elif op.operation_type in ('modify', 'delete') and op.backup_path:
                    # 恢复备份
                    table_path = os.path.join(self.db_path, op.table_name)
                    if os.path.exists(op.backup_path):
                        shutil.copy2(op.backup_path, table_path)
                        os.remove(op.backup_path)
        finally:
            self.active = False

class DBManager:
    """数据库管理器"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock_manager = LockManager()
        self.current_transaction: Optional[Transaction] = None
        
        # 确保数据目录存在
        if not os.path.exists(db_path):
            os.makedirs(db_path)
            
    def begin_transaction(self):
        """开始事务"""
        if self.current_transaction:
            raise DBError("已有活动事务")
        self.current_transaction = Transaction(self.db_path)
        
    def commit_transaction(self):
        """提交事务"""
        if not self.current_transaction:
            raise DBError("没有活动事务")
        self.current_transaction.commit()
        self.current_transaction = None
        
    def rollback_transaction(self):
        """回滚事务"""
        if not self.current_transaction:
            raise DBError("���有活动事务")
        self.current_transaction.rollback()
        self.current_transaction = None
        
    def create_table(self, table: Table) -> str:
        """创建表"""
        if not self.current_transaction:
            raise DBError("没有活动事务")
            
        table_path = os.path.join(self.db_path, table.name)
        if os.path.exists(table_path):
            raise DBError(f"表 {table.name} 已存在")
            
        try:
            # 创建表目录
            os.makedirs(table_path)
            
            # 记录操作
            self.current_transaction.add_operation(
                TableOperation('create', table.name, is_new=True)
            )
            
            # 创建schema文件
            schema_file = os.path.join(table_path, f'{table.name}_schema.csv')
            with open(schema_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['column_name', 'data_type'])
                for col in table.columns:
                    writer.writerow([col.name, col.data_type.name])
                    
            # 创建数据文件
            data_file = os.path.join(table_path, f'{table.name}_data.csv')
            with open(data_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([col.name for col in table.columns])
                
            return f"表 {table.name} 创建成功"
            
        except Exception as e:
            # 清理已创建的文件
            if os.path.exists(table_path):
                shutil.rmtree(table_path)
            raise DBError(f"创建表失败: {str(e)}")
            
    def _backup_table(self, table_name: str) -> str:
        """创建表的备份"""
        table_path = os.path.join(self.db_path, table_name)
        backup_path = f"{table_path}.bak"
        shutil.copy2(table_path, backup_path)
        return backup_path
        
    def _get_schema(self, table_name: str) -> Dict[str, DataType]:
        """获取表结构"""
        schema_file = os.path.join(self.db_path, table_name, f'{table_name}_schema.csv')
        if not os.path.exists(schema_file):
            raise DBError(f"表 {table_name} 不存在")
            
        with open(schema_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return {row['column_name']: DataType[row['data_type']] for row in reader}
            
    def _validate_value(self, value: Any, expected_type: DataType, column_name: str) -> Any:
        """验证并转换值的类型"""
        try:
            if expected_type == DataType.INT:
                if isinstance(value, str) and value.isdigit():
                    return int(value)
                if isinstance(value, int):
                    return value
                raise SQLTypeError(f"列 {column_name} 需要整数类型")
                
            elif expected_type == DataType.FLOAT:
                if isinstance(value, (str, int, float)):
                    return float(value)
                raise SQLTypeError(f"列 {column_name} 需要浮点数类型")
                
            elif expected_type == DataType.CHAR:
                return str(value)
                
        except ValueError:
            raise SQLTypeError(f"无法将值 '{value}' 转换为 {expected_type.name} 类型")
            
        return value 
