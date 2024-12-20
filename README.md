# SQL命令执行器

这是一个基于Python的SQL命令执行器，支持基本的SQL操作，包括创建表、插入数据、查询数据、更新数据和删除数据。该项目实现了一个简单的数据库管理系统，使用CSV文件存储数据。

## 功能特点

- 支持基本SQL命令：
  - CREATE TABLE
  - INSERT INTO
  - SELECT (支持单表和多表查询)
  - UPDATE
  - DELETE
- 支持数据类型：INT、FLOAT、CHAR
- 支持条件查询(WHERE子句)
- 支持AND/OR逻辑运算
- 支持算术运算(+, -, *, /)
- 支持多表连接查询
- 支持事务管理
- Web界面支持
- 查询结果导出CSV

## 技术实现

- 前端：HTML、CSS、JavaScript
- 后端：Python Flask
- SQL解析：SLY(Python词法分析和语法分析库)
- 数据存储：CSV文件
- 并发控制：使用锁机制
- 事务管理：ACID特性支持

## 项目结构

- server.py：Flask服务器，处理HTTP请求
- sql_parser.py：SQL语句解析器，包含词法分析和语法分析
- sql_executor.py：SQL语句执行器，实现具体的SQL操作
- db_manager.py：数据库管理器，处理事务和并发控制
- templates/a.html：Web界面模板

## 安装和使用

1. 安装依赖：
```bash
pip install flask sly
```

2. 运行服务器：
```bash
python server.py
```

3. 访问Web界面：
```
http://localhost:5000
```

## SQL命令示例

### 1. 创建表
```sql
CREATE TABLE Products (
    productID INT,
    productName CHAR,
    price FLOAT,
    quantity INT
);
```

### 2. 插入数据
```sql
INSERT INTO Products VALUES (1, '苹果', 5.5, 10);
INSERT INTO Products VALUES (2, '香蕉', 3.99, 20);
```

### 3. 查询数据
```sql
-- 基础查询
SELECT * FROM Products;

-- 条件查询
SELECT productName, price FROM Products WHERE price < 5.0;

-- 多表连接查询
SELECT Orders.orderID, Orders.customerName, Products.productName 
FROM Orders, Products 
WHERE Orders.productID = Products.productID;
```

### 4. 更新数据
```sql
-- 基础更新
UPDATE Products SET price = 6.5 WHERE productName = '苹果';

-- 算术运算更新
UPDATE Products SET quantity = quantity + 10 WHERE price < 5.0;
```

### 5. 删除数据
```sql
DELETE FROM Products WHERE price > 10.0;
```

## 注意事项

- CHAR类型的值必须用引号：'value'
- INT类型的值必须是整数且不带引号：18
- FLOAT类型的值必须带小数点且不带引号：92.5
- 表名和列名区分大小写
- 多条SQL语句用分号(;)隔开
- 支持同时执行多条语句

## 错误处理

系统会对SQL语句进行语法检查，并在执行过程中进行类型检查。错误信息会清晰地显示在Web界面上，包括：
- 语法错误
- 类型错误
- 表不存在
- 列不存在
- 数据类型不匹配等

## 数据导出

查询结果可以通过Web界面直接导出为CSV文件。

## 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## 许可证

MIT License
