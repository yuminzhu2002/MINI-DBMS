# MINI-DBMS
# 创建表
CREATE TABLE Products (productName CHAR,price FLOAT,quantity INT)
CREATE TABLE Students (studentName CHAR,age INT,score FLOAT)

# 插入数据
INSERT INTO Products VALUES ('苹果', 5.5, 10)
INSERT INTO Products VALUES ('香蕉', 3.99, 20)
INSERT INTO Products VALUES ('橙子', 4.5, 15)

INSERT INTO Students VALUES ('张三', 18, 92.5)
INSERT INTO Students VALUES ('李四', 19, 88.5)
INSERT INTO Students VALUES ('王五', 18, 95.5)

# 查询数据
SELECT * FROM Products
SELECT productName,price FROM Products
SELECT * FROM Products WHERE productName = '苹果'
SELECT * FROM Products WHERE productName = '苹果' AND price = 5.5
SELECT * FROM Products WHERE quantity = 10 AND price = 5.5
SELECT * FROM Students WHERE age = 18 AND score = 95.5

# 更新数据
UPDATE Products SET price = 6.5 WHERE productName = '苹果'
UPDATE Students SET score = 95.5 WHERE studentName = '张三'

# 删除数据
DELETE FROM Products WHERE productName = '香蕉'

# 注意：
# - CHAR类型的值必须用引号：'值'
# - INT类型的值必须是整数且不带引号：18
# - FLOAT类型的值必须带小数点且不带引号：92.5
# - WHERE条件支持AND连接两个条件
