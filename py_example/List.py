import other_dbpy
import time

db = other_dbpy.open_db(r"D:\tmp\welcome-to-sled")

a = other_dbpy.List(db, "114514")  # 创建一个列表

# print(a.delete(0))
print(a.append([1, 1, 4, 5, 1, 4]))
print(a.access(0))
print(a.delete(0))  # 删除index=0的元素

print(a.len())  # 获取列表长度

t1 = time.time()
for i in range(100):  # 遍历追加100个元素
    a.append([1, 1, 4, 5, 1, 4])
t2 = time.time()
print(t2 - t1)

t1 = time.time()
for i in range(a.len()):  # 遍历访问所有元素
    print(a.access(i))
t2 = time.time()
print(t2 - t1)  # 计时

t1 = time.time()
for i in range(a.len()):  # 遍历访问所有元素
    print(a.delete(i))
t2 = time.time()
print(t2 - t1)  # 计时
