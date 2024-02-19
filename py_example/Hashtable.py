import time
t1 = time.time()
import other_dbpy
t2 = time.time()
print("导包耗时:",t2-t1)

db = other_dbpy.open_db(r"D:\tmp\welcome-to-sled").clone()

data = other_dbpy.Hashtable(db, "用户名索引")
pwd = other_dbpy.List(db, "密码")

user_name = "xingzhi"
user_pwd = b"aaa"

t1 = time.time()
data.insert(user_name, str(pwd.len()).encode())
pwd.append(user_pwd)
t2 = time.time()
print("写入耗时:",t2-t1)

t1 = time.time()
print("查询结果:",bytes(pwd.access(int(bytes(data.get(user_name)).decode()))) == user_pwd)  # 密码
t2 = time.time()
print("查询耗时",t2-t1)

print("将hash表转化为列表结果:",data.to_tuple_list())

data.delete(user_name)
pwd.delete(pwd.len())

print(data.get('xingzhi'))
