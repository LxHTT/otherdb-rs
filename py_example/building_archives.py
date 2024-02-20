rets = []  # 构建索引列表
values = []  # 构建索引对应的值的列表


def building(data, route: list):
    if type(data) == list:
        for i in range(len(data)):
            if type(data[i]) == tuple:
                for index, value in enumerate(data[i]):
                    route1 = route.copy()
                    route1.append(("TupleList", [str(i), str(index), len(data[i])]))
                    building(value, route1)
            else:
                route1 = route.copy()
                route1.append(("List", [str(i)]))
                building(data[i], route1)
    elif type(data) == dict:
        for k, v in data.items():
            route1 = route.copy()
            route1.append(("Hashtable", [str(k)]))
            building(v, route1)
    elif type(data) == str:
        rets.append(route)
        values.append(data)


building(
    [
        {"ha": "723988"},
        {"ha1": "??723988"},
    ]
    , [])

print(rets, "\n", values)

# 下面是写入部分

import other_dbpy
import time
db = other_dbpy.open_db(r"D:\tmp\welcome-to-sled")

archives = other_dbpy.archive_new(db, "8732748")

for i in range(len(rets)):
    a = archives.new(rets[i])  # 把所有的内容写入
    a.overwrite(bytes(values[i],encoding='utf-8'))
    print(i)
    time.sleep(0.01)

for i in range(len(rets)):
    a = archives.open(rets[i])
    print(rets[i], a.access() == list(bytes(values[i],encoding='utf-8')))  # 测试读取
    time.sleep(0.01)
