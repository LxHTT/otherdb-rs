import other_dbpy

db = other_dbpy.open_db(r"D:\tmp\welcome-to-sled")

# other_dbpy.archive_new(db,"878129128") # 新建这个档案

archive = other_dbpy.archive_open(db,"878129128") # 打开这个档案

# archive.new([
#     ("Hashtable",["lst"]),
#     ("List",["8"]),
#     ("TupleList",["8","1","2"]),
# ]) 新建这个值

my_data = archive.open([ # 打开这个值
    ("Hashtable",["lst"]), # 一级 Hashtable
    ("List",["8"]), # 二级的 List
    ("TupleList",["8","1","2"]), # 三级的元组列表 [index,TupleIndex,TupleLen]
])
# 类似于python的
# my_data['Hashtable'][8][8][1]

print(my_data.access())
print(my_data.overwrite(b"114514-7"))
print(my_data.access())
# print(my_data.delete())
# print(my_data.access())