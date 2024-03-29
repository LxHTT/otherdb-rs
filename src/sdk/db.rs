fn ivec_to_string(v:sled::IVec) -> String{
    String::from_utf8(v.to_vec()).expect("code error")
}

pub mod kv_operation {
    use sled::{Db, IVec};

    pub fn initialization(path:String) -> Db{
        // 创建一个储存引擎对象
        sled::open(path).expect("Failed to open database")
    }

    #[derive(Clone)]
    #[derive(Debug)]
    pub struct KvDbOpera {
        // 键值对操作对象
        db : Db
    }

    pub type KvDbOperaObject = KvDbOpera; // 创建类型(方便读)

    pub trait KvDbOperaTrait {
        type Output;
        fn new(db: Db) -> <Self as KvDbOperaTrait>::Output;
        fn insert<T: AsRef<[u8]>,V:Into<IVec>>(&self, key: T, value: V) -> Result<bool, String>;
        fn get<T: AsRef<[u8]>>(&self, key: T) -> Result<Option<IVec>, String>;
        fn delete<T: AsRef<[u8]>>(&self, key: T) -> Result<bool, String>;
    }

    impl KvDbOperaTrait for KvDbOpera {
        type Output = Self;
        fn new(db: Db) -> <KvDbOpera as KvDbOperaTrait>::Output {
            KvDbOpera { db }
        }
        fn insert<T: AsRef<[u8]>,V:Into<IVec>>(&self, key: T, value: V) -> Result<bool, String> {
            match self.db.insert(key, value) {
                Ok(_) => Ok(true),
                Err(e) => Err(format!("{:?}", e)),
            }
        }
        fn get<T: AsRef<[u8]>>(&self, key: T) -> Result<Option<IVec>, String> {
            match self.db.get(key) {
                Ok(value) => Ok(value),
                Err(e) => Err(format!("{:?}", e)),
            }
        }
        fn delete<T: AsRef<[u8]>>(&self, key: T) -> Result<bool, String> {
            match self.db.remove(key) {
                Ok(_) => Ok(true),
                Err(e) => Err(format!("{:?}", e))
            }
        }
    }

    #[test]
    fn test1(){
        let a = KvDbOpera::new(initialization("/tmp/welcome-to-sled".to_string()));
        dbg!(&a.insert(&[1,3,5,7,9], &[1,1,4,5,1,4]).expect("")) ;
        dbg!(&a.get(&[1,3,5,7,9]));
        dbg!(&a.delete(&[1, 3, 5, 7, 9]));
        dbg!(&a.get(&"114514"));
    }
}

pub mod data_conversion {
    pub fn bitwise_division(dividend: u32, divisor: u32) -> (u32, u32) {
        // 位运算除法
        let mut quotient = 0;
        let mut remainder = dividend;

        for i in (0..32).rev() {
            if remainder >= (divisor << i) {
                remainder -= divisor << i;
                quotient |= 1 << i;
            }
        }

        (quotient, remainder)
    }

    pub fn section_to_int(section: &[u8]) -> u128 {
        // 切片转整数
        let mut b: u128 = 0;
        for i in section {
            b += *i as u128;
        }
        b
    }

    pub fn int_to_vec(int: u32, vec: &mut Vec<u8>) {
        // 整数转向量
        let (quotient, remainder) = bitwise_division(int, 255);
        for _ in 0..quotient {
            vec.push(255);
        }
        vec.push(remainder as u8);
    }

    #[test]
    fn section_to_int_and_int_to_vec_test() {
        let mut vec = vec![];
        int_to_vec(99012892, &mut vec);
        // dbg!(&vec);
        dbg!(&section_to_int(&vec));
    }
}


pub mod list_db {
    use sled::IVec;
    use crate::sdk::db::kv_operation::{initialization, KvDbOpera, KvDbOperaObject};
    use crate::sdk::db::kv_operation::KvDbOperaTrait;

    #[derive(Debug)]
    #[derive(Clone)]
    pub struct ListDb {
        // 数据库列表对象
        pub(crate) db: KvDbOperaObject,
        pub(crate) name: String,
    }

    impl ListDb {
        // 以Vec为主的原始列表
        pub(crate) fn get_key(&self, index: usize) -> String {
            format!("List:{}:{}", &self.name, index)
        }

        pub(crate) fn length(&self) -> Option<usize> {
            match self.db.get(format!("List:{}", self.name)) {
                Ok(v) => match v {
                    None => None,
                    Some(v1) => {
                        Some(crate::sdk::string_to_usize(String::from_utf8(v1.to_vec()).unwrap()))
                    }
                },
                _ => None,
            }
        }
        pub(crate) fn change_length(&self, len:usize) -> Result<bool,String> {
            // 更改列表长度
            let alen = self.length().unwrap(); // 原始列表长度
            match self.db.insert(format!("List:{}",&self.name),&*(len.to_string())) {
                Ok(_) => Ok(true), // 更改长度成功
                Err(e) => match self.db.insert(format!("List:{}",&self.name),&*(alen.to_string())) {
                    Ok(_) => Ok(true), // 改回原来的
                    Err(e1) => Err(format!("{},{}",e,e1)), // 抢救失败
                },
            }
        }
    }

    impl ListDb {
        pub(crate) fn new(db:KvDbOperaObject, name: String) -> Result<Self,String> {
            // 创建一个新的对象
            let key = format!("List:{name}");
            match Self::open(db.clone(),name.clone()) {
                Some(t) => Ok(t), // 列表已存在,则不创建,直接返回
                None => {
                    match db.insert(&key,&*(0.to_string())) {
                        Ok(_) => Ok(ListDb {db,name}), // 返回正确的对象
                        Err(e) => match db.delete(&key) { // 创建列表失败,开始收拾残局(也就是删掉创建到一半的列表)
                            Ok(_) => Err(e),
                            Err(e1) => Err(format!("{},{}",e,e1)) // 收拾残局失败
                        }
                    }
                }
            }
        }

        pub(crate) fn open(db:KvDbOperaObject, name: String) -> Option<Self> {
            // 打开一个哈希表
            let key = format!("List:{name}");
            match db.get(&key).expect("Data acquisition failed") {
                Some(_) => Some(ListDb {db,name}), // 列表已存在,直接返回
                None => None
            }
        }

        pub(crate) fn append(&self, value: &Vec<u8>) -> Result<bool,String> {
            // 追加
            let index = match self.length() { // 获取原来列表长度
                None => { return Err("Failed to obtain List length".to_string()) }
                Some(l) => l
            };
            match self.change_length(index+1) { // 先给列表长度加一,这是为了通过覆写的列表长度检查
                Ok(_) => self.overwrite(index, value), // 覆写新index对应的数据
                Err(e) => Err(e),
            }
        }
        pub(crate) fn access(&self, index: usize) -> Option<Vec<u8>> {
            // 访问数据
            match self.db.get(self.get_key(index)) {
                Ok(v) => match v {
                    Some(t) => Some(t.to_vec()), // 把IVec转化为常用的Vec
                    _ => None
                },
                _ => None
            }
        }

        pub(crate) fn overwrite(&self, index: usize, value: &Vec<u8>) -> Result<bool,String> {
            // 覆写数据
            if self.length().unwrap() >= index + 1 { // 检查index是否超过列表长度
                match self.db.insert(self.get_key(index), IVec::from(value.clone())) { // 调用数据库执行写入
                    Ok(_) => Ok(true),
                    Err(e) => match self.delete(index) { // 覆写失败,收拾残局
                        Ok(_) => Err(e),
                        Err(e1) => Err(format!("{e},{e1}")) // 收拾残局失败,没救了
                    },
                }
            } else {
                Err("Index too large".to_string()) // index超过了列表长度,不符合列表元素覆写规则
            }

        }
        pub(crate) fn safety_overwrite(&self, index: usize, value: &Vec<u8>) -> Result<bool,String> {
            // 带有边界检查的覆写
            if self.length().unwrap() != 0{
                if self.length().unwrap()-1 < index {
                    self.change_length(index+1).unwrap();
                    self.overwrite(index, value)
                } else {
                    self.overwrite(index, value)
                }
            } else {
                self.change_length(index+1).unwrap();
                self.overwrite(index, value)
            }

        }
        pub(crate) fn delete(&self, index: usize) -> Result<bool,String> {
            if index != 0 {
                if self.length().unwrap()-1 == index {
                    match self.change_length(index-1) {
                        Ok(_) => {},
                        Err(e) =>  {dbg!(e);todo!();} // 没救了
                    }
                };
            }

            self.db.delete(self.get_key(index))
        }
    }

    // impl Iterator for ListDB<String> {
    //     type Item = Option<String>;
    //
    //     fn next(&mut self) -> Option<Self::Item> {
    //         if (self.length().unwrap() <= self.iteration){
    //             None
    //         } else {
    //             Some(self.access(self.iteration))
    //         }
    //     }
    // }
    #[test]
    fn list_test_string(){
        let a = ListDb::open(
            KvDbOpera::new(initialization("/tmp/welcome-to-sled".to_string())),
            "113314".to_string()
        ).expect("");
        // dbg!(&a.append(&"很可爱?".to_string()));
        // dbg!(&a.append(&"Yes,cute?".to_string()));

        // for _ in 0..1000{
        //     &a.append(&"kawaii?".to_string());
        // }
        // dbg!(&a.access(0));
        // dbg!(&a.delete(0));
        // dbg!(&a.access(0));
        // dbg!("开始遍历");
        // for i in a {
        //     dbg!(i);
        // }
        for i in 0..a.length().unwrap() {
            println!("{:?}",a.delete(i))
        }
        a.change_length(0+1).unwrap();
        a.overwrite(0,&vec![1]).unwrap();
        dbg!(a.access(0)) ;
    }
}

pub mod tuple_list_db {
    use crate::sdk::db::list_db as list;
    use crate::sdk::db::kv_operation::{initialization, KvDbOperaObject};

    #[derive(Debug)]
    #[derive(Clone)]
    pub struct TupleList {
        list : list::ListDb,
        name : String,
        len : u16,
    }

    impl  TupleList {
        // 元组列表的思想是利用列表来储存固定长度的元组
        // 也就是类似于: 元组列表([(1,2),(3,4)]),实际列表([1,2,3,4])
        // 因此元组列表的实际列表的长度有 2*元组列表长度 的关系
        pub(crate) fn new(db: KvDbOperaObject, name: String, len: u16) -> Result<Self, String> {
            // 创建一个元组列表对象
            match list::ListDb::new(db,format!("Tuple:{name}")) { // 构建列表对象
                Ok(list) => Ok(TupleList { list,name, len}),
                Err(e) => Err(format!("Failed to create List : {e}")),
            }
        }

        pub(crate) fn open(db: KvDbOperaObject, name: String, len: u16) -> Option<Self> {
            // 打开一个元组列表,并且创建对象
            match list::ListDb::open(db,format!("Tuple:{name}")) {
                Some(list) => Some(TupleList { list,name, len}),
                _ => None,
            }
        }

        pub(crate) fn append(&self, value: &Vec<&Vec<u8>>) -> Result<bool, String> {
            // 追加元组元素
            self.list.change_length(self.list.length().unwrap() + value.len()).unwrap();
            self.overwrite(self.length().unwrap()-1,value)
        }

        pub(crate) fn access(&self, index: usize) -> Option<Vec<Option<Vec<u8>>>> {
            // 访问,如果返回结果为 [] ,则就等于没有
            let mut ret:Vec<Option<Vec<u8>>> = vec![];
            for i in index*(self.len as usize)..(index*(self.len as usize))+self.len as usize{
                ret.push(self.list.access(i));
            }
            return if ret == vec![] {
                None
            } else {
                Option::from(ret)
            }
        }

        pub(crate) fn overwrite(&self, index: usize, value: &Vec<&Vec<u8>>) -> Result<bool, String> {
            // 覆写元组
            if value.len() != (self.len as usize)  { return Err("Value length error".to_string()) } // 输入长度错误
            for i in  0..value.len(){
                match self.list.overwrite(index*(self.len as usize)+i, value[i]) { // 覆写原始数据
                    Ok(_) => {  },
                    Err(e) => {
                        for j in (index*(self.len as usize))..(index*(self.len as usize))+i{ // 收拾残局(注意当前覆写的残局已被 self.List.overwrite 收拾干净了,所以不需要再收拾一遍了)
                            let _ = &self.list.delete(j);
                        }return Err(e); // 输出错误
                    }
                }
            }
            Ok(true)
        }
        pub(crate) fn safety_overwrite(&self, index: usize, value: &Vec<&Vec<u8>>) -> Result<bool, String> {
            if value.len() != (self.len as usize)  { return Err("Value length error".to_string()) } // 输入长度错误
            for i in  0..value.len(){
                match self.list.safety_overwrite(index*(self.len as usize)+i, value[i]) { // 覆写原始数据
                    Ok(_) => {  },
                    Err(e) => {
                        for j in (index*(self.len as usize))..(index*(self.len as usize))+i{ // 收拾残局(注意当前覆写的残局已被 self.List.overwrite 收拾干净了,所以不需要再收拾一遍了)
                            let _ = &self.list.delete(j);
                        }return Err(e); // 输出错误
                    }
                }
            }
            Ok(true)
        }
        pub(crate) fn safety_overwrite_tuple_elements(&self, index: usize,tuple_index:u16,value : &Vec<u8>) -> Result<bool, String> {
            // 列表index和列表index对应的元组index
            // 覆写列表中的元组中的某个元素的值
            if tuple_index > self.len { return Err("Value length error".to_string()) } // 输入长度错误
            match self.list.safety_overwrite(self.get_list_index(index,tuple_index), value) { // 覆写原始数据
                Ok(_) => { Ok(true) },
                Err(e) => {
                        let _ = self.delete_tuple_elements(index,tuple_index).unwrap(); // 收拾残局,失败即报错
                        return Err(e); // 输出错误
                }
            }
        }

        pub(crate) fn overwrite_tuple_elements(&self, index: usize,tuple_index:u16,value : &Vec<u8>) -> Result<bool, String> {
            // 列表index和列表index对应的元组index
            // 覆写列表中的元组中的某个元素的值
            if tuple_index > self.len { return Err("Value length error".to_string()) } // 输入长度错误
            match self.list.overwrite(self.get_list_index(index,tuple_index), value) { // 覆写原始数据
                Ok(_) => { Ok(true) },
                Err(e) => {
                    let _ = self.delete_tuple_elements(index,tuple_index).unwrap(); // 收拾残局,失败即报错
                    return Err(e); // 输出错误
                }
            }
        }

        pub(crate) fn access_tuple_elements(&self, index: usize,tuple_index:u16) -> Option<Vec<u8>> {
            self.list.access(self.get_list_index(index,tuple_index))
        }

        pub(crate) fn delete_tuple_elements(&self, index: usize,tuple_index:u16) -> Result<bool, String> {
            // 列表index和列表index对应的元组index
            // 删除列表中的元组中的某个元素的值
            self.list.delete(self.get_list_index(index,tuple_index))
        }

        fn get_list_index(&self, index: usize,tuple_index:u16) -> usize {
            // 列表index和列表index对应的元组index
            index*(self.len as usize)+(tuple_index as usize)
        }

        pub(crate) fn delete(&self, index: usize){
            // 删除列表中的某个元组
            for j in (index*(self.len as usize))..(index*(self.len as usize))+(self.len as usize) { // 收拾残局(注意当前覆写的残局已被 self.List.overwrite 收拾干净了,所以不需要再收拾一遍了)
                let _ = &self.list.delete(j);
            }
        }

        fn get_key(&self, index: usize) -> Vec<String> {
            // 返回在数据库中的索引
            let list_hard_index = index*(self.len as usize);
            let mut ret = vec![];
            for i in 0..self.len{
                ret.push(self.list.get_key(list_hard_index+(i as usize)));
            }
            ret
        }

        pub(crate) fn length(&self) -> Option<usize> {
            match self.list.length() {
                Some(t) => {
                    Some(t/(self.len as usize))},
                None => None,
            }
        }
    }
    #[test]
    fn test(){
        use crate::sdk::db::kv_operation::KvDbOperaTrait;
        let a = TupleList::open( // 如果这个元组列表不存在,需要先 new
            KvDbOperaObject::new(initialization("/tmp/welcome-to-sled".to_string())), "156745qxxs23".to_string(), 2).unwrap();
        dbg!(a.length().unwrap());
        dbg!(&a.append(&vec![&"I love".to_string().as_bytes().to_vec(), &"XXXXXXXXXXXX".to_string().as_bytes().to_vec()]));
        dbg!(a.length().unwrap());
        dbg!(&a.access(a.length().unwrap()-1));
        dbg!(&a.delete(a.length().unwrap()-1));
        dbg!(a.length().unwrap());
        dbg!(&a.access(a.length().unwrap()));
        dbg!(&a.append(&vec![&"I love".to_string().as_bytes().to_vec(), &"She".to_string().as_bytes().to_vec()]));
        dbg!(&a.overwrite(0,&vec![&"I love".to_string().as_bytes().to_vec(),& "He".to_string().as_bytes().to_vec()]));
        dbg!(&a.access(a.length().unwrap()-1));
        dbg!(&a.delete(a.length().unwrap()-1));
        dbg!(a.length().unwrap());
    }
}

pub mod hashtable_sled_db {
    // 储存键列表 + 基于sled的哈希表
    use crate::sdk::db::kv_operation::{initialization, KvDbOperaObject};
    use crate::sdk::db::list_db::ListDb;
    use crate::sdk::db::kv_operation::KvDbOperaTrait;

    struct OriginalHashtable { // 原始hash表,只有键值对
        db : KvDbOperaObject,
        name : String,
    }

    struct Hashtable { // 原始hash表,只有键值对
        hashtable: OriginalHashtable,
        list : ListDb,
    }

    impl OriginalHashtable {
        fn new(db:KvDbOperaObject , name : String) -> Self {
            OriginalHashtable { db,name }
        }

        fn insert(&self,key:&String,value:Vec<u8>) -> Result<bool,String> {
            self.db.insert(self.get_key(key),value)
        }

        fn get(&self,key:&String) -> Result<Option<Vec<u8>>,String> {
            match self.db.get(self.get_key(key)) {
                Ok(t) => Ok(match t {
                    Some(t1) => Some(t1.to_vec()),
                    _ => None
                }),
                Err(e) => Err(e)
            }
        }

        fn delete(&self,key:&String) {
            let _ = self.db.delete(self.get_key(key));
        }

        fn get_key(&self,key:&String) -> String {
            format!("key-value:{}:{}",self.name,&key)
        }

        fn upgrade_to_iterable(self,list:ListDb) -> Hashtable {
            Hashtable { hashtable: self, list }
        }
    }



    // impl Hashtable {
    //     fn insert(&self,key:&String,value:Vec<u8>) -> Result<bool,String> {
    //         self.hashtable.insert(key,value)
    //     }
    //
    //     fn get(&self,key:&String) -> Result<Option<Vec<u8>>,String> {
    //         match self.db.get(self.get_key(key)) {
    //             Ok(t) => Ok(match t {
    //                 Some(t1) => Some(t1.to_vec()),
    //                 _ => None
    //             }),
    //             Err(e) => Err(e)
    //         }
    //     }
    //
    //     fn delete(&self,key:&String) {
    //         if let _ = self.db.delete(self.get_key(key)) {}
    //     }
    //
    //     fn get_key(&self,key:&String) -> String {
    //         format!("key-value:{}:{}",self.name,&key)
    //     }
    // }

    #[test]
    fn test_original_hashtable () {
        let a = OriginalHashtable::new(KvDbOperaObject::new(initialization("/tmp/welcome-to-sled".to_string())),"823789792".to_string());
        dbg!(&a.insert(&"heheh".to_string(), vec![1, 1, 4, 5, 1, 4]));
        dbg!(&a.get(&"heheh".to_string()));
        dbg!(&a.delete(&"heheh".to_string()));
        dbg!(&a.get(&"heheh".to_string()));
    }

}

pub mod hashtable_zipper_db {
    // 拉链法哈希表
    use crate::sdk::db::list_db::ListDb;
    use crate::sdk::db::kv_operation::{initialization, KvDbOperaObject};
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;
    use crate::sdk::db::tuple_list_db::TupleList;

    #[derive(Debug)]
    #[derive(Clone)]
    pub struct Hashtable {
        db : KvDbOperaObject,
        hashlist : ListDb,
    }

    impl Hashtable {
        pub fn new(db:KvDbOperaObject,name:String) -> Self {
            Hashtable { db: db.clone() , hashlist : ListDb::new(db.clone(),format!("HashtableHashlist:{name}")).unwrap() } // 创建list对象
        }

        pub fn open(db:KvDbOperaObject,name:String) -> Self {
            Hashtable { db: db.clone() , hashlist : ListDb::open(db.clone(),format!("HashtableHashlist:{name}")).unwrap() } // 创建list对象
        }

        pub fn insert(&self,key:&String,value:&Vec<u8>) -> Result<bool,String> {
            let hash_value = self.get_hash(key);
            return match self.hashlist.access(hash_value) {
                Some(lzip_name) => { // 此情况为hash碰撞的情况
                    let lzip = self.new_lzip(lzip_name);
                    // 此元组列表为 [(key,value)]
                    for i in 0..lzip.length().unwrap() {
                        if &String::from_utf8(lzip.access(i).unwrap()[0].clone().unwrap()).unwrap() == key {
                            return lzip.overwrite(i, &vec![&key.as_bytes().to_vec(), value])
                        }
                    } // 判断有没有已经存在的键,如果存在,就直接改
                    lzip.append(&vec![&key.as_bytes().to_vec(), value]) // 如果不存在,直接追加
                }
                None => { // 没有碰撞
                    if (hash_value + 1) > self.hashlist.length().unwrap() { // 当前键大于列表长度
                        self.hashlist.change_length(hash_value + 1).unwrap(); // 更改列表长度,以用于下面的覆写
                    }
                    self.hashlist.overwrite(hash_value, &key.as_bytes().to_vec()).unwrap(); // 覆写为lzip的名字

                    let lzip = self.new_lzip(key.as_bytes().to_vec());
                    lzip.append(&vec![&key.as_bytes().to_vec(), value]) // 追加以完成写入
                }
            };
        }
        pub fn get(&self,key:&String) -> Option<Vec<u8>> {
            let hash_value = self.get_hash(key);
            return match self.hashlist.access(hash_value) {
                Some(lzip_name) => {
                    let lzip = self.new_lzip(lzip_name);
                    // 此元组列表为 [(key,value)]
                    for i in 0..lzip.length().unwrap() { // 处理可能的哈希碰撞
                        if &String::from_utf8(lzip.access(i).unwrap()[0].clone().unwrap()).unwrap() == key {
                            return Some(lzip.access(i).unwrap()[1].clone().unwrap().clone()) // 提取出value
                        }
                    } // 判断有没有已经存在的键
                    None
                },
                None =>  None ,
            }

        }

        pub fn delete(&self,key:&String) -> Result<bool,String> {
            let hash_value = self.get_hash(key);
            return match self.hashlist.access(hash_value) {
                Some(lzip_name) => {
                    let lzip = self.new_lzip(lzip_name);
                    // 此元组列表为 [(key,value)]
                    for i in 0..lzip.length().unwrap() { // 处理可能的哈希碰撞
                        if &String::from_utf8(lzip.access(i).unwrap()[0].clone().unwrap()).unwrap() == key {
                            lzip.delete(i);
                            return Ok(true)
                        }
                    } // 判断有没有已经存在的键
                    Err("Key does not exist".to_string())
                },
                None =>  Err("Key does not exist".to_string()) ,
            }
        }
        pub fn to_tuple_list(&self, number_of_entries:Option<usize>) -> Vec<(String, Vec<u8>)> {
            // 时间复杂度极高,慎用!这是把哈希表所有内容提取出来的方法之一
            // 算法过程 : 迭代散列的所有元素,也就是迭代 self.hashlist,将其中的元组列表的数据挨个提取出来
            // number_of_entries 为条目数量
            let mut ret = vec![];
            match number_of_entries {
                Some(t) => {
                    let mut number_of_entries_i = 0usize;
                    for i in 0..self.hashlist.length().unwrap() {
                        match self.hashlist.access(i) {
                            Some(lzip_name) => {
                                let lzip = self.new_lzip(lzip_name);
                                for i in 0..lzip.length().unwrap(){
                                    let value = lzip.access(i).unwrap();
                                    ret.push((String::from_utf8(value[0].clone().unwrap().clone()).unwrap(),value[1].clone().unwrap().clone()));
                                    if number_of_entries_i >= t {
                                        return ret
                                    } else {
                                        number_of_entries_i += 1;
                                    }
                                }
                            },
                            _ => {},
                        }
                    }
                }
                None => {
                    for i in 0..self.hashlist.length().unwrap() {
                        match self.hashlist.access(i) {
                            Some(lzip_name) => {
                                let lzip = self.new_lzip(lzip_name);
                                for i in 0..lzip.length().unwrap(){
                                    let value = lzip.access(i).unwrap();
                                    ret.push((String::from_utf8(value[0].clone().unwrap().clone()).unwrap(),value[1].clone().unwrap().clone()));
                                }
                            },
                            _ => {},
                        }
                    }
                    return ret
                }
            }
            ret
        }

        fn get_hash(&self,key:&String) -> usize {
            let mut hasher = DefaultHasher::new();key.hash(& mut hasher );
            (hasher.finish() % 10000) as usize
        }

        fn new_lzip(&self,name:Vec<u8>) -> TupleList {
            TupleList::new(self.db.clone(), format!("lzip:{}:{}",self.hashlist.name,String::from_utf8(name).unwrap()), 2).unwrap()
        }
    }

    #[test]
    fn test_hashtable(){
        use crate::sdk::db::kv_operation::KvDbOperaTrait;
        let a = Hashtable::open(KvDbOperaObject::new(initialization("/tmp/welcome-to-sled".to_string())),"1]&_+3)_~*-1)4".to_string());
        dbg!(&a.insert(&"lst".to_string(), &vec![1, 5, 2]));
        dbg!(&a.get(&"lst".to_string()));
        dbg!(&a.insert(&"I li".to_string(), &vec![1, 1, 4, 5, 1, 4]));

        dbg!(&a.to_tuple_list(Some(10usize)));
        dbg!(&a.delete(&"lst".to_string()));
        dbg!(&a.get(&"lst".to_string()));
    }
}
