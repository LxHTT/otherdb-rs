// 索引vec语法解析
// 用来方便操作数据库
use crate::sdk::db::kv_operation::{initialization, KvDbOperaObject, KvDbOperaTrait};
use crate::sdk::db::list_db::ListDb;
use crate::sdk::db::hashtable_zipper_db::Hashtable;
use crate::sdk::db::tuple_list_db::TupleList;
use uuid::Uuid;

#[derive(Clone)]
#[derive(Debug)]
pub enum IndexVecElement { // IndexVec 支持的值
    Hashtable(String), // hashtable key
    List(usize), // list's index
    TupleList(usize,u16,u16), // list's index & tuple's index & tuple's len
    HeadMarking, // 用于标记一个IndexVec的开始,里面包含指向档案数据的Index
}

#[derive(Debug)]
#[derive(Clone)]
pub enum AnalysisElement { // ElementAnalysisResults 中结果的对象 , 同时也是查询结果
    Hashtable(Hashtable),
    List(ListDb),
    TupleList(TupleList),
    HeadMarking, // 用于标记一个IndexVec的开始
}

pub struct ElementAnalysisResults(AnalysisElement,IndexVecElement); // IndexVecOperate::AnalysisElement 返回值
// struct EAR(AE,IVE); // as ElementAnalysisResults
pub type IndexVec = Vec<IndexVecElement>;
pub type AE = AnalysisElement;
pub type IVE = IndexVecElement;
type UuidIndex = String; // 这里使用uuid创建中间索引
pub type EAR = ElementAnalysisResults;

fn option_vec_to_string(vec: Option<Vec<u8>>) -> String {
    String::from_utf8(vec.unwrap()).unwrap()
}

#[derive(Clone)]
pub struct Archive {
    db : KvDbOperaObject,
    name : String,
    head_index_uuid : String,
}

impl EAR {
    fn new(ae:AE,ive:IVE) -> Self {
        ElementAnalysisResults(ae,ive)
    }
    pub fn access(&self) -> Option<Vec<u8>> {
        // 访问 EAR 指定的内容
        match self.0.clone() {
            AE::HeadMarking => { None },
            AE::Hashtable(obj) => {
                Some(if let IVE::Hashtable(key) = self.1.clone() {
                    return obj.get(&key)
                })
            },
            AE::List(obj) => {
                Some(if let IVE::List(index) = self.1.clone() {
                    return obj.access(index)
                })
            }
            AE::TupleList(obj) => {
                Some(if let IVE::TupleList(index,tindex,_) = self.1.clone() {
                    return obj.access_tuple_elements(index,tindex)
                })
            }
        };
        None
    }
    pub fn overwrite(&self,data:&Vec<u8>) -> Result<bool,String> {
        // 覆写 EAR 指定的内容
        match self.0.clone() {
            AE::HeadMarking => { None },
            AE::Hashtable(obj) => {
                Some(if let IVE::Hashtable(key) = self.1.clone() {
                    return obj.insert(&key,data)
                })
            },
            AE::List(obj) => {
                Some(if let IVE::List(index) = self.1.clone() {
                    return obj.safety_overwrite(index,data)
                })
            }
            AE::TupleList(obj) => {
                Some(if let IVE::TupleList(index,tindex,_) = self.1.clone() {
                    return obj.safety_overwrite_tuple_elements(index,tindex,data)
                })
            }
        };
        Err("Incorrect ElementAnalysisResults (EAR) object format".to_string())
    }
    pub fn delete(&self) -> Result<bool,String> {
        // 删除 EAR 指定的内容
        match self.0.clone() {
            AE::HeadMarking => { None },
            AE::Hashtable(obj) => {
                Some(if let IVE::Hashtable(key) = self.1.clone() {
                    return obj.delete(&key)
                })
            },
            AE::List(obj) => {
                Some(if let IVE::List(index) = self.1.clone() {
                    return obj.delete(index)
                })
            }
            AE::TupleList(obj) => {
                Some(if let IVE::TupleList(index,tindex,_) = self.1.clone() {
                    return obj.delete_tuple_elements(index,tindex)
                })
            }
        };
        Err("Incorrect ElementAnalysisResults (EAR) object format".to_string())
    }
    pub fn to_ive(&self) -> IVE {
        // 转化为IVE
        return self.1.clone()
    }
}

impl Archive {
    pub fn new_object(db:KvDbOperaObject,name:String) -> Self {
        // 创建一个 Archive 对象,如果其不存在会创建
        // db : 数据库对象
        // name : 档案名字
        let head_index_uuid = Uuid::new_v4().to_string();
        db.insert(format!("archive:{name}").as_bytes().to_vec(),head_index_uuid.clone().as_bytes().to_vec()).unwrap(); // 创建档案
        Self { db,name, head_index_uuid}
    }

    pub fn open_object(db:KvDbOperaObject,name:String) -> Self {
        // 打开一个 Archive 对象,如果其不存在会报错
        // db : 数据库对象
        // name : 档案名字
        let head_index_uuid = String::from_utf8(db.get(format!("archive:{}",&name)).unwrap().unwrap().to_vec()).unwrap();
        Self { db , name , head_index_uuid }
    }

    fn new_database_objects(&self,el:IVE,index_uuid:UuidIndex) -> EAR {
        // 获得某个元素的数据库对象,如果其不存在,则创建
        match el {
            IVE::Hashtable(key) => {
                EAR::new(AE::Hashtable(Hashtable::new(self.db.clone(),index_uuid)),IVE::Hashtable(key))
            },
            IVE::List(index) => {
                EAR::new(AE::List(ListDb::new(self.db.clone(),index_uuid).unwrap()),IVE::List(index))
            },
            IVE::TupleList(index,tuple_index,len) => {
                EAR::new(AE::TupleList(TupleList::new(self.db.clone(),index_uuid,len).unwrap()),IVE::TupleList(index,tuple_index,len))
            },
            IVE::HeadMarking => {
                EAR::new(AE::HeadMarking,IVE::HeadMarking)
            },
        }
    }
    fn get_database_objects(&self,el:IVE,index_uuid:UuidIndex) -> EAR {
        // 获得某个元素的数据库对象
        match el {
            IVE::Hashtable(key) => {
                EAR::new(AE::Hashtable(Hashtable::open(self.db.clone(),index_uuid)),IVE::Hashtable(key))
            },
            IVE::List(index) => {
                EAR::new(AE::List(ListDb::open(self.db.clone(),index_uuid).unwrap()),IVE::List(index))
            },
            IVE::TupleList(index,tuple_index,len) => {
                EAR::new(AE::TupleList(TupleList::open(self.db.clone(),index_uuid,len).unwrap()),IVE::TupleList(index,tuple_index,len))
            },
            IVE::HeadMarking => {
                EAR::new(AE::HeadMarking,IVE::HeadMarking)
            },
        }
    }

    fn analysis_element(&self,el:IVE,previous_parsing_result_data:Option<UuidIndex>) -> UuidIndex {
        // 解析 IndexVec 中的某个元素(IVE类型),返回对应的UuidIndex
        match el {
            IVE::Hashtable(key) => {
                option_vec_to_string(Hashtable::open(self.db.clone(),previous_parsing_result_data.unwrap()).get(&key))
            },
            IVE::List(index) => {
                option_vec_to_string(ListDb::open(self.db.clone(),previous_parsing_result_data.unwrap()).unwrap().access(index))
            },
            IVE::TupleList(index,tuple_index,len) => {
                option_vec_to_string(TupleList::open(self.db.clone(),previous_parsing_result_data.unwrap(),len).unwrap().access_tuple_elements(index,tuple_index))
            },
            IVE::HeadMarking => {
                self.head_index_uuid.clone()
            },
        }
    }
    fn establish_uuid_index(&self,el:IVE,previous_parsing_result_data:Option<UuidIndex>) -> UuidIndex {
        // 在某一个元素之上建立一个索引,只适用于 new 模式
        let uuid = Uuid::new_v4().to_string();
        match el {
            IVE::Hashtable(key) => {
                Hashtable::new(self.db.clone(), previous_parsing_result_data.unwrap()).insert(&key, &uuid.clone().as_bytes().to_vec())
            },
            IVE::List(index) => {
                let list = ListDb::new(self.db.clone(), previous_parsing_result_data.unwrap()).unwrap();
                list.safety_overwrite(index,&uuid.clone().as_bytes().to_vec())
            },
            IVE::TupleList(index,tuple_index,len) => {
                TupleList::new(self.db.clone(), previous_parsing_result_data.unwrap(), len).unwrap().overwrite_tuple_elements(index, tuple_index, &uuid.as_bytes().to_vec())
            },
            IVE::HeadMarking => { return self.head_index_uuid.clone() },
        }.unwrap();
        uuid
    }
    fn _new(&self, iv:IndexVec,head:usize, index_uuid:Option<UuidIndex>) -> Option<EAR>{
        // 新建一个 IndexVec 对应的数据库对象
        if head < iv.len()-1 {
            self._new(iv.clone(),head+1,Some(self.establish_uuid_index(iv[head].clone(),index_uuid)))
        } else {
            Some(self.new_database_objects(iv[head].clone(),index_uuid.unwrap()))
        }
    }
    fn _open(&self, iv:IndexVec,head:usize, index_uuid:Option<UuidIndex>) -> Option<EAR>{
        // 打开一个 IndexVec 对应的数据库对象
        if head < iv.len()-1 {
            // dbg!(&self.analysis_element(iv[head].clone(),index_uuid.clone()));
            self._open(iv.clone(),head+1,Some(self.analysis_element(iv[head].clone(),index_uuid)))
        } else {
            Some(self.get_database_objects(iv[head].clone(),index_uuid.unwrap()))
        }
    }
    pub fn new(&self, iv:IndexVec) -> Option<EAR> {
        self._new(iv, 0, None)
    }
    pub fn open(&self, iv:IndexVec) -> Option<EAR> {
        self._open(iv, 0, None)
    }
    pub fn delete(&self, iv:IndexVec) -> Result<bool,String> {
        self.open(iv).unwrap().delete()
    }
}

#[test]
fn test1() {
    let a = Archive::open_object(KvDbOperaObject::new(initialization("/tmp/welcome-to-sled".to_string()))
                                , "12_+_514".to_string());
    let iv = vec![IVE::HeadMarking, IVE::List(3), IVE::Hashtable("lst".to_string())];
    let lev = a.open(iv.clone()).unwrap();
    dbg!(&lev.overwrite(&vec![1, 1, 4, 5, 1, 4]));
    dbg!(&lev.access());
    dbg!(&a.delete(iv.clone()));
    // || dbg!(&lev.delete())
    dbg!(&lev.access());
    // dbg!(&l);
}