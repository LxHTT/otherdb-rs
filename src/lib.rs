use pyo3::prelude::*;
mod sdk;

use sdk::db::kv_operation;
use crate::sdk::db::list_db::ListDb;
use sdk::db::hashtable_zipper_db::Hashtable as HashtableDb;
use crate::sdk::db::kv_operation::{ KvDbOperaTrait};
use sdk::archive;

/// A Python module implemented in Rust.
#[pymodule]
fn other_dbpy(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open_db, m)?)?;
    m.add_function(wrap_pyfunction!(archive_new, m)?)?;
    m.add_function(wrap_pyfunction!(archive_open, m)?)?;

    m.add_class::<PyKvDbOperaObject>()?;
    m.add_class::<List>()?;
    m.add_class::<Hashtable>()?;
    m.add_class::<Archive>()?;
    m.add_class::<EAR>()?;

    Ok(())
}


#[pyfunction]
fn open_db(path:String) -> PyKvDbOperaObject{
    PyKvDbOperaObject{ db : kv_operation::KvDbOpera::new(kv_operation::initialization(path)) }
}

#[derive(Clone)]
#[pyclass]
struct PyKvDbOperaObject{
    db : kv_operation::KvDbOperaObject
}
#[pymethods]
impl PyKvDbOperaObject {
    fn clone(&self) -> Self {
        // 在python克隆自己,我是线程安全的
        self.clone()
    }
}


#[pyclass]
struct List {
    list_db_obj :ListDb
}

#[pymethods]
impl List {
    #[new]
    fn new(db:PyKvDbOperaObject,name:String) -> Self {
        List{ list_db_obj: ListDb::new(db.db.clone(),name).unwrap() }
    }

    // fn open(db:PyKvDbOperaObject,name:String) -> Self {
    //     List{ list_db_obj: ListDb::open(db.db.clone(),name).unwrap() }
    // }

    fn append(&self,value:Vec<u8>) -> bool {
        match self.list_db_obj.append(&value) {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    fn access(&self,index:usize) -> Option<Vec<u8>> {
        match self.list_db_obj.access(index) {
            Some(t) => Some(t),
            _ => None,
        }
    }
    fn overwrite(&self,index:usize,value:Vec<u8>) -> bool {
        match self.list_db_obj.overwrite(index,&value) {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    fn delete(&self,index:usize) -> bool {
        match self.list_db_obj.delete(index) {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    fn len(&self) -> Option<usize> {
        self.list_db_obj.length()
    }
}

#[pyclass]
struct Hashtable {
    hashtable : HashtableDb
}

#[pymethods]
impl Hashtable {
    #[new]
    fn new(db:PyKvDbOperaObject,name:String) -> Self {
        Hashtable { hashtable : HashtableDb::new(db.db,name) }
    }

    // fn open(db:PyKvDbOperaObject,name:String) -> Self {
    //     Hashtable { hashtable : HashtableDb::open(db.db,name) }
    // }

    fn insert(&self,key:String,value:Vec<u8>) -> bool {
        match self.hashtable.insert(&key, &value) {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    fn get(&self,key:String) -> Option<Vec<u8>> {
        self.hashtable.get(&key)
    }
    fn delete(&self,key:String) -> bool {
        match self.hashtable.delete(&key) {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    fn to_tuple_list(&self,number_of_entries:Option<usize>) -> Vec<(String, Vec<u8>)>{
        self.hashtable.to_tuple_list(number_of_entries)
    }
}

#[pyfunction]
fn archive_open(db:PyKvDbOperaObject,name:String) -> Archive {
    // archive's open 模式的构造函数 ( 打开一个 archive )
    Archive {archive:archive::Archive::open_object(db.db,name) }
}

#[pyfunction]
fn archive_new(db:PyKvDbOperaObject,name:String) -> Archive {
    // archive's new 模式的构造函数 ( 打开或者创建一个 archive )
    Archive {archive:archive::Archive::new_object(db.db,name) }
}

#[pyclass]
struct Archive {
    archive:archive::Archive
}

fn py_index_list_to_rs_index_vec(py_index_list:Vec<(&str,Vec<String>)>) -> Result<archive::IndexVec,String> {
    let mut iv:archive::IndexVec = vec![archive::IVE::HeadMarking];
    for item in py_index_list {
        match item {
            ("Hashtable",data) => {
                iv.push(archive::IVE::Hashtable(data[0].clone()))
            },
            ("List",data) => {
                iv.push(archive::IVE::List(match data[0].clone().parse::<usize>() {
                    Ok(num) => num,
                    Err(e) => {return Err(format!("字符串解析失败,列表索引不是整数:{}",e)) }
                })) // 列表索引的字符串解析
            },
            ("TupleList",data) => {
                iv.push(archive::IVE::TupleList(
                    match data[0].clone().parse::<usize>() {
                        Ok(num) => num,
                        Err(e) => {return Err(format!("字符串解析失败,列表索引不是整数:{}",e)) }
                    },
                    match data[1].clone().parse::<u16>() {
                        Ok(num) => num,
                        Err(e) => {return Err(format!("字符串解析失败,元组索引不是整数:{}",e)) }
                    },
                    match data[2].clone().parse::<u16>() {
                        Ok(num) => num,
                        Err(e) => {return Err(format!("字符串解析失败,元组长度不是整数:{}",e)) }
                    },
                ))
            }
            _ => {
                return Err("Non-existent data type".to_string())
            }
        }
    };
    Ok(iv)
}

#[pymethods]
impl Archive {
    fn new(&self,py_index_list:Vec<(&str,Vec<String>)>) -> EAR{
        EAR{ear:self.archive.new(py_index_list_to_rs_index_vec(py_index_list).unwrap()).unwrap() }
    }
    fn open(&self,py_index_list:Vec<(&str,Vec<String>)>) -> EAR{
        EAR{ear:self.archive.open(py_index_list_to_rs_index_vec(py_index_list).unwrap()).unwrap() }
    }
}

#[pyclass]
struct EAR {
    ear : archive::EAR
}
#[pymethods]
impl EAR{
    fn access(&self) -> Option<Vec<u8>> {
        self.ear.access()
    }
    fn delete(&self) -> Option<bool> {
        if let Ok(t) = self.ear.delete() {
            Some(t)
        } else {
            None
        }
    }
    fn overwrite(&self,data:Vec<u8>) -> Option<bool> {
        if let Ok(t) = self.ear.overwrite(&data) {
            Some(t)
        } else {
            None
        }
    }
}

#[test]
fn test_a(){
    let db = open_db(r"D:\tmp\welcome-to-sled".to_string());
    let archive = archive_open(db,"878129128".to_string());
    let list = vec![
        ("Hashtable",vec!["lst".to_string() ]),
        ("List",vec!["8".to_string()]),
        ("TupleList",vec!["8".to_string(),"1".to_string(),"2".to_string()]),
    ];
    let my_data = archive.open(list);
    dbg!(&my_data.overwrite(b"114514".to_vec()));
    dbg!(&my_data.access());
    dbg!(&my_data.delete());
    dbg!(&my_data.access());

}