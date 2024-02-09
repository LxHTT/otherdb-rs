use std::sync::Arc;
use pyo3::prelude::*;
mod sdk;
use sdk::db::kv_operation;
use crate::sdk::db::list_db::ListDb;
use sdk::db::hashtable_zipper_db::Hashtable as HashtableDb;

/// A Python module implemented in Rust.
#[pymodule]
fn other_dbpy(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open_db, m)?)?;
    // m.add_function(wrap_pyfunction!(initialization_db,m)?)?;
    m.add_class::<PyKvDbOperaObject>()?;
    m.add_class::<List>()?;
    m.add_class::<Hashtable>()?;
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
    fn append(&self,value:Vec<u8>) -> bool {
        match self.list_db_obj.append(value) {
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
        match self.list_db_obj.overwrite(index,value) {
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
    fn insert(&self,key:String,value:Vec<u8>) -> bool {
        match self.hashtable.insert(&key, value) {
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
