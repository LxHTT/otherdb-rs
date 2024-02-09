use sled::IVec;

#[test]
fn sled_test(){
    // 打开数据库
    let tree = &sled::open("/tmp/welcome-to-sled").expect("open");

// 插入KV，读取Key对应的值
    tree.insert([1,3,5,7,9], &*"Hah").expect("TODO: panic message");
    // assert_eq!(tree.get(&"KEY1"), Ok(Some(sled::IVec::from("VAL1"))));
    let a = tree.get([1,3,5,7,9]).unwrap().unwrap();
    dbg!(&String::from_utf8(a.to_vec()));
// 范围查询
//     for kv in tree.range("KEY1".."KEY9") {
//         ...
//     }

// 删除
    tree.remove([1, 3, 5, 7, 9]).expect("TODO: panic message") ;

// atomic compare and swap，可以用在并发编程中
//     tree.compare_and_swap("KEY1", Some("VAL1"), Some("VAL2"));

// 阻塞直到所有修改都写入硬盘
    let _ = tree.flush();
}
