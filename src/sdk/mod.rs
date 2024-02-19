pub mod db;
pub mod archive;

fn string_to_usize(number_str:String) -> usize{
    // dbg!(&number_str);
    number_str.parse::<usize>().expect("String parsing to usize failed")
}

