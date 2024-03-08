use my_db::Database;



fn main() {
    let mut db = Database::new();
    for _ in 0..1000 {
        db.create("Hello World".to_owned()).unwrap();
    }
}

