use prost::Message;
use sraft::sraft::api::grpc;

fn main() -> () {
    let ks = fjall::Config::new("./scratch.db").open().unwrap();
    let db = ks
        .open_partition("scrach", fjall::PartitionCreateOptions::default())
        .unwrap();
    let mut entry = grpc::LogEntry {
        term: 137,
        command: Some(grpc::Command {
            key: "fooooooooooooooooooooooooooooooooooooooo".to_string(),
            value: Vec::from("chlos chlos chlos chlos chlos"),
        }),
    };
    db.insert(0u64.to_be_bytes(), entry.encode_to_vec())
        .unwrap();
    let v = db.get(0u64.to_be_bytes()).unwrap().unwrap();
    println!("{:?}", grpc::LogEntry::decode(v.as_ref()).unwrap());

    let mut b = Vec::new();
    // println!("{} {}", b.len(), b.capacity());
    // b.resize(entry.encoded_len(), 0);
    // println!("{} {}", b.len(), b.capacity());
    entry.encode(&mut b).unwrap();
    println!("{} {}", b.len(), b.capacity());
    println!(">>>>>>>>>>>>> {:?}", b);
    println!(">>>>>>>>>>>>> {:?}", entry.encode_to_vec());
    entry.command = Some(grpc::Command {
        key: "AAA".to_string(),
        value: Vec::from("AAA"),
    });
    b.truncate(0);
    entry.encode(&mut b).unwrap();
    println!(">>>>>>>>>>>>> {:?}", b);
    println!(">>>>>>>>>>>>> {:?}", entry.encode_to_vec());

    ()
}
