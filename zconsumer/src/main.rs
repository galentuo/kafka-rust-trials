extern crate kafka;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::error::Error as KafkaError;

fn main(){
    let broker = "localhost:9092".to_owned();
    let topic = "my-topic".to_owned();
    let group = "my-group".to_owned();
    
    if let Err(e) = consume_msgs(group, topic, vec![broker]) {
        println!("Failed consuming messages: {}", e);
    }
}

fn consume_msgs(group: String, topic: String, brokers: Vec<String>) -> Result<(), KafkaError> {
    let mut consumer = Consumer::from_hosts(brokers)
                    .with_topic(topic)
                    .with_group(group)
                    .with_fallback_offset(FetchOffset::Earliest)
                    .with_offset_storage(GroupOffsetStorage::Kafka)
                    .create().unwrap();
    
    loop{
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {  
                println!("{:?}", m.offset);
                let s = match std::str::from_utf8(m.value) {
                    Ok(v) => v,
                    Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                };
                println!("result: {}", s);
            }
            consumer.consume_messageset(ms);
        }
        consumer.commit_consumed().unwrap();
    }
}
