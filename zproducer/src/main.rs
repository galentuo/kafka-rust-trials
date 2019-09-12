extern crate kafka;

use std::time::Duration;

use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::error::Error as KafkaError;

/// This program demonstrates sending single message through a
/// `Producer`.  This is a convenient higher-level client that will
/// fit most use cases.
fn main() {

    let broker = "localhost:9092";
    let topic = "my-topic";

    let data = "{\"msg\":\"hello\"}".as_bytes();

    if let Err(e) = produce_message(data, topic, vec![broker.to_owned()]) {
        println!("Failed producing messages: {}", e);
    }
}

fn produce_message<'a, 'b>(
    data: &'a [u8],
    topic: &'b str,
    brokers: Vec<String>,
) -> Result<(), KafkaError> {
    println!("About to publish a message at {:?} to: {}", brokers, topic);

    // ~ create a producer. this is a relatively costly operation, so
    // you'll do this typically once in your application and re-use
    // the instance many times.
    let mut producer = 
        Producer::from_hosts(brokers)
             // ~ give the brokers one second time to ack the message
             .with_ack_timeout(Duration::from_secs(1))
             // ~ require only one broker to ack the message
             .with_required_acks(RequiredAcks::One)
             // ~ build the producer with the above settings
             .create().unwrap();
    

    // ~ now send a single message.  this is a synchronous/blocking
    // operation.
    let _res = producer.send(&Record::from_value(topic, data));

    Ok(())
}