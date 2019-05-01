extern crate kafka;
extern crate csv;
extern crate json;
extern crate num_traits;
extern crate eventual;

use std::fs;
use std::time::Duration;
use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::Error;
use json::JsonValue;
use json::number::Number;

const ROW_COUNT: u64 = 1;

//TODO: change csv reading: csv::Reader to ReaderBuf?

fn produce_message(data: String, topic: &str, brokers: Vec<String>) -> Result<(), Error> {
    println!("About to publish a message at {:?} to: {}", brokers, topic);

    let mut producer = Producer::from_hosts(brokers)
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create().expect("Producer create error");
    producer.send(&Record::from_value(topic, data))?;

    Ok(())
}


//TODO: change reading (too many useless operations should be replaced with string processing functions)
fn read_and_send_data(filepath: &str, row_skip: u64) {
    let broker: &str = "localhost:9092";
    let topic: &str = "test";
    csv::Reader::from_path(filepath).expect("File open error")
        .records()
        .skip(row_skip as usize)
        .take(ROW_COUNT as usize)
        .for_each(|x| {
            let x = x.expect("Can't read item");
            let snd_data = x.iter()
                .skip(1)
                .map(|x| {x.to_string()})
                .fold("".to_string(), |acc, el| {
                    if acc == "".to_string(){
                        el
                    } else {
                        format!("{} {}", acc, el)
                    }
                });
        println!("{}", snd_data);
        produce_message(snd_data, topic, vec![broker.to_owned()])
            .expect("Failed producing messages");
        });
}

fn data_process(config_json: &mut JsonValue, name: &str, config_fname: &str) {
    let config: &mut JsonValue = & mut config_json[name];
    let data_fname: &str = config["fname"].as_str()
        .expect("Json to str parse error");
    let data_row_n: u64 = config["rowN"].as_u64()
        .expect("Json to int parse error");

    read_and_send_data(data_fname, data_row_n);

    let new_rn = (data_row_n + ROW_COUNT) as f64;

    config["rowN"] = JsonValue::Number(Number::from(new_rn));
    let new_row_count_str = config_json.pretty(2);
    fs::write(config_fname, new_row_count_str).expect("Error write json");
}

fn main() {
   //Useful code, if sending would have period smaller than 1 second
    /*let timer = eventual::Timer::new();
    let ticks = timer.interval_ms(1000).iter();
    for _ in ticks {*/
        let config_fname: &str = "config.json";
        let config_file: String = fs::read_to_string(config_fname)
            .expect("Json read error");
        let mut config_json: JsonValue = json::parse(config_file.as_str())
            .expect("Json parse error");
        let src_list_copy: JsonValue = config_json["sources"].clone();

        let src_names: Vec<&str> = src_list_copy.members()
            .map(|source| {source.as_str().expect("Error parse json")})
            .collect();

        for src_name in src_names {
            data_process(&mut config_json, src_name, config_fname);
        }
    //}
}