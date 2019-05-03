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

const ROW_COUNT: usize = 1;

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
fn read_and_send_data(filepath: &str, currency_fname: &str, row_skip: usize, topic: &str) {
    let broker: String = "localhost:9092".to_string();
    //let topic: &str = "test";
    let mut main_data_reader = csv::Reader::from_path(filepath)
        .expect("File open error");
    let main_data = main_data_reader.records().skip(row_skip).take(ROW_COUNT);
    let mut currency_data_reader = csv::Reader::from_path(currency_fname)
        .expect("File open error");
    let currency_data = currency_data_reader.records().skip(row_skip).take(ROW_COUNT);
    main_data.zip(currency_data)
        .for_each(|x| {
            let str1 = x.0.expect("Can't read item");
            let str2 = x.1.expect("Can't read item");

            let snd_data1 = str1.iter()
                .map(|x| {x.to_string()})
                .fold("".to_string(), |acc, el| {
                    if acc == "".to_string(){
                        el
                    } else {
                        format!("{} {}", acc, el)
                    }
                });
            let snd_data2 = str2.iter()
                .skip(1)
                .map(|x| {x.to_string()})
                .fold("".to_string(), |acc, el| {
                    if acc == "".to_string(){
                        el
                    } else {
                        format!("{} {}", acc, el)
                    }
                });
            let snd_data = format!("{} {}", snd_data1, snd_data2);
            println!("{}", snd_data);
            produce_message(snd_data, topic, vec![broker.to_owned()])
                .expect("Failed producing message");
        });
}

fn data_process(config_json: &mut JsonValue, name: &str, config_fname: &str) {
    let config: &mut JsonValue = & mut config_json[name];
    let data_fname: &str = config["fname"].as_str()
        .expect("Json to str parse error");
    let currency_fname: &str = config["currency_file"].as_str()
        .expect("Json to str parse error");
    let data_row_n: usize = config["rowN"].as_usize()
        .expect("Json to int parse error");

    read_and_send_data(data_fname, currency_fname, data_row_n, name);

    let new_rn = (data_row_n + ROW_COUNT) as f64;

    config["rowN"] = JsonValue::Number(Number::from(new_rn));
    let new_row_count_str = config_json.pretty(2);
    fs::write(config_fname, new_row_count_str).expect("Error write json");
}

fn main() {
    /*let start = std::time::SystemTime::now();
    let api_key = alpha_vantage::set_api("DHIA0K4MIXGWKLM3");
    let data = api_key.forex(
        ForexFunction::Weekly,
        "EUR",
        "USD",
        Interval::None,
    OutputSize::None);
    let mut entries = data.entry().unwrap();
    entries.sort_by(|x, y| {
        x.time().partial_cmp(&y.time()).unwrap()
    });
    let period = entries.into_iter().filter(|x| {
            x.time() >= "2016-10-30".to_string() && x.time() <= "2018-10-29".to_string()
        }).collect::<Vec<_>>();*/
    //println!("{:?}", period);

    /*let client = alphavantage::Client::new("DHIA0K4MIXGWKLM3");
    let data: TimeSeries = client.get_time_series_weekly("EUR").unwrap();
    println!("{:?}", data);*/

    /*let mut writer = csv::Writer::from_path("new.csv").unwrap();
    writer.write_record(&["DATE", "OPEN", "HIGH", "LOW", "CLOSE"]);
    for x in period {
        writer.write_record(&[
            x.time(),
            x.open().to_string(),
            x.high().to_string(),
            x.low().to_string(),
            x.close().to_string()]
        ).unwrap();
    }*/


    let config_fname: &str = "config.json";
    let config_file: String = fs::read_to_string(config_fname)
        .expect("Json read error");
    let mut config_json: JsonValue = json::parse(config_file.as_str())
        .expect("Json parse error");
    let src_list_copy: JsonValue = config_json["sources"].clone();
    let src_names: Vec<&str> = src_list_copy.members()
        .map(|source| {source.as_str().expect("Error parse json")})
        .collect();

    let timer = eventual::Timer::new();
    let ticks = timer.interval_ms(1000).iter();

    for _ in ticks {
        for src_name in src_names.clone() {
            data_process(&mut config_json, src_name, config_fname);
        }
    }
}