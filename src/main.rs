extern crate btleplug;
extern crate clap;
extern crate ruuvi_sensor_protocol;

use crate::ruuvi_sensor_protocol::{
    Acceleration, BatteryPotential, Co2, Humidity, MeasurementSequenceNumber, MovementCounter,
    Pm25, Pressure, Temperature, TransmitterPower, DataFormat, AirDensity,
};
use clap::Parser;
use std::collections::BTreeMap;
use std::io::Write;
use std::panic::{self, PanicHookInfo};
use std::time::SystemTime;
pub mod ruuvi;
use ruuvi::{on_measurement, Measurement};

pub mod influxdb;
use influxdb::{DataPoint, FieldValue};

use crate::influxdb::write_line_to_influx;
use btleplug::Error::PermissionDenied;
use reqwest::Client;

fn tag_set(
    aliases: &BTreeMap<String, String>,
    measurement: &Measurement,
    options: &Options,
) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    let mut address = measurement.address.to_string();
    if !options.keep_mac_colons {
        address.retain(|c| c != ':');
    }
    tags.insert("mac".to_string(), address.to_string());
    tags.insert(
        "name".to_string(),
        aliases.get(&address).unwrap_or(&address).to_string(),
    );
    tags
}

macro_rules! to_float {
    ( $value: expr, $scale: expr ) => {{
        FieldValue::FloatValue(f64::from($value) / $scale)
    }};
}

macro_rules! add_value_float {
    ( $fields: ident, $value: expr, $field: expr, $scale: expr ) => {{
        if let Some(value) = $value {
            $fields.insert($field.to_string(), to_float!(value, $scale));
        }
    }};
}

macro_rules! to_integer {
    // Added
    ( $value: expr) => {{
        FieldValue::IntegerValue(i64::from($value))
    }};
}

macro_rules! add_value_integer {
    ( $fields: ident, $value: expr, $field: expr) => {{
        if let Some(value) = $value {
            $fields.insert($field.to_string(), to_integer!(value));
        }
    }};
}

// Important note! Some of the field names have been changed from the original lautis version to be
// more like the field names in RuuviCollector by Scrin (https://github.com/Scrin/RuuviCollector).
fn field_set(measurement: &Measurement) -> BTreeMap<String, FieldValue> {
    let mut fields = BTreeMap::new();
    add_value_float!(
        fields,
        measurement.sensor_values.temperature_as_millicelsius(),
        "temperature",
        1000.0
    );
    add_value_float!(
        fields,
        measurement.sensor_values.dew_point_as_celsius(
            measurement
                .sensor_values
                .humidity_as_ppm()
                .unwrap_or_else(|| 999_999_999) as f32
                / 10_000.0
        ),
        "dewPoint",
        1.0
    );
    add_value_float!(
        fields,
        measurement.sensor_values.humidity_as_ppm(),
        "humidity",
        10000.0
    );
    add_value_float!(
        fields,
        measurement
            .sensor_values
            .absolute_humidity_as_grams_per_cubic_meter(
                measurement
                    .sensor_values
                    .temperature_as_millicelsius()
                    .unwrap_or_else(|| 999_999_999) as f32
                    / 1000.0
            ),
        "absoluteHumidity",
        1.0
    );
    add_value_float!(
        fields,
        measurement.sensor_values.pressure_as_pascals(),
        "pressure",
        1000.0
    );
    add_value_float!(
        fields,
        measurement.sensor_values.battery_potential_as_millivolts(),
        "batteryVoltage",
        1000.0
    );
    add_value_integer!(
        fields,
        measurement.sensor_values.tx_power_as_dbm(),
        "txPower"
    );
    if measurement.sensor_values.tx_power_as_dbm().is_none() { // Added
        if measurement.tx_power.is_none() {
            println!("No tx power found for mac {}", measurement.address);
        }
        add_value_integer!(
            fields,
            measurement.tx_power,
            "txPower"
        );
    }
    add_value_integer!(
        fields,
        measurement.sensor_values.movement_counter(),
        "movementCounter"
    );
    add_value_integer!(
        fields,
        measurement.sensor_values.measurement_sequence_number(),
        "measurementSequenceNumber"
    );
    add_value_float!(
        fields,
        measurement
            .sensor_values
            .pm25_as_10micrograms_per_cubicmeter(),
        "pm25",
        1.0
    );
    add_value_float!(
        fields,
        measurement.sensor_values.co2_as_ppm(),
        "co2",
        1.0
    );
    add_value_integer!(
        fields,
        measurement.sensor_values.get_dataformat(),
        "dataFormat"
    );
    add_value_integer!(
        fields,
        measurement.rssi,
        "rssi"
    );
    add_value_float!(
        fields,
        measurement.sensor_values.get_air_density_kg_per_m3(),
        "airDensity",
        1.0
    );

    add_value_float!(
        fields,
        measurement.sensor_values.saturation_vapor_pressure_as_hpa(),
        "equilibriumVaporPressure",
        0.01
    );

    if let Some(ref acceleration) = measurement.sensor_values.acceleration_vector_as_milli_g() {
        fields.insert(
            "accelerationX".to_string(),
            to_float!(acceleration.0, 1000.0),
        );
        fields.insert(
            "accelerationY".to_string(),
            to_float!(acceleration.1, 1000.0),
        );
        fields.insert(
            "accelerationZ".to_string(),
            to_float!(acceleration.2, 1000.0),
        );
    }

    fields
}

fn to_data_point(
    aliases: &BTreeMap<String, String>,
    name: String,
    measurement: &Measurement,
    options: &Options,
) -> DataPoint {
    DataPoint {
        measurement: name,
        tag_set: tag_set(aliases, &measurement, options),
        field_set: field_set(&measurement),
        timestamp: Some(SystemTime::now()),
    }
}

#[derive(Debug, Clone)]
pub struct Alias {
    pub address: String,
    pub name: String,
}

fn parse_alias(src: &str) -> Result<Alias, String> {
    let index = src.find('=');
    match index {
        Some(i) => {
            let (address, name) = src.split_at(i);
            Ok(Alias {
                address: address.to_string(),
                name: name.get(1..).unwrap_or("").to_string(),
            })
        }
        None => Err("invalid alias".to_string()),
    }
}

fn alias_map(aliases: &[Alias]) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for alias in aliases.iter() {
        map.insert(alias.address.to_string(), alias.name.to_string());
    }
    map
}

// Note! Some breaking changes done:
// - default value for influxdb_measurement has been changed slightly
// - added option to *keep* the colons in mac address. So by default the colons in mac address
//   will be removed (meaning AA:BB:CC:DD:EE will be converted to AABBCCDDEE)
#[derive(Parser, Debug, Clone)]
#[clap(author, about, rename_all = "kebab-case")]
struct Options {
    #[clap(long, default_value = "ruuvi_measurements")]
    /// The name of the measurement in InfluxDB line protocol.
    influxdb_measurement: String,
    #[clap(long, parse(try_from_str = parse_alias))]
    /// Specify human-readable alias for RuuviTag id. For example --alias DE:AD:BE:EF:00:00=Sauna.
    alias: Vec<Alias>,
    /// Verbose output, print parse errors for unrecognized data
    #[clap(short = 'v', long = "verbose")]
    verbose: bool,
    /// Do not strip the colons from the MAC address. The influxdb ruuvi database created with
    /// RuuviCollector strips the colons from the mac address.
    #[clap(short = 'm', long = "keep-mac-colons")]
    keep_mac_colons: bool,
    /// Comma separated list of which versions of the Ruuvi Sensor Data format to handle.
    /// If empty, all versions are handled.
    #[clap(long = "ruuvi-data-format-versions", use_value_delimiter = true)]
    data_format_versions: Vec<u8>,
}

async fn print_result_async(
    aliases: &BTreeMap<String, String>,
    name: &str,
    measurement: Measurement,
    http_client: Option<&Client>,
    options: &Options,
) {
    if options
        .data_format_versions
        .contains(&measurement.sensor_values.get_dataformat().unwrap())
        || options.data_format_versions.is_empty()
    {
        let datapoint = to_data_point(&aliases, name.to_string(), &measurement, options);
        match writeln!(std::io::stdout(), "{datapoint}",) {
            Ok(_) => (),
            Err(error) => {
                eprintln!("error: {}", error);
                ::std::process::exit(1);
            }
        }

        match http_client {
            Some(client) => {
                write_line_to_influx(client, datapoint.to_string()).await;
            }
            None => {
                println!("No http client set!");
                ::std::process::exit(1);
            }
        }
    }
}

#[tokio::main]
async fn listen(options: Options) -> Result<(), btleplug::Error> {
    let verbose = options.verbose;
    on_measurement(Box::new(move |result| match result {
        Ok(measurement) => {
            let name = options.influxdb_measurement.clone();
            let client = Client::new();
            let opt = options.clone();
            let aliases = alias_map(&options.alias);
            tokio::spawn(async move {
                print_result_async(&aliases, &name, measurement, Some(&client), &opt).await;
            });
        }
        Err(error) => {
            if verbose {
                eprintln!("{}", error)
            }
        }
    })).await
}

fn main() {
    panic::set_hook(Box::new(move |info: &PanicHookInfo| {
        eprintln!("Panic! {}", info);
        std::process::exit(0x2);
    }));
    let options = Options::parse();
    match listen(options) {
        Ok(_) => std::process::exit(0x0),
        Err(why) => {
            match why {
                PermissionDenied => println!("error: Permission Denied. Have you run setcap?"),
                _ => eprintln!("error: {}", why),
            }
            std::process::exit(0x1);
        }
    }
}