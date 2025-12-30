use btleplug::api::{BDAddr, Central, CentralEvent, Manager as _, Peripheral, ScanFilter};
use btleplug::platform::{Adapter, PeripheralId};
use ruuvi_sensor_protocol::{ParseError, SensorValues};
use btleplug::api;
use futures::stream::StreamExt;

// Measurement from RuuviTag sensor
#[derive(Debug)]
pub struct Measurement {
    pub address: BDAddr,
    pub tx_power: Option<i16>,
    pub rssi: Option<i16>,
    pub sensor_values: SensorValues,
}
const MANUFACTURER_DATA_ID: u16 = 0x0499;
trait ToSensorValue {
    async fn to_sensor_value(self) -> Result<SensorValues, ParseError>;
}

impl<T: api::Peripheral> ToSensorValue for T {
    async fn to_sensor_value(self) -> Result<SensorValues, ParseError> {
        match self.properties().await {
            Ok(prop) => {match prop {
                Some(data) => {
                    if data.manufacturer_data.contains_key(&MANUFACTURER_DATA_ID) {
                        from_manufacturer_data(&data.manufacturer_data[&MANUFACTURER_DATA_ID])
                    } else {
                        Err(ParseError::UnknownManufacturerId(0))
                    }
                }
                None => {Err(ParseError::EmptyValue)}
            }},
            Err(_) => {Err(ParseError::EmptyValue)}
        }
    }
}

fn from_manufacturer_data(data: &[u8]) -> Result<SensorValues, ParseError> {
    if data.len() > 2 {
        SensorValues::from_manufacturer_specific_data(&data)
    } else {
        Err(ParseError::EmptyValue)
    }
}

async fn on_event_with_address(
    central: &Adapter,
    address: &PeripheralId,
) -> Option<Result<Measurement, ParseError>> {
    match central.peripheral(address).await {
        Ok(peripheral) => {
            let address = peripheral.address();
            let properties = peripheral.properties().await.ok()??;
            let rssi = properties.rssi;
            let tx_power = properties.tx_power_level;

            match peripheral.to_sensor_value().await {
                Ok(sensor_values) => Some(Ok(Measurement {
                    address,
                    rssi,
                    tx_power,
                    sensor_values,
                })),
                Err(error) => Some(Err(error)),
            }
        }
        Err(_) => Some(Err(ParseError::EmptyValue))
    }
}

async fn on_event(
    central: &Adapter,
    event: CentralEvent,
) -> Option<Result<Measurement, ParseError>> {
    match event {
        CentralEvent::DeviceDiscovered(address) => { on_event_with_address(central, &address).await },
        CentralEvent::DeviceUpdated(address) => on_event_with_address(central, &address).await,
        CentralEvent::DeviceConnected(_) => None,
        CentralEvent::DeviceDisconnected(_) => None,
        CentralEvent::ManufacturerDataAdvertisement { .. } => {None}
        CentralEvent::ServiceDataAdvertisement { .. } => {None}
        CentralEvent::ServicesAdvertisement { .. } => {None}
        CentralEvent::StateUpdate(_) => {None}
    }
}

// Stream of RuuviTag measurements that gets passed to the given callback. Blocks and never stops.
pub async fn on_measurement(
    f: Box<dyn Fn(Result<Measurement, ParseError>) + Send>,
) -> Result<(), btleplug::Error> {
    let manager : btleplug::platform::Manager = btleplug::platform::Manager::new().await?;

    // get bluetooth adapter
    let adapters = manager.adapters().await?;

    let adapter : Adapter = adapters
        .into_iter()
        .next()
        .expect("Bluetooth adapter not available");

    let mut events = adapter.events().await?;

    adapter.start_scan(ScanFilter::default()).await?;

    while let Some(event) = events.next().await {
        if let Some(result) = on_event(&adapter, event).await {
            f(result)
        }
    }

    Err(btleplug::Error::NotSupported(String::from("No events received")))
}
