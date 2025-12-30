# RuuviTag Listener

A command-line client to listen to [RuuviTag](https://ruuvi.com) sensor measurements over Bluetooth LE and output as [InfluxDB line protocol](https://docs.influxdata.com/influxdb/v1.7/write_protocols/line_protocol_reference/).

## Fork info
Note that this fork contains some (breaking) modifications from the original to fit more to my needs. Use with your own risk.

It might be important to notice that this fork also tries to insert the lines into InfluxDB by default which might not be desirable for you.

## Requirements

* RuuviTag Bluetooth sensor or Ruuvi Air Indoor Air Quality Monitor
* Linux with BlueZ Bluetooth stack

## Usage
**Before running the program see the [Fork info](#fork-info) section.**
```sh
ruuvitag-listener
```

Running `ruuvitag-listener` will output measurements to STDOUT until interrupted.

Example output:

```
ruuvi_measurement,name=F7:2A:60:0D:6E:1E acceleration_x=-0.055,acceleration_y=-0.032,acceleration_z=0.998,battery_potential=3.007,humidity=19.5,pressure=101.481,temperature=19.63 1546681652675044272
ruuvi_measurement,name=F1:FC:AA:80:4E:59 acceleration_x=0.005,acceleration_y=0.015,acceleration_z=1.036,battery_potential=2.989,humidity=17.5,pressure=101.536,temperature=21.97 1546681653451240083
ruuvi_measurement,name=F1:FC:AA:80:4E:59 acceleration_x=0.002,acceleration_y=0.017,acceleration_z=1.032,battery_potential=2.977,humidity=17.5,pressure=101.536,temperature=21.97 1546681654458923308
ruuvi_measurement,name=F7:2A:60:0D:6E:1E acceleration_x=-0.052,acceleration_y=-0.032,acceleration_z=1,battery_potential=3.013,humidity=19.5,pressure=101.481,temperature=19.63 1546681655691300729
```

You can also define the InfluxDB measurement name or aliases using command line arguments. For example

```sh
ruuvitag-listener --influxdb-measurement=ruuvi --alias F1:FC:AA:80:4E:59=Indoor --alias F7:2A:60:0D:6E:1E=Outdoor
```

```
ruuvi,name=Indoor acceleration_x=0,acceleration_y=0.017,acceleration_z=1.027,battery_potential=2.989,humidity=17.5,pressure=101.54,temperature=21.97 1546681957964524841
ruuvi,name=Outdoor acceleration_x=-0.054,acceleration_y=-0.032,acceleration_z=1.005,battery_potential=3.013,humidity=83.5,pressure=101.487,temperature=-5.63 1546681958085455294
```

All options can be listed with `ruuvitag-listener --help`.

## Development

Use [cargo](https://doc.rust-lang.org/stable/cargo/) to build the project to target/debug directory:

```sh
cargo build
```

Tests can be run with

```sh
cargo test
```

## License

MIT
