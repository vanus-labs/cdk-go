# cdk-go for Vance Connectors
[![License](https://img.shields.io/badge/License-Apache_2.0-green.svg)](https://github.com/linkall-labs/cdk-go/blob/main/LICENSE)

The Go Connector-Development Kit (CDK) aims to help you build a new Vance connector in minutes.

In Vance, a connector is either a Source or a Sink.

A valid Vance Source generally:
- Retrieves data from real world data producers
- Transforms retrieved data into CloudEvents
- Delivers transformed CloudEvents to a HTTP target

And a valid Vance Sink generally:
- Retrieves CloudEvents via HTTP requests
- Uses retrieved CloudEvents in specific business logics