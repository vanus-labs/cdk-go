package main

import "linkall.com/cdk-go/v1/connector"

func main() {
	go connector.RunSource("MySource", CreateSource)
	connector.RunSink("EventDisplay", CreateSink)

}
