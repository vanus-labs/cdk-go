/*
Copyright 2022-Present The Vance Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"github.com/linkall-labs/cdk-go/connector"
)

func main() {
	//The following codes run a HttpSource which retrieve http requests and
	//transform requests into standard CloudEvents. It finally delivers those
	//CloudEvents to the EventDisplaySink.
	//To test this example, you can send Http requests to http://localhost:8080

	//The EventDisplaySink simply receives CloudEvents and prints them.

	go connector.RunSource("HttpSource", CreateSource)

	connector.RunSink("EventDisplay", CreateSink)

}
