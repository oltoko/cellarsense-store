// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oltoko/cellarsense-store/sensorstore"
	"github.com/oltoko/go-am2320"

	bolt "go.etcd.io/bbolt"
)

const (
	dbName         = "cellarsense.db"
	sensorID       = "test"
	entries        = 100
	timeFormat     = time.RFC3339
	timeResolution = time.Minute * 10
)

func main() {

	log.Println("Opening Database", dbName)
	db, err := bolt.Open(dbName, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	log.Println("Creating Store")
	store, err := sensorstore.New(db, timeResolution)
	if err != nil {
		log.Fatalln(err)
	}

	c, err := store.StoreValuesChannel(sensorID)
	if err != nil {
		log.Fatalln(err)
	}

	ticker := time.NewTicker(timeResolution)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	readSensorValues(c, ticker, signals)
}

func readSensorValues(c chan sensorstore.SensorValues, ticker *time.Ticker, signals chan os.Signal) {

	sensor := am2320.Create(am2320.DefaultI2CAddr)

	for {
		select {
		case <-ticker.C:

			values, err := sensor.Read()
			if err != nil {
				log.Fatalln("Failed to read sensor values", err)
			}

			c <- sensorstore.SensorValues{Temperature: values.Temperature, Humidity: values.Humidity}

		case s := <-signals:
			log.Println("Received Signal", s, "shutting down!")
			return
		}
	}
}
