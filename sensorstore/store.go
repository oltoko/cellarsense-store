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

package sensorstore

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	sensorBucket = "sensors"
	timeFormat   = time.RFC3339
)

type Store struct {
	db         *bolt.DB
	resolution time.Duration
	rootBucket string
}

type SensorValues struct {
	Temperature, Humidity float32
}

type TimedSensorValues struct {
	Timestamp time.Time
	Values    SensorValues
}

func New(db *bolt.DB, resolution time.Duration) (*Store, error) {

	log.Println("Initialize Sensor Store with bucket", sensorBucket)
	err := db.Update(func(tx *bolt.Tx) error {

		_, err := tx.CreateBucketIfNotExists([]byte(sensorBucket))

		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %s", sensorBucket, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Store{db: db, resolution: resolution, rootBucket: sensorBucket}, nil
}

func (s *Store) StoreValuesChannel(sensorID string) (chan SensorValues, error) {

	// create the Bucket for the specific sensorID
	err := s.db.Update(func(tx *bolt.Tx) error {

		root := tx.Bucket([]byte(s.rootBucket))

		_, err := root.CreateBucketIfNotExists([]byte(sensorID))

		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %s", sensorID, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// create the channel which can be used to store the values
	c := make(chan SensorValues)
	go s.storeValuesRoutine(c, sensorID)

	return c, nil
}

func (s *Store) storeValuesRoutine(c chan SensorValues, sensorID string) {

	log.Println("Starting routine for", sensorID)
	for {
		select {
		case values, open := <-c:

			if !open {
				log.Println("Stop storing values for", sensorID)
				return
			}

			s.StoreValues(sensorID, values)
		}
	}
}

func (s *Store) StoreValues(sensorID string, values SensorValues) error {

	err := s.db.Update(func(tx *bolt.Tx) error {

		root := tx.Bucket([]byte(s.rootBucket))

		b := root.Bucket([]byte(sensorID))

		now := time.Now()
		key := []byte(now.Truncate(s.resolution).Format(timeFormat))

		if value, err := json.Marshal(values); err != nil {
			return err
		} else if err := b.Put(key, value); err != nil {
			return err
		}

		return nil
	})

	return err
}

func (s *Store) ReadLastValue(sensorID string) (*TimedSensorValues, error) {

	var lastValue *TimedSensorValues

	err := s.db.View(func(tx *bolt.Tx) error {

		root := tx.Bucket([]byte(s.rootBucket))
		c := root.Bucket([]byte(sensorID)).Cursor()

		k, v := c.Last()

		result, err := convertToValues(k, v)
		lastValue = result

		return err
	})

	return lastValue, err
}

func (s *Store) ReadValues(sensorID string, duration time.Duration) ([]*TimedSensorValues, error) {

	now := time.Now()

	endTime := now.Truncate(s.resolution)
	var start []byte

	if duration > 0 {
		start = []byte(now.Add(-duration).Truncate(s.resolution).Format(timeFormat))
	} else {
		start = []byte(now.Add(duration).Truncate(s.resolution).Format(timeFormat))
	}

	valuesList := []*TimedSensorValues{}

	err := s.db.View(func(tx *bolt.Tx) error {

		root := tx.Bucket([]byte(s.rootBucket))
		c := root.Bucket([]byte(sensorID)).Cursor()

		for k, v := c.Seek(start); len(k) != 0; k, v = c.Next() {

			currentKey, err := time.Parse(timeFormat, string(k))
			if err != nil {
				return err
			}

			if currentKey.After(endTime) {
				break
			}

			values, err := convertToValues(k, v)
			if err != nil {
				return err
			}

			valuesList = append(valuesList, values)
		}

		return nil
	})

	return valuesList, err
}

func convertToValues(k []byte, v []byte) (*TimedSensorValues, error) {

	timestamp, err := time.Parse(timeFormat, string(k))
	if err != nil {
		return nil, err
	}

	values := SensorValues{}
	err = json.Unmarshal(v, &values)
	if err != nil {
		return nil, err
	}

	result := TimedSensorValues{Timestamp: timestamp, Values: values}

	return &result, nil
}
