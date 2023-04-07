#!/bin/bash

PROJECT_ID=$1
DATASET=$2

bq mk $DATASET
bq query --use_legacy_sql=false \
	"CREATE TABLE $PROJECT_ID.$DATASET.tlc_green_trips_2021	COPY bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2021"
bq query --use_legacy_sql=false \
	"CREATE TABLE $PROJECT_ID.$DATASET.tlc_green_trips_2022	COPY bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2022"
bq query --use_legacy_sql=false \
	"CREATE TABLE $PROJECT_ID.$DATASET.total_green_trips_22_21 \
		AS SELECT vendor_id, COUNT(*) AS number_of_trips \
		FROM ( \
		SELECT vendor_id FROM $PROJECT_ID.$DATASET.tlc_green_trips_2022 \
		UNION ALL \
		SELECT vendor_id FROM $PROJECT_ID.$DATASET.tlc_green_trips_2021 \
		) \
		GROUP BY vendor_id \
	"
