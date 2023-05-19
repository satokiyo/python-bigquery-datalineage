# Python Datalineage

A script that uses the Python SDK libraries for DataCatalog and DataLineage to trace the lineage of a specified Bigquery table and display the last updated time.

## zenn

https://zenn.dev/satokiyo/articles/20230406-python-datalineage

## setup

Please set value to the following variables defined in the Makefile

```makefile:Makefile
PROJECT_ID:=datalineage-demo
USER_ADDRESS:=<YOUR ADDRESS>
DATASET:=data_lineage_demo
BILLING_ACCOUNT_ID:=<YOUR ACCOUNT_ID>
```

Set up an environment with make commands

```bash:bash
make build-infra

# This will execute the following commands.
	# $(MAKE) create-project
	# $(MAKE) enable-billing-account
	# $(MAKE) enable-api
	# $(MAKE) add-iam-policy
	# $(MAKE) create-bq-table
```

You may also need to execute the following command.

```bash:bash
gcloud auth application-default login
```

# Run

Specify the table_id in the Makefile.

```makefile:Makefile
table_id:=$(PROJECT_ID).$(DATASET).total_green_trips_22_21
FQN=bigquery:$(table_id)
```

Display the upstream/downstream lineage tables and their last update timestamps.

```bash:bash
make run

# This will execute the following commands.
#  poetry run python src/main.py $(PROJECT_NO) $(LOCATION) $(FQN)
```

The output will be as follows.

```bash
start!!
fully_qualified_name: bigquery:datalineage-demo.data_lineage_demo.total_green_trips_22_21
--------------------
upstream source: fully_qualified_name: "bigquery:datalineage-demo.data_lineage_demo.tlc_green_trips_2021"
 (n=1)
create_time: 2023-04-06 17:18:07
update_time: 2023-04-06 17:18:07
--------------------
upstream source: fully_qualified_name: "bigquery:bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2021"
 (n=2)
create_time: 2022-09-14 13:11:36
update_time: 2022-09-14 13:11:36
--------------------
upstream source: fully_qualified_name: "bigquery:datalineage-demo.data_lineage_demo.tlc_green_trips_2022"
 (n=1)
create_time: 2023-04-06 17:18:10
update_time: 2023-04-06 17:18:10
--------------------
upstream source: fully_qualified_name: "bigquery:bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2022"
 (n=2)
create_time: 2022-09-14 13:11:54
update_time: 2022-09-14 13:11:54
```

## clean up

```bash:bash
make clean-infra
```
