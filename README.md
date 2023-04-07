# Python Datalineage

DataCatalog と DataLineage の Python SDK ライブラリ経由で、指定した Bigquery テーブルのリネージをたどり、最終更新時間を表示するスクリプト

## zenn

https://zenn.dev/satokiyo/articles/20230406-python-datalineage

## 環境準備

Makefile 内で定義した変数を適当な値に変更

```makefile:Makefile
PROJECT_ID:=datalineage-demo
USER_ADDRESS:=<YOUR ADDRESS>
DATASET:=data_lineage_demo
BILLING_ACCOUNT_ID:=<YOUR ACCOUNT_ID>
```

make コマンドで環境を構築

```bash:bash
make build-infra

# 以下の処理を実行する
  # デモ用プロジェクト作成
  # 請求先アカウントとの紐づけ（リネージグラフを見るために必要）
  # API有効化
  # IAMロール付与
  # 検証用BigQueryのテーブル作成
```

# 実行

Makefile に定義した変数 FQN で、作成したテーブルを指定

```makefile:Makefile
FQN:=bigquery:datalineage-demo.data_lineage_demo.total_green_trips_22_21
```

指定したテーブルの上流/下流のリネージテーブルと、その最終更新タイムスタンプを表示

```bash:bash
make run

#  実行されるコマンド
#  poetry run python src/main.py $(PROJECT_NO) $(LOCATION) $(FQN)
```

出力は以下のようになる。

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
