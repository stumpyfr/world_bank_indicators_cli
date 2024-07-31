#!/bin/sh

DATE="2023:2020"
json_data=$(cat create.json)

echo "$json_data" | jq -c '.[]' | while read item; do
  id=$(echo $item | jq -r '.id')
  name=$(echo $item | jq -r '.name')

  echo "Downloading data for indicator: $id\t$name and date: $DATE"
  go run main.go dl -t $DATE -i $id -d output.db --csv $id.csv --nb_per_page 50
done
