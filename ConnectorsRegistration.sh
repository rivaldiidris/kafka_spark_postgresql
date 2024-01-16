curl -X POST -H "Content-Type: application/json" --data @postgres-sink-config.json http://localhost:8083/connectors

curl http://localhost:8083/connectors/postgres-sink-test/status

curl -X DELETE http://localhost:8083/connectors/postgres-sink-test