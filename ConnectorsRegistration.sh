curl -X POST -H "Content-Type: application/json" --data @postgres-sink-config.json http://161.97.89.248:8083/connectors

curl http://161.97.89.248:8083/connectors/postgres-sink/status

curl -X DELETE http://localhost:8083/connectors/postgres-sink-test