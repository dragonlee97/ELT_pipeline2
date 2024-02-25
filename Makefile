prepare:
	mkdir -p ./logs ./plugins
	docker compose up airflow-init

run-airflow:
	docker compose -f ./docker-compose.yaml build airflow-worker airflow-scheduler airflow-webserver
	docker compose -f ./docker-compose.yaml up airflow-worker airflow-scheduler airflow-webserver --remove-orphans

run-minio:
	docker compose -f ./docker-compose.yaml up minio mc

run:
	docker compose up -d

stop:
	docker compose down --volumes --rmi all
