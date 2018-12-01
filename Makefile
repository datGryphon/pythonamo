PYCMD=python3
DATABASE=dynamo.py
CLIENT=client.py
PORT=13337
HOSTFILE=hostfile

run-db-docker: stop-docker
	docker-compose up machine1 machine2 machine3 machine4 machine5

stop-docker:
	docker-compose down

run-client-docker:
	docker-compose run client1

testcase:
	docker-compose run testcases
	make run-client-docker