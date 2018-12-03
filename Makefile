PYCMD=python3
DATABASE=dynamo.py
CLIENT=client.py
PORT=13337
HOSTFILE=hostfile

clean:
	rm -f *.ring
	rm -f *.db
	rm -f *.pickle

run-db-docker: stop-docker clean
	docker-compose up machine1 machine2 machine3 machine4 machine5 machine6 machine7 machine8 machine9

stop-docker:
	docker-compose down

run-client-docker:
	docker-compose run client1

testcase:
	docker-compose run testcases
	make run-client-docker
