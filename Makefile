.PHONY: test-stack app all logs logs-test stop-app stop-test clean clean-all local-test monitor topics

local-test:
	python tests/test_deterministic_flow.py

test-stack:
	docker network create kafka-retail-test 2>/dev/null || true
	docker compose -f docker-compose.test.yml up -d
	@echo "Test Kafka ready on port 9093"
	@echo "Kafka UI ready at http://localhost:8081"

app:
	docker compose -f docker-compose.app.yml up -d

all: test-stack
	@sleep 5
	docker compose -f docker-compose.app.yml up -d

logs:
	docker compose -f docker-compose.app.yml logs -f

logs-test:
	docker compose -f docker-compose.test.yml logs -f

stop-app:
	docker compose -f docker-compose.app.yml down

stop-test:
	docker compose -f docker-compose.test.yml down

clean:
	docker compose -f docker-compose.test.yml down -v
	docker compose -f docker-compose.app.yml down

clean-all: clean
	docker network rm kafka-retail-test 2>/dev/null || true

monitor:
	docker compose -f docker-compose.app.yml logs -f & \
	docker compose -f docker-compose.test.yml logs -f kafka-ui & \
	wait

topics:
	docker compose -f docker-compose.test.yml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9093 --describe
