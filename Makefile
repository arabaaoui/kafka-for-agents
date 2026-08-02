.PHONY: test-stack app all logs logs-test stop-app stop-test clean clean-all local-test monitor topics demo-1 demo-2 demo-3 check

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

# ---- Demo scripts ----

demo-1: test-stack
	@sleep 5
	docker compose -f docker-compose.app.yml up -d
	@echo ""
	@echo "=== Demo 1: Full Pipeline ==="
	@echo "Agents + simulator running. Watch logs: make logs"
	@echo "Kafka UI: http://localhost:8081"
	./scripts/demo-1-full-pipeline.sh

demo-2:
	@echo "=== Demo 2: Horizontal Scaling ==="
	./scripts/demo-2-scale.sh

demo-3:
	@echo "=== Demo 3: Crash Recovery ==="
	./scripts/demo-3-crash.sh

# ---- Pipeline health check ----

check:
	@echo "=== Pipeline Health Check ==="
	@echo ""
	@echo "--- Consumer Groups ---"
	@docker compose -f docker-compose.test.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9093 --list 2>/dev/null | while read group; do \
		echo ""; \
		echo "Group: $$group"; \
		docker compose -f docker-compose.test.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9093 --group "$$group" --describe 2>/dev/null | tail -n +3; \
	done
	@echo ""
	@echo "--- Topic Message Counts ---"
	@for topic in orders stocks anomalies tasks audit; do \
		count=$$(docker compose -f docker-compose.test.yml exec -T kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server kafka:9093 --topic $$topic --time -1 2>/dev/null | awk -F: '{sum+=$$NF} END{print sum+0}'); \
		echo "  $$topic: $$count messages"; \
	done
	@echo ""
	@echo "--- Recent Anomalies (last 10) ---"
	@docker compose -f docker-compose.app.yml logs --tail=200 2>/dev/null | grep -i "anomaly produced\|ANOMALIE" | tail -10 || echo "  (no anomalies yet)"
	@echo ""
	@echo "--- Recent Tasks (last 10) ---"
	@docker compose -f docker-compose.app.yml logs --tail=200 2>/dev/null | grep -i "task.*ACK\|task.*completed" | tail -10 || echo "  (no tasks yet)"
