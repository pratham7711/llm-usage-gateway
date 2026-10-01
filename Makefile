COMPOSE = docker compose -f deploy/docker-compose.yml
MODULES = gateway metering mock-upstream
OTEL_VERSION = 2.31.1

.PHONY: build test images otel-agent up down logs bench k3d-up k3d-down

build:
	mvn -B -q -DskipTests package

test:
	mvn -B verify

otel-agent: deploy/otel/opentelemetry-javaagent.jar

deploy/otel/opentelemetry-javaagent.jar:
	mkdir -p deploy/otel
	curl -fsSL -o $@ https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v$(OTEL_VERSION)/opentelemetry-javaagent.jar

images: build otel-agent
	for m in $(MODULES); do docker build -q --build-arg MODULE=$$m -t llmgw/$$m:dev . ; done

up: images
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f gateway metering

bench:
	TRACE_SAMPLE_RATIO=0.01 python3 loadtest/bench.py steps
	python3 loadtest/bench.py overload
	python3 loadtest/bench.py consumer-kill
	python3 loadtest/bench.py broker-restart
	python3 results/build_page.py

k3d-up: images
	./deploy/k8s/k3d-up.sh

k3d-down:
	k3d cluster delete llmgw
