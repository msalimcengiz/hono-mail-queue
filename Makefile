NODE := docker run --rm -v $(CURDIR):/app -w /app node:24-alpine
NODE_NET := docker run --rm --network mailq_default -v $(CURDIR):/app -w /app \
	-e API_URL=http://api:3000 -e MAILPIT_URL=http://mailpit:8025 \
	-e DATABASE_URL=postgres://mail:mail@postgres:5432/mail node:24-alpine

.PHONY: demo install up down check test test-integration bench chaos k8s-validate

demo:             ## Guided tour: starts the stack and checks each behaviour (~2 min)
	./demo/run.sh

# Dependencies are installed inside a Node container on first use.
node_modules: package-lock.json
	$(NODE) npm ci --no-fund --no-audit
	@touch node_modules

install: node_modules

up:               ## API :3000, 2 workers, Postgres, RabbitMQ (UI :15672), Mailpit (UI :8025)
	docker compose up -d --build --wait

down:
	docker compose --profile observability down -v

check: node_modules  ## typecheck + lint + unit tests
	$(NODE) sh -c 'npm run typecheck && npm run lint && npm test'

test: check

test-integration: node_modules up
	$(NODE_NET) npm run test:integration

bench: node_modules up         ## 20k bulk + 300 transactional, 10% SMTP failures
	RATE_BULK=500 docker compose up -d --wait worker
	$(NODE_NET) sh -c 'BULK=20000 TX=300 CHAOS=10 npx tsx bench/run.ts'

chaos: node_modules  ## workers, API and RabbitMQ SIGKILLed under load; every email checked
	./bench/chaos.sh

k8s-validate:
	docker run --rm -v $(CURDIR)/deploy/k8s:/k registry.k8s.io/kubectl:v1.34.1 kustomize /k > rendered.yaml
	docker run --rm -v $(CURDIR)/rendered.yaml:/r.yaml ghcr.io/yannh/kubeconform:latest -strict -summary /r.yaml
