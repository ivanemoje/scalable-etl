up:
	docker compose up -d --build

down:
	docker compose down

volumes:
	docker compose down --volumes

.PHONY: up down volumes