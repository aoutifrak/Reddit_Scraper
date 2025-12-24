# Reddit User Scraper Pipeline - Service Management
# Usage: make install && make start


SHELL := /bin/bash
SERVICE_NAME := reddit-scraper
SERVICE_FILE := /etc/systemd/system/$(SERVICE_NAME).service
SCRIPT_DIR := $(shell pwd)
PYTHON := $(shell which python3)
CURRENT_USER := $(shell whoami)

.PHONY: all install uninstall start stop restart status logs enable disable clean vpn-up vpn-down vpn-status test help

all: help

help:
	@echo "Reddit User Scraper - Service Management"
	@echo ""
	@echo "Usage:"
	@echo "  make install     - Install systemd service"
	@echo "  make uninstall   - Remove systemd service"
	@echo "  make start       - Start the scraper service"
	@echo "  make stop        - Stop the scraper service"
	@echo "  make restart     - Restart the scraper service"
	@echo "  make status      - Show service status"
	@echo "  make logs        - Follow service logs"
	@echo "  make logs-full   - Show full service logs"
	@echo "  make enable      - Enable service on boot"
	@echo "  make disable     - Disable service on boot"
	@echo ""
	@echo "  make vpn-up      - Start Gluetun VPN container"
	@echo "  make vpn-down    - Stop Gluetun VPN container"
	@echo "  make vpn-status  - Check VPN status and IP"
	@echo "  make vpn-logs    - Show VPN container logs"
	@echo ""
	@echo "  make test        - Run quick test (5 subs, single-pass)"
	@echo "  make test-ssh    - Test SSH upload configuration"
	@echo "  make clean       - Remove output files (keeps dedup state)"
	@echo "  make clean-all   - Remove ALL data including dedup state"
	@echo "  make config      - Show current configuration"

install: create-service
	@sudo systemctl daemon-reload
	@sudo systemctl enable $(SERVICE_NAME)
	@echo ""
	@echo "✅ Service installed successfully!"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Configure SSH upload in .env (optional)"
	@echo "  2. Run: make vpn-up    (start VPN)"
	@echo "  3. Run: make start     (start scraper)"
	@echo "  4. Run: make logs      (view logs)"

create-service:
	@echo "Creating systemd service file..."
	@echo '[Unit]' | sudo tee $(SERVICE_FILE) > /dev/null
	@echo 'Description=Reddit User Scraper Pipeline' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'After=network.target docker.service' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'Requires=docker.service' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo '' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo '[Service]' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'Type=simple' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'User=$(CURRENT_USER)' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'Group=docker' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'WorkingDirectory=$(SCRIPT_DIR)' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'EnvironmentFile=$(SCRIPT_DIR)/.env' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'ExecStart=$(PYTHON) $(SCRIPT_DIR)/pipeline.py --output-dir $(SCRIPT_DIR)/pipeline_output' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'Restart=always' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'RestartSec=60' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'TimeoutStopSec=30' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'StandardOutput=journal' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'StandardError=journal' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'SyslogIdentifier=$(SERVICE_NAME)' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo '' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo '[Install]' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo 'WantedBy=multi-user.target' | sudo tee -a $(SERVICE_FILE) > /dev/null
	@echo "Service file created at $(SERVICE_FILE)"

uninstall: stop
	@echo "Removing service..."
	@sudo systemctl disable $(SERVICE_NAME) 2>/dev/null || true
	@sudo rm -f $(SERVICE_FILE)
	@sudo systemctl daemon-reload
	@echo "✅ Service uninstalled"

start:
	@echo "Starting $(SERVICE_NAME)..."
	@sudo systemctl start $(SERVICE_NAME)
	@sleep 2
	@sudo systemctl status $(SERVICE_NAME) --no-pager

stop:
	@echo "Stopping $(SERVICE_NAME)..."
	@sudo systemctl stop $(SERVICE_NAME) 2>/dev/null || true
	@echo "✅ Service stopped"

restart:
	@echo "Restarting $(SERVICE_NAME)..."
	@sudo systemctl restart $(SERVICE_NAME)
	@sleep 2
	@sudo systemctl status $(SERVICE_NAME) --no-pager

status:
	@sudo systemctl status $(SERVICE_NAME) --no-pager || true
	@echo ""
	@echo "--- Quick Stats ---"
	@echo -n "Unique users collected: " && wc -l < pipeline_output/dedup_users.txt 2>/dev/null || echo "0"
	@echo -n "Subreddits processed: " && jq '.processed | length' pipeline_output/dedup_subreddits.json 2>/dev/null || echo "0"
	@echo -n "Chunk files created: " && ls pipeline_output/users/*.csv 2>/dev/null | wc -l || echo "0"

enable:
	@sudo systemctl enable $(SERVICE_NAME)
	@echo "✅ Service enabled (will start on boot)"

disable:
	@sudo systemctl disable $(SERVICE_NAME)
	@echo "✅ Service disabled (won't start on boot)"

logs:
	@sudo journalctl -u $(SERVICE_NAME) -f

logs-full:
	@sudo journalctl -u $(SERVICE_NAME) --no-pager

vpn-up:
	@echo "Starting Gluetun VPN..."
	@docker-compose up -d gluetun
	@echo "Waiting for VPN to connect..."
	@sleep 10
	@$(MAKE) vpn-status

vpn-down:
	@echo "Stopping Gluetun VPN..."
	@docker stop gluetun 2>/dev/null || true
	@echo "✅ VPN stopped"

vpn-status:
	@echo "--- VPN Status ---"
	@docker ps --filter name=gluetun --format "Container: {{.Names}} | Status: {{.Status}}" 2>/dev/null || echo "Container not running"
	@echo ""
	@echo "Public IP:"
	@curl -s -x http://localhost:8888 http://httpbin.org/ip 2>/dev/null | jq -r '.origin' || echo "VPN not reachable"

vpn-logs:
	@docker logs gluetun --tail 50 -f

test:
	@echo "Running quick test (5 subreddits, single-pass)..."
	@$(PYTHON) pipeline.py --batch-size 5 --posts-per-sub 10 --single-pass --output-dir ./test_output

test-ssh:
	@echo "Testing SSH connection..."
	@. ./.env 2>/dev/null; \
	if [ -z "$$SSH_HOST" ] || [ -z "$$SSH_USER" ]; then \
		echo "❌ SSH_HOST and SSH_USER not set in .env"; \
		exit 1; \
	fi; \
	echo "Connecting to $$SSH_USER@$$SSH_HOST..."; \
	ssh -o BatchMode=yes -o ConnectTimeout=5 "$$SSH_USER@$$SSH_HOST" "echo '✅ SSH connection successful'" || echo "❌ SSH connection failed"

clean:
	@echo "Cleaning output files (keeping dedup state)..."
	@rm -rf pipeline_output/users/*.csv pipeline_output/users/*.json 2>/dev/null || true
	@rm -rf test_output 2>/dev/null || true
	@echo "✅ Cleaned"

clean-all:
	@echo "⚠️  This will delete ALL data including collected users!"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] && rm -rf pipeline_output test_output test_ssh_output test_dedup && echo "✅ All data removed" || echo "Cancelled"

config:
	@echo "--- Current Configuration ---"
	@echo "Script directory: $(SCRIPT_DIR)"
	@echo "Python: $(PYTHON)"
	@echo "User: $(CURRENT_USER)"
	@echo ""
	@echo "--- Environment (.env) ---"
	@grep -E "^(SSH_|GLUETUN_|VPN_)" .env 2>/dev/null | grep -v PASSWORD || echo "No relevant vars in .env"
	@echo ""
	@echo "--- Data Status ---"
	@ls -lh pipeline_output/dedup_users.txt 2>/dev/null || echo "Users file: not found"
	@ls -lh pipeline_output/dedup_subreddits.json 2>/dev/null || echo "Subreddits file: not found"
