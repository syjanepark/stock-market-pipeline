.PHONY: help setup install terraform-init terraform-apply airflow-init airflow-start dashboard clean

help:
	@echo "Stock Market Pipeline - Quick Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install         - Install all dependencies"
	@echo "  make terraform-init  - Initialize Terraform"
	@echo "  make terraform-apply - Deploy AWS infrastructure"
	@echo "  make airflow-init    - Setup Airflow"
	@echo ""
	@echo "Run:"
	@echo "  make airflow-start   - Start Airflow (http://localhost:8080)"
	@echo "  make dashboard       - Start Dashboard (http://localhost:8501)"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean           - Remove temp files"

install:
	@echo "Installing dependencies..."
	python3 -m venv venv
	./venv/bin/pip install -r requirements.txt
	@echo "✅ Dependencies installed!"

terraform-init:
	@echo "Initializing Terraform..."
	cd terraform && terraform init

terraform-apply:
	@echo "Deploying AWS infrastructure..."
	cd terraform && terraform apply

airflow-init:
	@echo "Initializing Airflow..."
	./venv/bin/airflow db init
	./venv/bin/airflow users create --username admin --password admin \
		--firstname Jane --lastname Park --role Admin \
		--email janesypark122@gmail.com
	mkdir -p ~/airflow/dags
	cp airflow/dags/stock_market_pipeline_dag.py ~/airflow/dags/
	@echo "✅ Airflow ready!"

airflow-start:
	@echo "Starting Airflow..."
	./venv/bin/airflow scheduler &
	./venv/bin/airflow webserver --port 8080

dashboard:
	@echo "Starting Streamlit dashboard..."
	./venv/bin/streamlit run dashboard/app.py

clean:
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "✅ Clean!"
