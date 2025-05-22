# dental-data-pipeline

## Project Overview

**dental-data-pipeline** is a Python-based toolkit for secure, flexible, and cost-effective data transformation across databases. It provides libraries and Airflow DAGs for data copying, minimization, and sanitization, with a focus on sensitive data handling and ease of integration.

## Features

- **Data Copying & Synchronization:** Move and sync data between databases using Airflow DAGs and Python scripts.
- **Data Minimization & Sanitization:** Tools for reducing and cleaning sensitive data, including obfuscation and masking.
- **Support for Non-Relational Data:** Handles JSON fields within PostgreSQL.
- **Customizable Workflows:** Easily extend or modify data transformation logic.
- **Open Source:** Freely available for use and contribution.

## Tech Stack

- **Ubuntu Linux:** The project is developed and tested on Ubuntu, ensuring compatibility and leveraging the stability of this OS for data engineering workflows.
- **Python:** Core language for all libraries and scripts.
- **Apache Airflow:** Orchestrates ETL workflows via DAGs (`dags/`, `ddp_airflow/dags/`).
- **Docker:** Containerized deployment and orchestration (`docker-compose.yaml`).
- **PostgreSQL:** Example target for relational and semi-structured data.
- **CSV:** Example data source (`bank_data/bank.csv`).

## Project Structure

```
.  
├── airflow/                # Airflow configuration and DAGs
├── bank_data/              # Example CSV data
├── dags/                   # Airflow DAGs (root-level)
├── ddp_airflow/            # Main package: config, DAGs, src
│   ├── config/             # Configuration files (e.g., dbm_config.json)
│   ├── dags/               # Airflow DAGs: copy_db_dag.py, minimization_dag.py, test_dag.py
│   └── src/                # Core Python modules: clean.py, copy_file.py, create_tables.py, minimize_data.py
├── docker-compose.yaml     # Docker orchestration
├── setup.py                # Python package setup
└── README.md               # Project documentation
```

## Getting Started

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/dental-data-pipeline.git
   cd dental-data-pipeline
   ```

2. **Build and start with Docker:**
   ```bash
   docker-compose up --build
   ```

3. **Configure Airflow:**
   - Place your DAGs in `dags/` or `ddp_airflow/dags/`.
   - Update database connection settings in `ddp_airflow/config/dbm_config.json`.

4. **Run Data Pipelines:**
   - Use the Airflow UI to trigger DAGs for data copying or minimization.
   - Example DAGs: `copy_db_dag.py`, `minimization_dag.py`.

## Example Usage

- **Copy data between databases:**  
  Trigger the `copy_db_dag` in Airflow to copy data as configured.
- **Minimize sensitive data:**  
  Use the `minimization_dag` to sanitize and obfuscate sensitive fields.

## Development Challenges

- **DAG Dependency Structure:**
  Designing the structure of DAG dependencies in Airflow was a key challenge. It required careful planning to ensure tasks executed in the correct order and that dependencies were clear and maintainable.

- **Airflow Imports and Organization:**
  Organizing imports for Airflow, especially when splitting logic across multiple files and packages, was non-trivial. Ensuring that all modules were discoverable by Airflow and that import paths were correct took several iterations.

- **Linux Environment:**
  Developing on Ubuntu provided a stable base, but required attention to permissions, environment variables, and compatibility with Docker and Airflow.

## Contributing

Contributions are welcome! Please open issues or submit pull requests for improvements or new features.

## License

This project is open-source and free to use under the MIT License.
