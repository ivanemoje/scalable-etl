# Listen Brainz Analysis

## Task

visit [scalable.anynameworks.com](scalable.anynameworks.com) for my deep dive technical review of this task.

The task is linked [here](task/scalable-test.pdf)

## Getting started

### Clone

Clone the [repo](https://github.com/ivanemoje/scalable-etl)

```bash
git clone git@github.com:ivanemoje/scalable-etl.git
```

### Deployment

#### local installation

You need to install the required dependencies in `requirements.txt`

Running `pip install -r requirements.txt` will install them.

> **_NOTE:_** Make sure to have spark set locally before running below.

Running the pytest is easy. You need to run `python -m pytest` and you're good to go.

#### Docker

To launch the Spark and Iceberg Docker containers, run:

```bash
make up
```

Or `docker compose up` if you're on Windows!

Then, you should be able to access a Jupyter notebook at []`localhost:8889`](http://127.0.0.1:8889/tree?) (or what environment vairbale you set)

The notebook to be able to run is the `analysis.ipynb` inside the `notebooks` folder.

###### Infrastructure Port Mapping

| Service | Localhost Port | Container Port | Description |
| :--- | :--- | :--- | :--- |
| **Jupyter Notebook** | `8889` | `8888` | Spark-Iceberg Notebook environment |
| **Spark Master** | `8081` | `8080` | Spark Master Web UI |
| **Spark History** | `10002` | `18080` | Spark History Server |
| **Spark Executor UIs**| `4043-4045` | `4040-4042` | Application-level monitoring |
| **Iceberg REST** | `8182` | `8181` | Iceberg REST Catalog API |
| **MinIO API** | `9100` | `9000` | S3-compatible API |
| **MinIO Console** | `9101` | `9001` | MinIO Browser UI |

---

## Connection Details

- **Jupyter Lab:** [http://localhost:8889](http://localhost:8889)
- **MinIO UI:** [http://localhost:9101](http://localhost:9101)
- **Spark UI:** [http://localhost:8081](http://localhost:8081)
- **Iceberg Catalog Endpoint:** `http://localhost:8182`
- **S3 Endpoint (MinIO):** `http://localhost:9100`

## Credentials

Review the .env.example file

##### Environment Reset

To clear your environment and reset Docker volumes, use:
```bash
make volumes
```

#### Deployment

The pipeline uses Github CI/CD pipelines (alternatives such as AWS Codepipline/Azure DevOps).

To deploy to production, create the infrastructure using the `Terraform` section

### IAM User

#### Terraform

```bash
make terraform
```

This will create a glue job,

you will need to use the source code in `src/ingest_job_glue.py`, `src/transform_job_glue.py`, `src/daily_job_glue.py`.

The IAM user `scalable` has been created with least priviledged access.

Currently, policy

```json
```

