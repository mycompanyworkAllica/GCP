To use cloud composer,
enable apis:
    "composer.googleapis.com",
    "workflows.googleapis.com",

### Cloud Composer ###
This is a managed Apache Airflow where the infrastructure is managed by google cloud.
### Apache Airflow ###
This is an open-source platform used to ochestrate complex workflows and data pipelines.
[Key features]
DAGs(Directed Acyclic Graphs): YOU DEFINE EACH WORKFLOWS AS DAGS, where each Node is a task and edges represent dependencies.
Python-based: workflows are written in python
Scheduling, you can run tasks at regular intervals or trigger them manually.
Tasks: this is  a step in the workflow
executor: runs the tasks
web ui: dashboard to monitor workflows.

### Cloud COmposer COmponents ###

>>> Scheduler
Function: The scheduler is the brain of Airflow. It parses DAGs (Directed Acyclic Graphs), determines task dependencies, and schedules tasks to run at the right time.

Key Role: It places tasks into the queue based on their schedule and dependencies.

Behavior: Continuously monitors DAGs and triggers tasks when conditions are met.

>>> DAG Processor
Function: This component parses and processes DAG files to ensure they are syntactically correct and ready for scheduling.

Key Role: Separates DAG parsing from the scheduler to improve performance and scalability.

Behavior: Reads Python DAG files, validates them, and sends metadata to the database.

>>> Worker
Function: Workers execute the actual tasks defined in your DAGs.

Key Role: They pull tasks from the queue and run them in isolated environments.

Behavior: Can scale horizontally to handle more concurrent tasks. Each worker runs tasks independently, often in Docker containers or Kubernetes pods.

>>> Web Server
Function: Provides the user interface for Airflow.

Key Role: Allows users to monitor DAGs, trigger tasks manually, view logs, and manage workflows.

Behavior: Runs a Flask-based UI that interacts with the Airflow metadata database.

>>> Triggerer
Function: Handles asynchronous task triggers, especially useful for sensors and event-driven workflows.

Key Role: Efficiently waits for external events (like file arrival or API response) without blocking resources.

Behavior: Uses async I/O to monitor and trigger tasks based on external conditions.

................ How They Work Together .......................................
DAG Processor parses your workflow definitions.

Scheduler decides when tasks should run.

Triggerer waits for external events if needed.

Workers execute the tasks.

Web Server lets you interact with everything visually.

### Creating Cloud composer envirronment ###
link: https://cloud.google.com/composer/docs/composer-3/create-environments#terraform
using shared vpcs: https://cloud.google.com/composer/docs/composer-3/configure-shared-vpc
dag google operators: https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/index.html
dag google transfer operators: https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/transfer/index.html
The service Account used by terraform must have composer.environments.create role
the service account to be used by cloud composer should have: composer.worker role. It would also require other roles based on the DAGs. dags could task composer to use: dataflows, dataproc, bigquery, storage e.t.c. so the SA would requre roles to access this services. 
Give SA bigquery user and bigquery data editor roles to create tables in bigquery

