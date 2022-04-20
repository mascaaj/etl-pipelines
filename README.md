# ETL Pipelines

- Serves as repository for data processing and ETL pipeline work
- Currently implemented course from Udemy: 
https://www.udemy.com/course/writing-production-ready-etl-pipelines-in-python-pandas/

- More content will be added soon

## Current status

- All tests passing ~99% coverage. Docker file builds
- Need to test locally, and push docker file to docker hub
- Need to create cron workflow on Argo Workflow to have this dockerfile run at specified time.
- Few variables to be moved to constants file.

## Overview:

This is a ETL pipeline using python, pandas, boto3. It:
1. Gathers files from s3 bucket (German stock exchange data)
2. Performs transformations, aggregations and clean up
3. Pushes report to s3 bucket

## Environment:

This project uses pipenv. This is slightly more involved than miniconda, but has downstream benefits during actions and dockerization. As my understanding improves, some steps here might be redundant.

1. Activate the pipenv shell
2. generate the lock file
   ```
   pipenv install --dev
   ```
3. Install the production environment, for system level install, append --system
    ```
    pipenv install --ignore-pipfile
    ```
4. Install the developer environment
   ```
   pipenv install --dev
   ```

Ensure folder has been added to path:

    ```
    export PYTHONPATH=<path-to-etl-pipelines>/xetra:PYTHONPATH
    ```

## AWS Keys:

- User needs to signup with aws. The keys for the s3 bucket need to be placed as environment variables
- Study at the config file for syntax

## Running the pipeline:

Entrypoint here is `run.py` file.
Docker instructions to come later

### Running codecoverage

```
coverage run --omit=tests* -m unittest discover -v
coverage report -m
```