# Shared ETL utility functions

- fetch energy data from ESPI APIs
- GCS support
- OEEM datastore loading

## Install

    git clone https://github.com/impactlab/oeem-etl
    cd oeem-etl
    pip install -e .

## Configure

Running a Luigi task requires a `.cfg` configuration file with the following section and parameters:

    [oeem]
    url=http://localhost:9009/
    access_token=tokstr
    project_owner=1
    file_storage=local
    local_data_directory=/test_data
    uploaded_base_path=uploaded
    formatted_base_path=oeem-format

## Run

This library isn't run directly. Rather, it's components are used to build datastore-specifiec ETL pipelines, which are
found in separate repos.

An example of a command to run a Luigi task from a client-specific module:

    export LUIGI_CONFIG_PATH=/test_data/config.cfg
    luigi --module oeem_etl_client LoadAll --local-scheduler

The client module that implements the task should be installed where Luigi can find it (e.g. `pip install -e .`).

## Data Format

The datastore expects CSV files with the following fields for loading:

**Projects**

| Field | Value |
| --- | --- |
| project_id | Client's id for this project |
| zipcode | |
| baseline_period_end | |
| reporting_period_start | |

**Traces**

| Field | Value |
| --- | --- |
| trace_id | Unique identifier for the trace |
| start | |
| interpretation | [EEMeter Interpretations](http://eemeter.readthedocs.io/en/latest/eemeter_api.html#module-eemeter.structures) |
| value | |
| estimated | |
| unit | `KWH` or `THM` |

**ProjectTraceMapping**

| Field | Value |
| --- | --- |
| project_id | |
| trace_id | |

**ProjectMetadata**

Both of the following methods are supported

Emit key, value pairs for a project into a csv with column headers as key name.

| Field | Value |
| --- | --- |
| project_id | |
| [Attribute Name] | [Attribute Value]

or

Emit key, value pairs for projects into a csv with the columns `project_id`, `key`, and `value`

| Field | Value |
| --- | --- |
| project_id | |
| key | [Attribute Name]
| value | [Attribute Value]
