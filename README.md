# ProReveal-Backend

This repository has the server-side source code of [ProReveal](https://github.com/proreveal/ProReveal).
Note that you can try ProReveal without a server (i.e., with a browser computation engine). However, with a server, you can save and restore an analysis session, access to the session through multiple devices (e.g., we have interfaces for desktops and mobile phones), load a remote dataset (e.g., one on [Amazon S3](https://aws.amazon.com/s3)), and use a distributed computing engine for more scalability.

## Installation

We strongly recommend using Anaconda since the server is developed and tested using Anaconda 4.5 with Python 3.6.5.
The following packages of compatible versions must be installed:

```
python-socketio==4.3.1
socketio==0.1.7
eventlet==0.24.1
pandas==0.24.2
```

After installing dependencies and cloning this repository, you can run a standalone server as follows:

```bash
python main.py [config_file]
```

Or, you can run the server with an Apache Spark cluster as follows:

```bash
spark-submit [spark_options] main.py [config_file]
```

## Configuration

A configuration file describes an engine type (`local` or `spark`) and the location of a dataset. 
Here is an example (`local.cfg`):

```
[backend]
type=local
dataset=data/movies
sample_rows=100
```

This configuration file uses pandas for computation (`type=local`).
When the server starts up, it first looks for [a metadata file](https://github.com/proreveal/ProReveal-Backend/blob/master/data/movies/metadata.json) in the given dataset directory (i.e., `data/movies/metadata.json`).
In this case, the dataset we are using (i.e., `data/movies`) consists of [a single JSON file](https://github.com/proreveal/ProReveal-Backend/blob/master/data/movies/movies.json), so you must describe the number of rows that you want to process in each iteration (i.e., 100).
If the dataset is already chunked and consists of multiple batches, each batch is processed one by one.
In this case, you cannot set `sample_rows`. See [an example](https://github.com/proreveal/ProReveal-Backend/blob/master/chunked_local.cfg) of using a chunked dataset.

Here is another example configuration file for working with a Spark cluster.

```
[backend]
type=spark
dataset=hdfs://<hdfs ip>/<dataset path>
eager=True
shuffle=True
```

Note that we set `type` to `spark` and `dataset` as a path to a remote dataset.

If `eager` is set to `True`, the server caches (e.g., by calling `df.cache()`) all datasets when initialized.

If `shuffle` is set to `True`, the server randomizes the processing order of batches for each query. 

## Datasets and `metadata.json`

A dataset is a directory in which `metadata.json` exists. Here is `metadata.json` of [a sample dataset](https://github.com/proreveal/ProReveal-Backend/tree/master/data/movies) with a single source:

```json
{
    "source": {
        "name": "Movies",
        "path": "movies.json"
    },
    "fields": "<field specifications>"
}
```

If a dataset consists of multiple sources, you need to specify the locations of sources in `metadata.json` as follows:

```json
{
    "source": {
        "name": "Movies",
        "batches": [
            {
                "path": "movies.1.json"
            },
            {
                "path": "movies.2.json"
            },
            {
                "path": "movies.3.json"
            },
            {
                "path": "movies.4.json"
            }
        ]
    },
    "fields": "<field specifications>"
}
```

You can use `csv`, `json`, `parquet`, and other data formats that pandas or Spark supports (depending on the engine type).

We need to know the number of records in batches for statistical inference. 
By default, the server counts the number of records in each batch, but if the number is known you can provide the number as follows:

```json
{
    "source": {
        "name": "gaia",
        "batches": [
            {
                "path": "0.parquet",
                "numRows": 90000000
            },
            {
                "path": "1.parquet",
                "numRows": 90000000
            }
        ]
    },
    "fields": "<field specifications>"
}
```

### Field Specification

`metadata.json` also specifies the names and types of fields in the dataset.
Here is an example:

```json
{
    "source": "<source specification>"
    "fields": [
        {
            "name": "Language",
            "vlType": "nominal",
            "dataType": "string"
        },
        {
            "name": "Popularity",
            "vlType": "quantitative",
            "min": 0,
            "max": 300,
            "numBins": 24,
            "dataType": "float"
        }
    ]
}
```

``vlType`` can be `key`, `nominal`, or `quantitative`. `key` is a nominal field with very high cardinality, such as a unique ID for each row, which cannot be included in a visualization query but should be displayed in the detail view.

``dataType`` can be `string`, `int`, and `float`. This field is used for formatting data values.

For quantitative fields, you can provide the parameters for binning by specifying `min`, `max`, and `numBins` which defaults to 40. If these parameters are not provided, the parameters are computed using the first batch of the dataset and extended.

## Session Management

If you create a new session, you will be able to see a three-letter code on the navigation bar.
Later, you can access the session by entering the code on the main page.
Sessions are stored in the memory of the server, so they will be lost if you shut down the server.
If multiple devices connect to the same session, each action occurs in the devices (e.g., creating a query) will be synchronized.

 