# dataflows-ckan

Dataflows processors to work with CKAN.

## Features

- `dump_to_ckan` processor

## Contents

<!--TOC-->

  - [Getting Started](#getting-started)
    - [Installation](#installation)
    - [Examples](#examples)
  - [Documentation](#documentation)
    - [dump_to_s3](#dump_to_s3)
  - [Contributing](#contributing)
  - [Changelog](#changelog)

<!--TOC-->

## Getting Started

### Installation

The package use semantic versioning. It means that major versions  could include breaking changes. It's recommended to specify `package` version range in your `setup/requirements` file e.g. `package>=1.0,<2.0`.

```bash
$ pip install dataflows-ckan
```

### Examples

These processors have to be used as a part of data flow. For example:

```python
flow = Flow(
    load('data/data.csv'),
    dump_to_ckan(
        host,
        api_key,
        owner_org,
        overwrite_existing_data=True,
        push_to_datastore=False,
        push_to_datastore_method='insert',
        **options,
    ),
)
flow.process()
```

## Documentation

### dump_to_ckan

Saves the DataPackage to a CKAN instance.

## Contributing

Create a virtual environment and install [Poetry](https://python-poetry.org/).

Then install the package in editable mode:

```
$ make install
```

Run the tests:

```
$ make test
```

Format your code:

```
$ make format
```

## Changelog

### 0.2.0

- Full port to dataflows, and some refactoring, with a basic integration test.

#### 0.1.0

- an initial port from https://github.com/frictionlessdata/datapackage-pipelines-ckan based on the great work of @brew and @amercader
