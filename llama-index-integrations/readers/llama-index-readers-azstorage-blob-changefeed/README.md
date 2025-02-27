# Azure Blob Change Feed Reader

This project provides an implementation of an Azure Blob Change Feed Reader that listens to changes in Azure Blob Storage. It processes events such as blob creation, deletion, and updates, allowing users to react to changes in their blob storage in real-time.

## Features

- Listens to the Azure Blob Storage change feed.
- Processes events for blob creation, deletion, and updates.
- Provides utility functions for logging and error handling.

## Installation

To install the required dependencies, run:

```
poetry install
```

## Usage

To use the `BlobChangeFeedReader`, you can create an instance of the class and start listening to the change feed. Below is a simple example:

```python
from src.blob_changefeed_reader import BlobChangeFeedReader

# Initialize the change feed reader
reader = BlobChangeFeedReader(connection_string="your_connection_string")

reader.load_data()
```

## Running Tests

To run the tests for the `BlobChangeFeedReader`, navigate to the `tests` directory and run:

```
pytest
```

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.