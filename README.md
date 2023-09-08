# confluent-connector

## Description

`confluent-connector` is an integration between Confluent Cloud and Weaviate that simplifies the process of ingesting streaming data into Weaviate. This project aims to make it easy for users to work with real-time data flows.

## Getting Started

### Prerequisites

- Confluent Cloud account
- Docker and Docker Compose installed
- Make utility
- Scala 2.12
- Spark 3.4.0

### Installation Steps

1. **Set Environment Variables**: Update the `CONFLUENT_` environment variables in [`devcontainer.json`](./.devcontainer/devcontainer.json) under `containerEnv` with your own Confluent Cloud details.
2. **Open in Dev Container**: Open the project in the development container.
3. **Build the Jar**: Run `make jar`. This will package the integration into a fat jar located in `target/scala-2.12`.

### Usage Examples

For a hands-on demonstration, refer to the [demo notebook](./notebooks/02_demo_confluent_weaviate.ipynb). It outlines how to set up and use the integration in a local Spark standalone cluster.

## Contributing

### Issues and Feature Requests

Your contributions and feedback are highly valued. If you encounter any issues or have ideas for improvements, please use the appropriate issue templates to guide your submissions:

- **Bug Reports**: If you come across a bug or unexpected behavior, please use the "Bug Report" template. **A minimal reproducible example is crucial for us to quickly identify and fix the issue.** Provide as much detail as possible, including steps to reproduce the issue, the environment you're using, and any error messages you received.
  
- **Performance Issues**: For performance-related concerns, use the "Performance Issue" template. Include any relevant metrics or logs to help us better understand the scope of the problem.

- **Feature Requests**: If you have a suggestion for a new feature or an enhancement to an existing one, please use the "Feature Request" template. Describe the feature, its benefits, and how you envision it working.

Utilizing these templates will help us better understand your needs and prioritize them more effectively.

## Additional Details

- **Scala and Spark**: The integration was developed using Scala 2.12 and Spark 3.4.0.
- **Test Dataset**: The dataset used for testing is the clickstream-users dataset.
- **Kafka Message Assumptions**:
  - Key is a string.
  - Value is serialized using Avro.
  - Timestamps are Unix timestamps in milliseconds.

### Dev Container

Using the development container is the recommended approach as it automatically provisions a Scala and Python environment and installs all required dependencies. If you're not familiar with what a development container is, you can learn more at [containers.dev](https://containers.dev/).


## Feedback

Your feedback is valuable! If you have any comments or run into issues, please open a GitHub issue to let us know.

---

This README is a work in progress and will be updated as the project evolves. Thank you for your interest in `confluent-connector`.
