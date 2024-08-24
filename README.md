# Microservice for Bubble App and Other Application

This repository contains the source code for a microservice designed to integrate with a [Bubble](https://bubble.io/) application. The microservice provides various functionalities, such as lead generation, data processing, and recommendations.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)
- [Testing](#testing)
- [CI/CD](#cicd)
- [Contributing](#contributing)
- [License](#license)

## Overview

This microservice is built to enhance the functionality of a Bubble App by providing backend services that handle complex operations outside the scope of Bubble's native capabilities. The services provided include:

## Architecture

The microservice is structured as follows:

- **`models/`**: Contains data models and schemas.
- **`src/`**: Main source code directory.
- **`tests/`**: Contains unit and integration tests.
- **`utils/`**: Utility functions and helpers.
- **`function_app.py`**: Entry point for the Azure Function App.
- **`.github/workflows/`**: CI/CD pipeline configurations.
