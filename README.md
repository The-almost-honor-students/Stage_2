# Stage 2 — The Almost Honor Students

This project corresponds to **Stage 2** of the *Project Gutenberg Book Search Engine*, developed using a modular and decoupled architecture.  
In this stage, four independent services —**Ingestion**, **Indexing**, **Search**, and **Control**— were implemented to handle the complete data flow from book retrieval to indexed search and performance benchmarking.

---

## Table of Contents
1. [Architecture Overview](#1-architecture-overview)
2. [Implemented Functionality](#2-implemented-functionality)  
3. [Environment Variables Configuration](#3-environment-variables-configuration)  
 2.1 [Ingestion Service](#ingestion-service)  
 2.2 [Indexing Service](#indexing-service)  
 2.3 [Search Service](#search-service)  
 2.4 [Control Service](#control-service)  
4. [Building the Project](#4-building-the-project)  
5. [Running with Docker](#5-running-with-docker)  
6. [Example Usage and Test Queries](#6-example-usage-and-test-queries)  
7. [Benchmarking](#7-benchmarking)  

---

## 1. Architecture Overview

Each service runs as an independent module with its own HTTP server (based on **Javalin**) and configuration managed through `.env` files located in the `resources` directory.  
The services communicate through **REST APIs**, and **MongoDB** acts as the main storage system for both metadata and inverted indexes.

The project follows **Clean Architecture / Hexagonal Architecture** principles, with clear separation of responsibilities:

- `application` → business logic and use cases.  
- `infrastructure` → external adapters (MongoDB, HTTP, S3/local FS).  
- `domain` → core entities of the data model.

---

## 2. Implemented Functionality

- Modular implementation of **Ingestion**, **Indexing**, **Search**, and **Control** services.  
- REST APIs built with **Javalin**.  
- Configuration managed via `.env` files under `resources/`.  
- Persistent data storage in **MongoDB** (`metadata` and `inverted_index` collections).  
- Text preprocessing and tokenization for indexing.  
- Workflow orchestration through the **Control Service**.  
- Integration of **JMH** benchmarking in all modules.  
- Full Docker containerization with `docker-compose`.  
- Clear and incremental **Git history** showing contributions and progress.ç
  
---

## 3. Environment Variables Configuration

Each service includes its own `.env` file inside the `resources/` directory.  
These files define URLs, ports, and database parameters without modifying the source code.

### Ingestion Service

**Purpose:** Downloads books from Project Gutenberg, stores them temporarily, and forwards them to the Indexing service.  
**File:** `resources/.env`

```env
URL_GUTENBERG=https://www.gutenberg.org
PORT=7070
```

---

### Indexing Service

**Purpose:** Processes the downloaded books, extracts metadata, generates an inverted index, and stores the results in MongoDB.  
**File:** `resources/.env`

```env
MONGO_URL=mongodb://localhost:27017
DATABASE_NAME=books
COLLECTION_METADATA=metadata
COLLECTION_INDEX=inverted_index
PORT=8080
```

---

### Search Service

**Purpose:** Performs full-text searches over the inverted index and returns results through REST API responses.  
**File:** `resources/.env`

```env
MONGO_URL=mongodb://localhost:27017
DATABASE_NAME=books
COLLECTION_METADATA=metadata
COLLECTION_INDEX=inverted_index
PORT=9090
```

---

### Control Service

**Purpose:** Orchestrates the workflow by coordinating ingestion, indexing, and search services.  
It also triggers benchmarking routines using **JMH** to evaluate performance.  
**File:** `resources/.env`

```env
INGESTION_URL=http://localhost:7070
INDEXING_URL=http://localhost:8080
SEARCH_URL=http://localhost:9090
```

---

## 4. Building the Project

Each microservice is built using **Maven**.

To build all services from the project root:

```bash
mvn clean package -DskipTests
```

To build a specific service (for example, `indexing`):

```bash
cd indexing
mvn clean package -DskipTests
```

This will generate a JAR file inside each module’s `target` directory.

---

## 5. Running with Docker

All services are **dockerized** and can be executed independently or together using `docker-compose`.

### Steps to Run

1. Create the `.env` files in each service’s `resources` folder:

   ```
   ingestion/resources/.env
   indexing/resources/.env
   search/resources/.env
   control/resources/.env
   ```

2. From the project root directory, run:

   ```bash
   docker-compose up --build -d
   ```

3. Once the containers are running, the services will be available at:
   - Ingestion → `http://localhost:7070`
   - Indexing → `http://localhost:8080`
   - Search → `http://localhost:9090`
   - Control → `http://localhost:6060`

---

## 6. Example Usage and Test Queries

Once the system is running, the services can be tested with tools like **curl** or **Postman** (recommended for easier visualization and testing).  
Below is a list of all available endpoints and example calls for each one. In case you use Postman, ignore curl calls, just take the examples, like http://localhost:7070/ingest/6036.  

### Available Endpoints

| Service      | Method | Endpoint                        | Description |
|---------------|---------|----------------------------------|-------------|
| **Ingestion** | POST    | `/ingest/{bookId}`              | Downloads a specific book from Project Gutenberg by ID and prepares it for indexing. |
| **Ingestion** | GET     | `/ingest/list`                  | Returns the list of ingested books. |
| **Ingestion** | GET     | `/ingest/status/{bookId}`       | Returns the ingestion status for a specific book. |
| **Indexing**  | POST    | `/index/update/{bookId}`        | Processes and indexes the specified book. |
| **Search**    | GET     | `/search?query=<keyword>`       | Searches for a specific keyword in the inverted index. |

### Example Queries

#### Ingest a Book
Example for book ID 6036:

```bash
curl -X POST http://localhost:7070/ingest/6036
```

#### Get the Ingested Book List

```bash
curl http://localhost:7070/ingest/list
```

#### Check Ingestion Status of a Book
Example for book ID 6036:

```bash
curl http://localhost:7070/ingest/status/6036
```

#### Index a Book
Example for book ID 6036:

```bash
curl -X POST "http://localhost:8080/index/update/6036"
```

#### Search for a Keyword
Example for the word “love”:

```bash
curl "http://localhost:9090/search?query=love"
```

#### Run the Complete Workflow via Control Service

```bash
curl -X POST http://localhost:6060/control/start
```

---

## 7. Benchmarking

Performance benchmarking has been implemented in **all services** — Ingestion, Indexing, Search, and Control — using **JMH (Java Microbenchmark Harness)**.  
Each benchmark evaluates throughput, latency, and scalability under different workloads.

### Location

The benchmark for the Control service can be found at:

```
control/src/main/java/com/tahs/benchmark/
```

### How to Execute

1. Ensure all services are **running via Docker Compose**:

   ```bash
   docker-compose up -d
   ```

2. Once the containers are up and connected, execute the benchmark from the **Control module**:

   ```bash
   cd control/src/main/java/com/tahs
   java Main
   ```

This will automatically perform end-to-end benchmarking of ingestion, indexing, and search through the orchestrator.

Benchmark results are printed to the console and may include:
- Operations per second (ops/s)
- Execution time per thread
- CPU and memory usage across services




