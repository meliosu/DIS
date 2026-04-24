# CrackHash

Distributed MD5 hash cracking system with fault tolerance.

## Architecture

<p align="center"> <img width="926" height="1565" alt="Untitled-2025-07-17-0055" src="https://github.com/user-attachments/assets/2c042334-740e-4b5d-8262-65628c4ce65f" /> </p>

### Overview

The system consists of a single manager and multiple workers.

When the manager accepts a hash crack request, it is saved to a **MongoDB** collection and split into tasks. The tasks are then passed to workers using **RabbitMQ**.

The manager sends tasks using **Tasks Exchange** which is a direct exchange that just forwards the messages to **Tasks Queue**, from where the workers consume them.

The workers send progress on their respective tasks using **Results Exchange** which is also a direct exchange. The results are consumed by the manager using **Results Queue**. After receiving updates, the request status is updated in the database.

The system is built with fault-tolerance in mind, meaning it can successfully withstand:
* Stopping the manager
* Stopping MongoDB replica set primary node
* Stopping RabbitMQ
* Stopping any worker (even if it already started a task)
* Worker crashes: can be tested by sending request for a `bom` word hash (`e2e6c938b1ba54909ea0b0952235bfaa`)

Also, messages that get requeued 3 or more times are automatically put into **Dead Letter Queue**, from where they are consumed by the manager and logged.

The public API is described in [API](#api).

Configuration is done through environment variables. The list of all variables and their description can be found [here](#configuration).

The project can be easily deployed using Docker (see [Docker](#docker) section).

## API

### Crack a Hash

`POST /api/hash/crack`

```json
{
  "hash": "fc93e9967a2da79dd6a37b332c2d2e17",
  "maxLength": 4
}
```

Response:

```json
{
  "requestId": "<uuid>"
}
```

### Check status

`GET /api/hash/status?requestId=<uuid>`

In progress:

```json
{
  "status": "IN_PROGRESS",
  "progress": 65,
  "data": null
}
```

Ready:

```json
{
  "status": "READY",
  "progress": 100,
  "data": ["abcd"]
}
```

Error:

```json
{
  "status": "ERROR",
  "progress": 30,
  "data": null
}
```

## Configuration

All configuration is done through environment variables in the `.env` file:

| Variable | Description | Default |
|---|---|---|
| `NUM_WORKERS` | Number of worker replicas to deploy | `3` |
| `MANAGER_PORT` | Manager external API port | `8080` |
| `LOG_LEVEL` | Log level (error, warn, info, debug, trace) | `info` |
| `ALPHABET` | Alphabet used for brute-forcing the hash | `abcdefghijklmnopqrstuvwxyz` |
| `RABBITMQ_ADDR` | AMQP URI | `amqp://guest:guest@rabbitmq:5672/%2f` |
| `MONGO_URI` | MongoDB replica set URI | `mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0` |
| `MONGO_DB` | Mongo database name | `crack_hash` |
| `PROGRESS_REPORT_INTERVAL_MS` | Worker progress report period to manager | `1000` |
| `WORKER_MAX_CONCURRENCY` | Max tasks processed concurrently by a worker | `4` |

## Docker

```bash
docker compose up --build -d
```

Manager API will be available at:

`http://localhost:${MANAGER_PORT}`
