# CrackHash

Distributed MD5 hash cracking system with fault tolerance.

## Architecture

**TODO**: Architecture Overview

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
| `WORKER_MAX_CONCURRENCY` | Max tasks processed concurrently by a manager | `4` |

## Docker deployment

```bash
docker compose up --build -d
```

Manager API will be available at:

`http://localhost:${MANAGER_PORT}`
