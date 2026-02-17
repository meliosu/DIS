# CrackHash

A distributed MD5 hash cracking system.

## Architecture

<p align="center"> <img width="743" height="769" alt="architecture" src="https://github.com/user-attachments/assets/ab7013d7-dbab-41c6-8b80-d5f02b0dfa6a" /> </p>

The system has two service types communicating over HTTP.

**Manager** exposes two HTTP servers:
- **External API** (port 80) — accepts crack requests from users and returns their status. The API description is [below](#external-api).
- **Internal API** (port 7123) — handles worker registration and task updates.

A background task runs periodically to detect timed-out workers and redistribute their remaining work to healthy workers.

**Worker** nodes register themselves with the manager on startup. When assigned a task, a worker generates all permutations in its assigned index range, computes the MD5 hash of each, and compares it to the target hash. Progress updates are sent back to the manager periodically.

## External API

### Crack a Hash

Submit an MD5 hash to crack by brute-force.

**Request**

**POST /api/hash/crack**
```
{
  "hash": "fc93e9967a2da79dd6a37b332c2d2e17",
  "maxLength": 5
}
```

| Field       | Type     | Description                                            |
|-------------|----------|--------------------------------------------------------|
| `hash`      | `string` | MD5 hash to crack (32 hex characters)                  |
| `maxLength` | `number` | Maximum length strings to try                          |

**Response** `200 OK`

```json
{
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

---

### Check Status

Poll the status of a previously submitted crack request.

**Request**

**GET /api/hash/status?requestId=a1b2c3d4-e5f6-7890-abcd-ef1234567890**

| Parameter   | Type   | Description                              |
|-------------|--------|------------------------------------------|
| `requestId` | `uuid` | The ID returned from the crack request   |

**Response** `200 OK` (in progress)

```json
{
  "status": "IN_PROGRESS",
  "progress": 42,
  "data": null
}
```

**Response** `200 OK` (completed, match found)

```json
{
  "status": "READY",
  "progress": 100,
  "data": ["hello"]
}
```

**Response** `200 OK` (completed, no match)

```json
{
  "status": "READY",
  "progress": 100,
  "data": null
}
```

**Response** `200 OK` (error)

```json
{
  "status": "ERROR",
  "progress": 100,
  "data": null
}
```

| Field      | Type            | Description                                         |
|------------|-----------------|-----------------------------------------------------|
| `status`   | `string`        | `IN_PROGRESS`, `READY`, or `ERROR`                  |
| `progress` | `number`        | Completion percentage (0–100)                       |
| `data`     | `string[] or null` | Matched plaintext strings, or `null` if none found  |

## Configuration

All configuration is done through environment variables in the `.env` file:

| Variable       | Default                                | Description                                                       |
|----------------|----------------------------------------|-------------------------------------------------------------------|
| `WORKERS`      | `3`                                    | Number of worker replicas to deploy                               |
| `MANAGER_PORT` | `8080`                                 | Manager external API port (on host)                               |
| `LOG`          | `info`                                 | Log level (error, warn, info, debug, trace)                       |
| `TIMEOUT`      | `100`                                  | Worker timeout in seconds                                         |
| `ALPHABET`     | `abcdefghijklmnopqrstuvwxyz0123456789` | Alphabet used for brute-forcing the hash                          |

## Deploy using Docker

Assuming you have [Docker](https://www.docker.com/) installed, just run

```bash
docker compose up --build
```
