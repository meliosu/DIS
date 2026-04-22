pub const ALPHABET_ENV: &str = "ALPHABET";

pub const MANAGER_BIND_ADDR_ENV: &str = "MANAGER_BIND_ADDR";
pub const DEFAULT_MANAGER_BIND_ADDR: &str = "0.0.0.0:80";

pub const RABBITMQ_ADDR_ENV: &str = "RABBITMQ_ADDR";
pub const DEFAULT_RABBITMQ_ADDR: &str = "amqp://guest:guest@rabbitmq:5672/%2f";

pub const MONGO_URI_ENV: &str = "MONGO_URI";
pub const DEFAULT_MONGO_URI: &str =
    "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0";
pub const MONGO_DB_ENV: &str = "MONGO_DB";
pub const DEFAULT_MONGO_DB: &str = "crack_hash";

pub const NUM_WORKERS_ENV: &str = "NUM_WORKERS";
pub const DEFAULT_WORKERS: usize = 3;
pub const PROGRESS_REPORT_INTERVAL_MS_ENV: &str = "PROGRESS_REPORT_INTERVAL_MS";
pub const DEFAULT_PROGRESS_REPORT_INTERVAL_MS: u64 = 1000;
pub const WORKER_MAX_CONCURRENCY_ENV: &str = "WORKER_MAX_CONCURRENCY";
pub const DEFAULT_WORKER_MAX_CONCURRENCY: usize = 4;

pub const REQUESTS_COLLECTION: &str = "requests";
pub const TASKS_COLLECTION: &str = "tasks";

pub const TASKS_EXCHANGE: &str = "hash.tasks.ex";
pub const TASKS_QUEUE: &str = "hash.tasks.q";
pub const TASKS_ROUTING_KEY: &str = "hash.tasks";

pub const RESULTS_EXCHANGE: &str = "hash.results.ex";
pub const RESULTS_QUEUE: &str = "hash.results.q";
pub const RESULTS_ROUTING_KEY: &str = "hash.results";

pub const DLX_EXCHANGE: &str = "hash.dlx.ex";
pub const DLQ_QUEUE: &str = "hash.dlq.q";
pub const TASKS_DLQ_ROUTING_KEY: &str = "hash.tasks.dlq";
pub const RESULTS_DLQ_ROUTING_KEY: &str = "hash.results.dlq";

pub const REQUEUE_DELIVERY_LIMIT: i32 = 3;
pub const REPUBLISH_INTERVAL_SECS: u64 = 5;
