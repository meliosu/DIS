pub const REQUEST_IN_PROGRESS: &str = "IN_PROGRESS";
pub const REQUEST_READY: &str = "READY";
pub const REQUEST_ERROR: &str = "ERROR";

pub const TASK_PENDING_PUBLISH: &str = "PENDING_PUBLISH";
pub const TASK_QUEUED: &str = "QUEUED";
pub const TASK_DONE: &str = "DONE";
pub const TASK_ERROR: &str = "ERROR";
pub const TASK_DLQ: &str = "DLQ";

pub const RESULTS_CONSUMER_TAG: &str = "manager-results-consumer";
pub const DLQ_CONSUMER_TAG: &str = "manager-dlq-consumer";
pub const RETRY_DELAY_SECS: u64 = 3;
