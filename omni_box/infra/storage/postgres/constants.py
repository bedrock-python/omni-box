"""Constants for PostgreSQL outbox storage."""

# Default batch size for database operations in repository
REPO_BATCH_SIZE = 1000

# Maximum batch size to prevent OOM
MAX_BATCH_SIZE = 10_000
