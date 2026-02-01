# Integration Testing for Halatio Tundra

This directory contains fixtures and configuration for integration testing with real databases.

## Quick Start

### Start Test Databases

```bash
# Start PostgreSQL and MySQL test databases
docker-compose -f docker-compose.test.yml up -d postgres mysql

# Wait for databases to be healthy
docker-compose -f docker-compose.test.yml ps

# View logs
docker-compose -f docker-compose.test.yml logs -f postgres mysql
```

### Test Database Connections

```bash
# Test PostgreSQL
curl -X POST http://localhost:8080/test/database-connection \
  -H "Content-Type: application/json" \
  -d '{
    "connector_type": "postgresql",
    "credentials": {
      "host": "localhost",
      "port": 5432,
      "database": "test_database",
      "username": "test_user",
      "password": "test_password"
    }
  }'

# Test MySQL
curl -X POST http://localhost:8080/test/database-connection \
  -H "Content-Type: application/json" \
  -d '{
    "connector_type": "mysql",
    "credentials": {
      "host": "localhost",
      "port": 3306,
      "database": "test_database",
      "username": "test_user",
      "password": "test_password"
    }
  }'
```

### Test Data Extraction

```bash
# Extract from PostgreSQL customers table
curl -X POST http://localhost:8080/convert/database \
  -H "Content-Type: application/json" \
  -d '{
    "output_url": "https://your-r2-signed-url...",
    "connector_type": "postgresql",
    "credentials_id": "test_postgres",
    "table_name": "customers"
  }'

# Extract with SQL query
curl -X POST http://localhost:8080/convert/database \
  -H "Content-Type: application/json" \
  -d '{
    "output_url": "https://your-r2-signed-url...",
    "connector_type": "postgresql",
    "credentials_id": "test_postgres",
    "query": "SELECT * FROM customers WHERE is_active = TRUE"
  }'

# Extract with partitioning (for large tables)
curl -X POST http://localhost:8080/convert/database \
  -H "Content-Type: application/json" \
  -d '{
    "output_url": "https://your-r2-signed-url...",
    "connector_type": "postgresql",
    "credentials_id": "test_postgres",
    "table_name": "orders",
    "partition_column": "id",
    "partition_num": 4
  }'
```

## Test Database Schema

### PostgreSQL

**customers** table:
- `id` (SERIAL PRIMARY KEY)
- `name` (VARCHAR)
- `email` (VARCHAR, UNIQUE)
- `created_at` (TIMESTAMP)
- `is_active` (BOOLEAN)
- `balance` (DECIMAL)

**orders** table (1000 rows):
- `id` (SERIAL PRIMARY KEY)
- `customer_id` (INT, FK to customers)
- `order_date` (TIMESTAMP)
- `total_amount` (DECIMAL)
- `status` (VARCHAR: pending/completed/cancelled)

### MySQL

Same schema as PostgreSQL, with MySQL-specific types.

## Database Credentials

### PostgreSQL
- Host: `localhost`
- Port: `5432`
- Database: `test_database`
- Username: `test_user`
- Password: `test_password`

### MySQL
- Host: `localhost`
- Port: `3306`
- Database: `test_database`
- Username: `test_user`
- Password: `test_password`

## Cleanup

```bash
# Stop and remove containers
docker-compose -f docker-compose.test.yml down

# Remove volumes (resets data)
docker-compose -f docker-compose.test.yml down -v
```

## CI/CD Integration

For automated testing in CI/CD:

```bash
# Start databases in background
docker-compose -f docker-compose.test.yml up -d postgres mysql

# Wait for healthy status
timeout 30 bash -c 'until docker-compose -f docker-compose.test.yml ps postgres | grep healthy; do sleep 1; done'

# Run tests
pytest tests/integration/

# Cleanup
docker-compose -f docker-compose.test.yml down -v
```

## Notes

- Test databases are pre-populated with sample data on startup
- Data is ephemeral (lost on container restart unless volumes are used)
- For production Secret Manager testing, mount service account JSON
- ConnectorX binary compatibility is verified by these tests
