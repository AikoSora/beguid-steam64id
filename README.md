# Steam64ID to BattlEye GUID Converter

A converter for Steam64ID to BattlEye GUID for generating and storing mappings in a database.

## Description

This module provides functionality for generating Steam64ID according to Valve specifications and converting them to BattlEye GUID format for storage in PostgreSQL database. The system supports table partitioning for efficient handling of large data volumes.

## Features

- Steam64ID generation according to Valve specifications
- Conversion to BattlEye GUID format
- Table partitioning for optimization
- Asynchronous processing with connection pooling
- Search result caching
- Error handling and retry mechanisms
- Multi-threaded data generation

## Installation

### Requirements

- Python 3.8+
- PostgreSQL 12+
- asyncpg
- **Storage**: Minimum 100GB free disk space for database

### Dependencies

```bash
pip install asyncpg
```

### Database Setup

1. Create a PostgreSQL database:
```sql
CREATE DATABASE steam64id_db;
```

**Note**: The database will require significant storage space (100GB+) as it stores mappings for all possible Steam64ID combinations across multiple universes.

2. Configure connection:
```python
db_config = DatabaseConfig(
    host="localhost",
    port=5432,
    database="steam64id_db",
    username="postgres",
    password="your_password"
)
```

## Usage

### Basic Usage

```python
import asyncio
from src import DatabaseConfig, Steam64IDConverter

async def main():
    db_config = DatabaseConfig(
        host="localhost",
        port=5432,
        database="steam64id_db",
        username="postgres",
        password="password"
    )
    
    async with Steam64IDConverter(db_config) as converter:
        # Start generation
        await converter.start_generation()
        
        # Search Steam64ID by GUID
        steam64id = await converter.find_steam64id_by_guid(
            "6d67b58640e2f223c22101615936a32c"
        )
        print(f"Found Steam64ID: {steam64id}")

asyncio.run(main())
```

## Architecture

### Partitioning

The system automatically creates partitioned tables:

```sql
steam64id_mapping_u{universe}_{start_range}_{end_range}
```

Partition ranges:
- 1 - 3,000,000
- 3,000,001 - 6,000,000
- 6,000,001 - 9,000,000
- ... and so on

### Caching

Search results are cached in the `guid_cache` table to speed up repeated queries.

## Logging

The system maintains detailed logs in the `steam64id_converter.log` file:

```
2024-01-01 12:00:00 - INFO - Database connection pool established
2024-01-01 12:00:01 - INFO - Universe 0: processing chunk 1-50000
2024-01-01 12:00:02 - INFO - Inserted 50000 records into table_name
```

## Error Handling

The system automatically handles:
- Database connection errors
- Generation timeouts
- Critical disk errors
- Data conflicts

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.