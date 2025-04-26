# Database Migrations

This directory contains database migration scripts for Coffee Copilot using Alembic.

## Migration Overview

Coffee Copilot uses Alembic to manage database schema changes. The migrations are organized as follows:

- `versions/`: Contains individual migration scripts, each representing a specific change to the database schema
- `env.py`: Alembic environment configuration
- `script.py.mako`: Template for generating new migration scripts

## Key Migrations

- **URL Uniqueness**: Added a unique constraint on the URL column in the Product table to prevent duplicate products
- **Blacklist Support**: Added blacklisting functionality to prevent certain products from being recommended
- **Extended Product Details**: Enhanced product details storage for better coffee recommendations

## Running Migrations

To apply all pending migrations:

```bash
alembic upgrade head
```

To create a new migration after changing models:

```bash
alembic revision --autogenerate -m "Description of changes"
```

## Order History Migration

For migrating order history from the old database to the new one with URL-based product identification, use the `migrate_order_history.py` script in the project root:

```bash
python migrate_order_history.py
```

This script:
1. Connects to both old and new databases
2. Migrates order history records while maintaining relationships
3. Matches products and variants using intelligent matching strategies
4. Preserves all order details and metadata

## Best Practices

1. Always run migrations in development before deploying to production
2. Commit migration scripts to version control
3. Never modify existing migration scripts after they've been applied
4. Test migrations thoroughly with representative data
