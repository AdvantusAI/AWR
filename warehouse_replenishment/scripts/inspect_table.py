from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from configparser import ConfigParser
import os

# Read database configuration
config = ConfigParser()
config.read('warehouse_replenishment/config/settings.ini')

# Create database connection string
db_url = f"postgresql://{config['DATABASE']['username']}:{config['DATABASE']['password']}@{config['DATABASE']['host']}:{config['DATABASE']['port']}/{config['DATABASE']['database']}"

# Create engine and inspector
engine = create_engine(db_url)
inspector = inspect(engine)

# Get table information
table_name = 'item'
columns = inspector.get_columns(table_name)
primary_keys = inspector.get_pk_constraint(table_name)
foreign_keys = inspector.get_foreign_keys(table_name)
indexes = inspector.get_indexes(table_name)

# Print table structure
print(f"\nTable Structure for '{table_name}':")
print("\nColumns:")
for column in columns:
    print(f"  - {column['name']}: {column['type']} (nullable: {column['nullable']})")

print("\nPrimary Keys:")
for pk in primary_keys['constrained_columns']:
    print(f"  - {pk}")

print("\nForeign Keys:")
for fk in foreign_keys:
    print(f"  - {fk['constrained_columns']} -> {fk['referred_table']}.{fk['referred_columns']}")

print("\nIndexes:")
for index in indexes:
    print(f"  - {index['name']}: {index['column_names']} (unique: {index['unique']})") 