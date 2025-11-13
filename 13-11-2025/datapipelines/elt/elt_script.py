import subprocess
import time

def wait_for_postgres(host, max_retries=5, delay_seconds=10):
    """
    Waits for a PostgreSQL server running on a specific host to be ready
    to accept connections by repeatedly calling pg_isready.
    """
    retries = 0
    print(f"Waiting for PostgreSQL on host: {host}...")
    while retries < max_retries:
        try:
            # Note: We use the default user 'postgres' and no password for pg_isready
            # The 'check=True' ensures a CalledProcessError is raised on non-zero status (not ready)
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True
            )
            if "accepting connections" in result.stdout:
                print(f"PostgreSQL is ready on host {host}.")
                return True
        except subprocess.CalledProcessError as e:
            # Non-zero exit status (2) from pg_isready means the server is not ready
            print(f"PostgreSQL is not ready yet: {e.stderr.strip() if e.stderr else e.stdout.strip()}. Retrying in {delay_seconds} seconds...")
            retries += 1
            time.sleep(delay_seconds)
    
    print(f"PostgreSQL did not become ready after {max_retries} attempts on host {host}.")
    return False

# 1. READINESS CHECKS
# We check for both source and destination databases by their service names
if not wait_for_postgres(host="source_postgres"):
    exit(1)
if not wait_for_postgres(host="destination_postgres"):
    exit(1)


print("Proceeding with ELT operations...")

# Configuration for Source Database (Extract)
source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'password',
    'host': 'source_postgres',
}

# Configuration for Destination Database (Load)
destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'password',
    'host': 'destination_postgres',
}

# 2. Extraction (E) - Dump data from source to a file (data_dump.sql)
print("Starting data extraction (pg_dump)...")
# --- FIX: Changed 'PGPWASSWORD' to 'PGPASSWORD' ---
subprocess_env = {'PGPASSWORD': source_config['password']} 

# Dump command: pg_dump -h source_postgres -U postgres -d source_db -f data_dump.sql
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-F', 'p',     # Use plain text format
    '--clean',     # Include commands to clean (drop) objects before creating them
]

try:
    subprocess.run(dump_command, env=subprocess_env, check=True)
    print("Data extraction successful. Dump file created.")
except subprocess.CalledProcessError as e:
    print(f"Data extraction failed: {e}")
    exit(1)


# 3. Loading (L) - Load data from file (data_dump.sql) into destination
print("Starting data loading (psql)...")
# --- FIX: Changed 'PGPWASSWORD' to 'PGPASSWORD' ---
subprocess_env = {'PGPASSWORD': destination_config['password']}

# Load command: psql -h destination_postgres -U postgres -d destination_db -a -f data_dump.sql
load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a',         # Echo all input lines to standard output
    '-f', 'data_dump.sql',
]

try:
    subprocess.run(load_command, env=subprocess_env, check=True)
    print("Data loading successful.")
except subprocess.CalledProcessError as e:
    print(f"Data loading failed: {e}")
    exit(1)
    
print("Data extraction and loading completed successfully. Ending ELT script.")