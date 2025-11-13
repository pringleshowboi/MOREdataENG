FROM python:3.11-slim

# Install dependencies to add the PostgreSQL repository and install the specific client version
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        # Install utility packages
        curl \
        gnupg \
        # Install lsb-release to resolve the lsb_release: not found issue and obtain the codename
        # OR: skip lsb-release and hardcode the codename as shown below for a slimmer image
        # lsb-release \
        # Add the PostgreSQL 18 repository key
        && curl -sS https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/keyrings/postgresql.gpg \
        # Add the repository to the sources list, using 'trixie' (the Debian codename)
        && sh -c 'echo "deb [signed-by=/etc/apt/keyrings/postgresql.gpg] https://apt.postgresql.org/pub/repos/apt trixie-pgdg main" > /etc/apt/sources.list.d/postgresql.list' \
        # Update again to recognize the new repository
        && apt-get update && \
        # Install the correct PostgreSQL 18 client
        apt-get install -y postgresql-client-18 && \
    # Clean up APT cache
    rm -rf /var/lib/apt/lists/*

COPY elt_script.py .

CMD ["python", "elt_script.py"]