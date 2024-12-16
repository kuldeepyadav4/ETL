# ETL
<br>
# PostgreSQL Configuration Guide

---

## Introduction
This guide explains how to effectively modify the `postgresql.conf` file in PostgreSQL to customize configurations such as ports, memory, and connection settings.

---

## Why Modify the `postgresql.conf` File?
The `postgresql.conf` file allows you to:
- Set the server's listening port.
- Optimize performance by adjusting memory and caching.
- Customize client connection limits.
- Enable logging for debugging and auditing purposes.

---

## Steps to Modify the `postgresql.conf` File
Follow these steps to safely modify and apply changes to the `postgresql.conf` file.

### 1. Locate the Configuration File
- The `postgresql.conf` file is usually located in:
  - **Windows:** `C:\Program Files\PostgreSQL\<version>\data\`

# open_meteo_api Connection;

Connection ID - open_meteo_api
Connection Type - HTTP
Host - https://api.open-meteo.com/

# Postgres Connection;

Connection ID - postgres_default
Connection Type - postgres
Host - "copy host name from docker"
Admin - postgres
Password - postgres
Port - 5432




