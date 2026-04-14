# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

```bash
python3 -m venv ~/.venvs/utilidades
pip install -r requirements.txt
```

Requires a `.env` file in the project root (not tracked in git). Variables follow the pattern:

```
DATABASE_USER=''
DATABASE_CLAVE=''
DATABASE_HOST=''
DATABASE_PORT=''
DATABASE_NAME=''
```

Each script may use different prefixed variables (e.g., `PG_DATABASE_*` for PostgreSQL, `B2_*` for Backblaze). Check the `config()` calls at the top of each script to know which variables it needs.

## Running Scripts

Each script is standalone and run directly:

```bash
python3 backup.py
python3 leer_log_apache.py
python3 proceso/validar_saldos_financiero.py
```

Most scripts present an interactive menu on launch.

## Architecture

This is a **collection of independent utility scripts** — not a single application. There is no shared framework, router, or app entry point.

### Script Categories

- **Backups:** `backup.py` — PostgreSQL full/schema backup and restore via `pg_dump`/`pg_restore`, with domain modification support
- **Log parsing:** `leer_log_apache.py`, `leer_log_postgresql.py` — SCP download + regex parse + batch insert into PostgreSQL
- **Data migration:** `migrar_semantica.py` — MySQL → PostgreSQL contact migration in 1000-record batches
- **Financial auditing:** `proceso/validar_saldos_financiero.py` — validates debit/credit balance across 45+ tenant MySQL databases (listed in `proceso/bases.txt`), tolerance threshold is 5.00
- **Cloud storage:** `backblaze_descargar_backup.py`, `comprimir_fichero_semantica.py`, `eliminar_fichero_energy.py` — Backblaze B2 operations using `b2sdk`
- **Data conversion:** `excel_json_*.py` scripts — Excel → JSON using pandas
- **Image compression:** `comprimir_imagen.py` — PIL-based compression

### Database Conventions

- **MySQL** multi-tenant databases all named `bd*` (e.g., `bdzinc`, `bdenergy`). Key tables: `gen_tercero`, `fin_movimiento`, `fin_saldo_cuenta`, `doc_fichero`
- **PostgreSQL** databases: dev (`bdniquel`), test (`bdtest`), prod (`bdreddoc`)
- Connections are always created at the start of each function using `config()` from `python-decouple`

### Common Script Pattern

```python
from decouple import config
import mysql.connector  # or psycopg2

def crear_conexion():
    return mysql.connector.connect(
        user=config('DATABASE_USER'), ...
    )

def mostrar_menu():
    # Interactive menu with single-key options
    pass

if __name__ == "__main__":
    mostrar_menu()
```

### proceso/ subdirectory

Contains batch-processing scripts that run against many databases at once. `bases.txt` holds the list of 45 tenant database names used by `validar_saldos_financiero.py`. Output is written to timestamped `.txt` files in that directory.
