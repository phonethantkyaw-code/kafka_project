# Router Command Lab — two-router frontend update

This is an upgrade for the working kafka-router-project on Ubuntu.
Keep your existing Kafka and Kafka UI containers. Stop the old Python backend and
worker with Ctrl+C before installing. No additional runtime packages are required.

## Install

Copy this ZIP from Windows to Ubuntu, extract in your home directory, then run:

```bash
cd ~
unzip -o kafka-router-upgrade.zip
bash ~/kafka-router-upgrade/install-update.sh
```

The installer backs up overwritten source files under the existing project.
It preserves .env, .venv, and jobs.sqlite3. It does not modify Kafka data.

Default router addresses:
- R1: existing ROUTER_HOST, currently 192.168.221.134
- R2: 192.168.221.135
Both use the existing ROUTER_USERNAME and ROUTER_PASSWORD.
For different logins, set R2_USERNAME and R2_PASSWORD in .env.
Set R1_HOST and R2_HOST if DHCP assigns different addresses.
Never commit .env, jobs.sqlite3, or the virtual environment to GitHub.

## Test SSH (Ubuntu)

```bash
cd ~/kafka-router-project
source .venv/bin/activate
python worker.py --ssh-test R1
python worker.py --ssh-test R2
```

## Terminal 1: backend and frontend

```bash
cd ~/kafka-router-project
source .venv/bin/activate
python -m uvicorn backend:app --host 0.0.0.0 --port 8001
```

## Terminal 2: worker

```bash
cd ~/kafka-router-project
source .venv/bin/activate
python worker.py
```

Open http://192.168.221.131:8001 in the Windows browser. Select R1 or R2,
select a command, and click Execute command. Status progresses from queued to
running to completed/failed. Results refresh every two seconds. Recent requests
survive backend and worker restarts.

## Architecture

Browser -> FastAPI -> Kafka topic router-commands -> worker -> Paramiko -> router.
The worker writes output into jobs.sqlite3. FastAPI reads that local database to
return results to the browser. Only the worker connects to routers. No passwords
are put in Kafka events or sent to the browser.

Both Python processes must run in the same project directory on the same Ubuntu
machine so they share the SQLite result store. This version is for your classroom
LAN and has no web authentication; do not expose port 8001 to the internet.
The frontend offers only read-only diagnostic commands. Kafka UI remains on 8081.

## Delivery behavior

One active worker is recommended. The same cisco-workers consumer group is reused.
Stop the old worker or it may consume requests intended for the new worker.
Completed/failed job IDs are deduplicated in SQLite. Results are saved before Kafka
offsets are committed. A crash after SSH execution but before saving can still
repeat the read-only command. This is not exactly-once execution.
SSH failures are saved as failed results, then acknowledged so later jobs continue.
Malformed events are logged with partition/offset and acknowledged without SSH.
Database failures stop the worker without acknowledging the request.

Backend delivery timeout is marked delivery_unknown, since Kafka may have accepted
the request. Check the existing job before submitting another request.
Queued jobs need a running worker. This small demo has no worker heartbeat,
cancellation, result retention policy, or background recovery of unsent jobs.

## Troubleshooting

- ModuleNotFoundError: activate .venv and install requirements.txt.
- Address already in use: stop the previous backend on 8001 with Ctrl+C.
- Page does not open: check the backend startup, Ubuntu IP and firewall rules.
- queued forever: check worker terminal and Kafka connectivity.
- AuthenticationException: update the chosen router's credentials in .env and restart worker.
- Database must be shared by both processes; keep JOBS_DB unset for the default.
- Results can be inspected at GET /jobs and GET /jobs/{job_id}.

## Roll back source files

Stop both Python processes and copy source files from the installer's printed backup
folder back to ~/kafka-router-project. The upgrade uses the original Kafka topic
and accepts old lab-router events as R1. Back up jobs.sqlite3 if you want to keep
result history separately.
