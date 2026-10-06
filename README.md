# Cisco commands through Kafka

Run this example on the same Ubuntu machine as your existing Kafka Docker container.
Python 3.10+ is required. Leave your existing Kafka/Kafka UI running.

Backend (FastAPI) -> router-commands topic -> worker -> Paramiko SSH -> Cisco router.
The backend creates the topic once and publishes a message for each request.
The worker prints results to its terminal. No result API or results topic is included.

## Install

Extract this ZIP, open a terminal inside kafka-router-project, then:

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
nano .env
```

Set ROUTER_HOST, ROUTER_USERNAME and ROUTER_PASSWORD to YOUR router's values.
The example IP is a placeholder. The router must be reachable from Ubuntu and
have SSH enabled, a local account, and permission to run the two show commands.
Do not put your real .env in GitHub. Credentials stay in the worker environment;
they are not sent through Kafka.

Paramiko 3.x is selected for older classroom Cisco images. Do not downgrade further
or change SSH algorithms blindly. If you get a key-exchange error, collect the
exact exception and router `show ip ssh` output for troubleshooting.
Paramiko does not automatically apply your OpenSSH ~/.ssh/config settings.

## Test SSH first

```bash
python -c "import paramiko; print(paramiko.__version__)"
python worker.py --ssh-test
```

You should see `show ip interface brief` output. Fix SSH before proceeding.
This lab example explicitly allows unknown host keys in .env.example; for a
verified connection set LAB_ALLOW_UNKNOWN_HOST_KEY=false and populate known_hosts
with a router key whose fingerprint you have verified.

## Start the first backend (terminal 1)

```bash
source .venv/bin/activate
python -m uvicorn backend:app --host 127.0.0.1 --port 8000
```

It creates router-commands if it does not exist. Use another port if 8000 is busy.

## Start the worker (terminal 2, same project folder)

```bash
source .venv/bin/activate
python worker.py
```

## Submit a job (terminal 3, on the same Ubuntu machine)

```bash
curl -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' -d '{"action":"interfaces"}'
```

The HTTP response says queued and gives a job_id. Terminal 2 prints the router
output and the same job_id. `{"action":"version"}` runs `show version`.
The local API docs are at http://localhost:8000/docs.

In Kafka UI, open Topics -> router-commands to inspect the JSON request event.

## Addresses

localhost:9092 must be a host-accessible advertised Kafka listener. If the backend
runs on another machine, configure Kafka's external advertised listener to the
reachable broker address; changing only KAFKA_BOOTSTRAP_SERVERS is insufficient.
Inside a container localhost means that container. For later containerization use
the Kafka service's internal listener on a shared Docker network, e.g. kafka:29092.
The worker must independently be able to reach the router over TCP port 22.

## Delivery and limits

Only predefined read-only show commands are accepted. This is a classroom example,
not a production job platform. The worker commits after successful SSH and printing.
A crash before commit can repeat a command. An error halts the worker without
committing; fix the cause and restart. A malformed event also halts this simple
worker. It has no dead-letter queue, durable result store, or automatic retries.
Do not extend it to configuration changes without idempotency and error handling.

Syntax was checked during preparation. Live Kafka/SSH testing requires your lab.
