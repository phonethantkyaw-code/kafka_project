"""Consume one job at a time and run show commands on the configured lab router."""
import json
import os
import re
import sys
import time
import paramiko
from kafka import KafkaConsumer
from settings import BOOTSTRAP, TOPIC, COMMANDS, ROUTERS, router_login
import job_store


def read_prompt(channel, prompt=None, timeout=30):
    output = ''
    deadline = time.monotonic() + timeout
    # Initial prompt detection supports ordinary Cisco hostnames, e.g. R1#.
    pattern = re.compile(r'(?m)^([\w.():/-]+[>#])\s*$')
    while time.monotonic() < deadline:
        if channel.recv_ready():
            data = channel.recv(65535)
            if not data:
                raise ConnectionError('Router closed the SSH channel')
            output += data.decode('utf-8', errors='replace').replace('\r', '')
            if len(output) > 2_000_000:
                raise RuntimeError('Router output exceeded the lab limit')
            if prompt is None:
                match = pattern.search(output)
                if match and output.rstrip().endswith(match.group(1)):
                    return output, match.group(1)
            elif output.rstrip().split('\n')[-1] == prompt:
                return output, prompt
        elif channel.closed:
            raise ConnectionError('Router closed the SSH channel')
        time.sleep(0.05)
    raise TimeoutError('No Cisco prompt received within 30 seconds')


def run_command(command, router_id="R1"):
    if command not in COMMANDS.values():
        raise ValueError('Only the configured show commands are allowed')
    login = router_login(router_id)
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    if os.getenv('LAB_ALLOW_UNKNOWN_HOST_KEY', 'false').lower() == 'true':
        # Explicit opt-in for an isolated classroom router only.
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            **login,
            look_for_keys=False, allow_agent=False,
            timeout=10, auth_timeout=15, banner_timeout=15,
        )
        channel = client.invoke_shell(width=200, height=1000)
        _, prompt = read_prompt(channel)
        channel.sendall('terminal length 0\n')
        output, _ = read_prompt(channel, prompt)
        if any(marker in output for marker in ('% Invalid', '% Error', '% Authorization')):
            raise RuntimeError(output)
        channel.sendall(command + '\n')
        output, _ = read_prompt(channel, prompt)
        if any(marker in output for marker in ('% Invalid', '% Error', '% Authorization', '% Incomplete', '% Ambiguous')):
            raise RuntimeError(output)
        return output
    finally:
        client.close()


def process_event(event):
    if not isinstance(event, dict):
        raise ValueError('Expected a JSON object')
    router_id = event.get('router_id')
    if router_id == 'lab-router':
        router_id = 'R1'  # Accept requests from the earlier project.
    if router_id not in ROUTERS or event.get('command') not in COMMANDS.values():
        raise ValueError('Unsupported router or command')
    if not isinstance(event.get('job_id'), str) or not event['job_id']:
        raise ValueError('Missing job_id')
    event = {**event, 'router_id': router_id}
    job_store.create(event)
    old = job_store.get(event['job_id'])
    if old['status'] in ('completed', 'failed'):
        return old
    job_store.update(event['job_id'], 'running')
    try:
        output = run_command(event['command'], router_id)
        status = 'completed'
    except Exception as exc:
        output = f'{type(exc).__name__}: {exc}'
        status = 'failed'
    job_store.update(event['job_id'], status, output)
    return job_store.get(event['job_id'])


def main():
    if '--ssh-test' in sys.argv:
        idx = sys.argv.index('--ssh-test')
        router_id = sys.argv[idx + 1] if len(sys.argv) > idx + 1 else 'R1'
        print(run_command(COMMANDS['interfaces'], router_id))
        return
    job_store.init_db()
    consumer = KafkaConsumer(
        TOPIC, bootstrap_servers=BOOTSTRAP, group_id='cisco-workers',
        auto_offset_reset='earliest', enable_auto_commit=False,
        max_poll_records=1, max_poll_interval_ms=300000)
    print(f'Listening on {TOPIC} for R1 and R2', flush=True)
    try:
        while True:
            for records in consumer.poll(timeout_ms=1000, max_records=1).values():
                for message in records:
                    try:
                        result = process_event(json.loads(message.value.decode('utf-8')))
                    except (ValueError, UnicodeError) as exc:
                        # Invalid requests cannot execute SSH; record location in the console.
                        print(f'Rejected partition={message.partition} offset={message.offset}: {exc}', flush=True)
                    else:
                        print(json.dumps(result, indent=2), flush=True)
                    # Results persist before acknowledgment; DB failures halt without commit.
                    consumer.commit()
    except KeyboardInterrupt:
        print('Worker stopped', flush=True)
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
