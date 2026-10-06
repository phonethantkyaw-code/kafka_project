import json
from contextlib import asynccontextmanager
from uuid import uuid4
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError
from settings import ROOT, BOOTSTRAP, TOPIC, COMMANDS, ROUTERS
import job_store

@asynccontextmanager
async def lifespan(app):
    job_store.init_db()
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        try:
            admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()
    app.state.producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP, acks='all',
        value_serializer=lambda v: json.dumps(v).encode(),
        max_block_ms=10000, request_timeout_ms=10000)
    try:
        yield
    finally:
        app.state.producer.close(timeout=10)

app = FastAPI(title='Router Command Lab', lifespan=lifespan)

class Job(BaseModel):
    router_id: str = 'R1'
    action: str = 'interfaces'

@app.get('/')
def home():
    return FileResponse(ROOT / 'index.html')

@app.get('/options')
def options():
    return {'routers': ROUTERS, 'commands': COMMANDS}

@app.post('/jobs', status_code=202)
def submit(job: Job):
    if job.router_id not in ROUTERS or job.action not in COMMANDS:
        raise HTTPException(400, 'Choose a listed router and command')
    event = {'job_id': str(uuid4()), 'router_id': job.router_id, 'command': COMMANDS[job.action]}
    job_store.create(event)
    try:
        app.state.producer.send(TOPIC, key=job.router_id.encode(), value=event).get(timeout=15)
    except KafkaError:
        # Delivery may have succeeded despite a timeout; do not overwrite a worker result.
        job_store.update(event['job_id'], 'delivery_unknown', 'Kafka delivery was not confirmed. Check this job before retrying.', only_queued=True)
        raise HTTPException(503, {'message': 'Delivery not confirmed', 'job_id': event['job_id']})
    return {'status': 'queued', **event}

@app.get('/jobs')
def recent():
    return job_store.recent()

@app.get('/jobs/{job_id}')
def result(job_id: str):
    job = job_store.get(job_id)
    if job is None:
        raise HTTPException(404, 'Job not found')
    return job
