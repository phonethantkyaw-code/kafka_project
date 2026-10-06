"""Local result storage shared by backend and worker on this Ubuntu machine."""
import sqlite3
from settings import DB_PATH

def connect():
    db = sqlite3.connect(DB_PATH, timeout=20)
    db.row_factory = sqlite3.Row
    return db

def init_db():
    with connect() as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('''CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY, router_id TEXT NOT NULL, command TEXT NOT NULL,
            status TEXT NOT NULL, output TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)''')

def create(event):
    with connect() as db:
        db.execute('INSERT OR IGNORE INTO jobs(job_id,router_id,command,status) VALUES(?,?,?,?)',
                   (event['job_id'], event['router_id'], event['command'], 'queued'))

def update(job_id, status, output='', only_queued=False):
    with connect() as db:
        sql = 'UPDATE jobs SET status=?,output=?,updated_at=CURRENT_TIMESTAMP WHERE job_id=?'
        if only_queued:
            sql += " AND status='queued'"
        db.execute(sql, (status, output, job_id))

def get(job_id):
    with connect() as db:
        row = db.execute('SELECT * FROM jobs WHERE job_id=?', (job_id,)).fetchone()
        return dict(row) if row else None

def recent():
    with connect() as db:
        return [dict(row) for row in db.execute('SELECT * FROM jobs ORDER BY created_at DESC, rowid DESC LIMIT 20')]
