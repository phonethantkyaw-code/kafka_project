import os
from pathlib import Path
from dotenv import load_dotenv
ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / '.env')
BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
TOPIC = os.getenv('KAFKA_TOPIC', 'router-commands')
DB_PATH = os.getenv('JOBS_DB', str(ROOT / 'jobs.sqlite3'))
COMMANDS = {'interfaces': 'show ip interface brief', 'version': 'show version', 'routes': 'show ip route'}
ROUTERS = {
    'R1': os.getenv('R1_HOST', os.getenv('ROUTER_HOST', '192.168.221.134')),
    'R2': os.getenv('R2_HOST', '192.168.221.135'),
}
def router_login(router_id):
    return {
        'hostname': ROUTERS[router_id],
        'port': int(os.getenv(router_id + '_PORT', os.getenv('ROUTER_PORT', '22'))),
        'username': os.getenv(router_id + '_USERNAME', os.getenv('ROUTER_USERNAME', 'admin')),
        'password': os.getenv(router_id + '_PASSWORD', os.environ.get('ROUTER_PASSWORD', '')),
    }
