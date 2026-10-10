"""A waiter subscribes without a polling process. Caller persists its own cursor."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'python'))
from membus import Client
from membus_signals import Listener

client=Client(r'\\.\pipe\LOCAL\membus-kv1',transport='pipe')
registration=None
try:
    with Listener() as listener:
        registration=client.subscribe('quota',listener.endpoint,prefix='event/',include_value=True)
        while True:
            change=listener.receive(timeout=60)
            print(change['key'],change['version'],change['value'],flush=True)
            # Save change['cursor'] after successful application handling.
            # On reconnect pass it as cursor=...; refusal23requires reconciliation.
finally:
    try:
        if registration:client.unsubscribe('quota',registration['subscription'])
    finally:client.close()
