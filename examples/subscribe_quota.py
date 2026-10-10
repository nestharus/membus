"""Blocking quota waiter with quiet timeouts and reconnect from its last cursor."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'python'))
from membus import Client, EpochChanged, Refusal
from membus_signals import Listener

client=Client(r'\\.\pipe\LOCAL\membus-kv1',transport='pipe')
cursor=None
try:
    while True:
        registration=None
        try:
            with Listener() as listener:
                try:
                    registration=client.subscribe('quota',listener.endpoint,prefix='event/',
                                                  include_value=True,cursor=cursor)
                except Refusal as error:
                    if error.code!=23:raise
                    cursor=None
                    registration=client.subscribe('quota',listener.endpoint,prefix='event/',include_value=True)
                    # Register live before reconciling state to cover concurrent writes.
                    print('reconcile',client.get('quota','open'),client.get('quota','resets'),flush=True)
                cursor=registration['cursor']
                while True:
                    try:change=listener.receive(timeout=60)
                    except TimeoutError:continue  # quiet may last hours
                    print(change['key'],change['version'],change['value'],flush=True)
                    cursor=change['cursor']  # save only after successful handling
        except EpochChanged:
            continue  # old cursor forces23and reconciliation in the next registration
        except (OSError,ConnectionError):
            client.close()
            continue
        finally:
            if registration:
                try:client.unsubscribe('quota',registration['subscription'])
                except (OSError,EpochChanged,Refusal):pass
finally:client.close()
