"""A failed hello releases its channel and propagates the original error."""
import pathlib,sys
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'python'))
import membus
closed=[]
class Channel:
    def close(self):closed.append(True)
def fail(self,*args):
    raise TimeoutError('injected hello read deadline')
old_pipe=membus.Pipe;old_exchange=membus.Client.exchange
try:
    membus.Pipe=lambda *args:Channel()
    membus.Client.exchange=fail
    client=membus.Client.__new__(membus.Client)
    try:client.__init__('unused','pipe')
    except TimeoutError:pass
    else:raise AssertionError('injected error not propagated')
    assert closed==[True] and client.channel is None
    # Direct reconnect failure must release too, not just constructor failure.
    try:client.connect(membus.deadline_ms())
    except TimeoutError:pass
    else:raise AssertionError('injected reconnect error not propagated')
    assert closed==[True,True] and client.channel is None
    print('PASS: failed hello and reconnect close channels and preserve TimeoutError')
finally:
    membus.Pipe=old_pipe;membus.Client.exchange=old_exchange
