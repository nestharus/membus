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

# A decoded but invalid new hello must not replace the previous validated epoch.
import struct
old_pipe=membus.Pipe;old_exchange=membus.Client.exchange
try:
    membus.Pipe=lambda *args:Channel()
    client=membus.Client.__new__(membus.Client)
    client.transport='pipe';client.address='unused';client.allow_mem=False;client.channel=None;client.epoch=1
    header=struct.pack('<QIIHBQ',2,membus.MAX_FRAME,membus.MAX_VALUE,membus.MAX_BATCH,0,86400000)
    membus.Client.exchange=lambda *args:header+struct.pack('<H',0)
    try:client.connect(membus.deadline_ms())
    except ValueError:pass
    else:raise AssertionError('invalid hello accepted')
    assert client.epoch==1 and client.channel is None
    offered=header+struct.pack('<H',len(membus.CODES))+b''.join(struct.pack('<HB',n,int(t))+membus.lp(name) for n,(name,t) in membus.CODES.items())
    membus.Client.exchange=lambda *args:offered
    try:client.connect(membus.deadline_ms())
    except membus.EpochChanged as e:assert (e.previous,e.current)==(1,2)
    else:raise AssertionError('new epoch hidden by failed earlier hello')
    assert client.epoch==2 and client.channel is not None
    client.close()
finally:
    membus.Pipe=old_pipe;membus.Client.exchange=old_exchange
print('PASS: failed hello validation preserves epoch; later valid new epoch signals EpochChanged')
