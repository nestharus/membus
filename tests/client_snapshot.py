"""Unknown initial capture outcomes propagate; known-token retries retain the view."""
import pathlib,sys
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'python'))
from membus import Client,Refusal,lp
class Channel:
    def close(self):pass
class Fake(Client):
    def __init__(self):
        self.timeout=10;self.channel=Channel();self.calls=0;self.reconnects=0
    def exchange(self,*args):
        self.calls+=1;raise ConnectionError('injected lost reply')
    def connect(self,*args):
        self.channel=Channel();self.reconnects+=1
for op in (10,11,12,14,15):
    c=Fake()
    try:c.call(op,'ledger',b'' if op==10 else lp(b''))
    except ConnectionError:pass
    else:raise AssertionError('initial capture transport error hidden')
    assert c.calls==1 and c.reconnects==0 and c.channel is None
for op in (11,12,14,15):
    c=Fake()
    try:c.call(op,'ledger',lp(b't'*16))
    except ConnectionError:pass
    else:raise AssertionError('transport error hidden')
    assert c.calls==2 and c.reconnects==1
class Scope(Client):
    def __init__(self):self.released=[]
    def snapshot_open(self,*args,**kw):return b't'*16
    def snapshot_release(self,ns,token,**kw):self.released.append((ns,token))
c=Scope()
try:
    with c.snapshot('ledger') as token:
        assert token==b't'*16
        raise RuntimeError('consumer failed')
except RuntimeError:pass
assert c.released==[('ledger',b't'*16)]
print('PASS: initial captures send once; known tokens retry same view; context releases on error')
