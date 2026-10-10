"""Decoder for the MBT1 envelope carried as a membus.change/1 value."""
from membus import Decoder

def decode_timer(body):
    d=Decoder(body)
    if d.read(4)!=b'MBT1' or d.unpack('<H')!=(1,):raise ValueError('unsupported timer protocol')
    timer_id=d.blob().decode('utf8')
    names=('generation','index','nominal_ms','due_ms','draw_ms','missed_count')
    result=dict(zip(names,d.unpack('<QQQQqQ')))
    result.update(timer_id=timer_id,payload=d.blob())
    if d.pos!=len(body) or not result['missed_count']:raise ValueError('invalid timer envelope')
    result['due_id']=(result['generation'],result['index'])
    return result
