from datetime import datetime,timezone
import re
from .common import require,ident,number,rows

def instant(value):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})',value):
        raise ValueError('timestamp requires ISO seconds and explicit timezone')
    if value[-6:-5] in ('+','-'):
        h,m=map(int,value[-5:].split(':'))
        if h>23 or m>59:raise ValueError('invalid UTC offset')
    return datetime.fromisoformat(value.replace('Z','+00:00')).astimezone(timezone.utc)

def iso(value):return value.isoformat().replace('+00:00','Z')

def parse(data):
    require(data,'as_of session_gap_seconds lookback_seconds events');cutoff=instant(data['as_of'])
    gap=number(data['session_gap_seconds'],1,86400);lookback=number(data['lookback_seconds'],1,31*86400)
    events={};duplicates=0
    for e in rows(data['events'],10000):
        require(e,'id user at kind channel amount_cents');eid=ident(e['id']);user=ident(e['user']);at=instant(e['at'])
        if e['kind']=='touch':
            ident(e['channel'])
            if e['channel']=='unattributed':raise ValueError('reserved attribution channel')
            if e['amount_cents'] is not None:raise ValueError('touch cannot have amount')
        elif e['kind']=='purchase':
            if e['channel'] is not None:raise ValueError('purchase channel must be null')
            number(e['amount_cents'])
        else:raise ValueError('unknown event kind')
        normalized=dict(e,user=user,at=at)
        if eid in events:
            if events[eid]!=normalized:raise ValueError('conflicting event ID')
            duplicates+=1;continue
        events[eid]=normalized
    ordered=sorted(events.values(),key=lambda e:(e['user'],e['at'],e['id']))
    return cutoff,gap,lookback,ordered,duplicates
