from .events import parse,iso
from .attribution import calculate

def analyze(data):
    cutoff,gap,lookback,events,duplicates=parse(data)
    result=calculate([e for e in events if e['at']<=cutoff],gap,lookback)
    result.update(as_of=iso(cutoff),duplicate_events=duplicates,future_event_ids=sorted(e['id'] for e in events if e['at']>cutoff))
    return result
