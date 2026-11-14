from bisect import bisect_left,bisect_right
from collections import defaultdict
from datetime import timedelta
from .events import iso

def calculate(events,gap,lookback):
    users=defaultdict(list)
    for e in events:users[e['user']].append(e)
    sessions=[];conversions=[];totals={'first_non_direct':defaultdict(lambda:{'conversions':0,'revenue_cents':0}),
                                    'last_non_direct':defaultdict(lambda:{'conversions':0,'revenue_cents':0})}
    for user,items in sorted(users.items()):
        touches=[e for e in items if e['kind']=='touch'];times=[e['at'] for e in touches];session=None;last=None
        non_direct=[e for e in touches if e['channel']!='direct'];non_direct_times=[e['at'] for e in non_direct]
        for e in items:
            if last is None or (e['at']-last).total_seconds()>gap:
                session={'id':e['id'],'user':user,'start':iso(e['at']),'end':iso(e['at']),'event_ids':[],'purchases':0,'revenue_cents':0}
                sessions.append(session)
            session['end']=iso(e['at']);session['event_ids'].append(e['id']);last=e['at']
            if e['kind']!='purchase':continue
            session['purchases']+=1;session['revenue_cents']+=e['amount_cents']
            start=e['at']-timedelta(seconds=lookback);left=bisect_left(non_direct_times,start);right=bisect_right(non_direct_times,e['at'])
            if left<right:first=non_direct[left];last_touch=non_direct[right-1]
            else:
                left=bisect_left(times,start);right=bisect_right(times,e['at'])
                first=touches[left] if left<right else None;last_touch=touches[right-1] if left<right else None
            chosen={'first_non_direct':first,'last_non_direct':last_touch};models={}
            for model,touch in chosen.items():
                channel=touch['channel'] if touch else 'unattributed';models[model]={'channel':channel,'touch_id':touch['id'] if touch else None}
                totals[model][channel]['conversions']+=1;totals[model][channel]['revenue_cents']+=e['amount_cents']
            conversions.append({'id':e['id'],'user':user,'at':iso(e['at']),'session_id':session['id'],'revenue_cents':e['amount_cents'],'models':models})
    return {'sessions':sessions,'conversions':conversions,'session_count':len(sessions),'conversion_count':len(conversions),
            'total_revenue_cents':sum(c['revenue_cents'] for c in conversions),
            'channel_totals':{model:{channel:dict(v) for channel,v in sorted(values.items())} for model,values in totals.items()}}
