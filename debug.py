import json
import datetime
import functools
from dateutil import tz

utc = tz.tzutc()
london = tz.gettz('Europe/London')
search = 10422505
threshold = 1.2
startTime = None
o = open('debug.json', 'w')
c = open('debug.csv', 'w')
inPlay = False
latestOdds = {}
infoSet = False
header = 'pt,time'
with open('data/xds/historic/ADVANCED/28938306/1.149082188.json', 'r') as f:
    for line in f:
        event = json.loads(line)
        pt = int(event['pt'])
        date = datetime.datetime.fromtimestamp(pt / 1000.0).replace(tzinfo=london).astimezone(utc)
        if 'mc' in event:
            for mc in event['mc']:
                if 'marketDefinition' in mc:
                    o.write(line)
                    md = mc['marketDefinition']
                    if md['status'] == 'OPEN' and not infoSet:
                        for runner in md['runners']:
                            latestOdds[runner['id']] = 0
                        for i in sorted(list(latestOdds.keys())):
                            header = '%s,%s' % (header, i)
                        c.write('%s,\n' % header)
                        infoSet = True
                    if 'inPlay' in md:
                        inPlay = md['inPlay']
                    if md['status'] == 'OPEN' and startTime is None:
                        startTime = datetime.datetime.strptime(md['marketTime'], '%Y-%m-%dT%H:%M:%S.%fZ')\
                            .replace(tzinfo=utc)
                if 'rc' in mc and startTime is not None and date > startTime and inPlay:
                    for rc in mc['rc']:
                        if rc['id'] in latestOdds:
                            latestOdds[rc['id']] = rc['ltp']
                        if rc['id'] == search and 'ltp' in rc and rc['ltp'] != 0:
                            o.write(line)
                    line = '%s,%s' % (pt, date)
                    for i in sorted(list(latestOdds.keys())):
                        line = '%s,%s' % (line, latestOdds[i])
                    c.write('%s,%s\n' % (line, len([x for x in latestOdds.values() if 0 < x < threshold]) > 0))
o.close()
c.close()
