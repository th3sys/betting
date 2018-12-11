import bz2
import datetime
import json
import logging
import os
from dateutil import tz
from betfairlightweight import StreamListener
from betfairlightweight.streaming.stream import MarketStream

"""
Data needs to be downloaded from:
    https://historicdata.betfair.com
"""

# setup logging
logger = logging.getLogger('app')
hdlr = logging.FileHandler('logging.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatter)
logger.addHandler(consoleHandler)
logger.setLevel(logging.INFO)


# create trading instance (app key must be activated for streaming)
# username = os.environ.get('username')
# trading = betfairlightweight.APIClient(username)
# trading.login()


class HistoricalStream(MarketStream):
    # create custom listener and stream

    def __init__(self, listener):
        super(HistoricalStream, self).__init__(listener)
        with open('output.txt', 'w') as output:
            output.write('Time,MarketId,Status,Inplay,SelectionId,LastPriceTraded\n')

    def on_process(self, market_books):
        with open('output.txt', 'a') as output:
            for market_book in market_books:
                for runner in market_book.runners:
                    # how to get runner details from the market definition
                    market_def = market_book.market_definition
                    runners_dict = {(runner.selection_id, runner.handicap): runner for runner in market_def.runners}
                    runner_def = runners_dict.get(
                        (runner.selection_id, runner.handicap)
                    )

                    output.write('%s,%s,%s,%s,%s,%s\n' % (
                        market_book.publish_time, market_book.market_id, market_book.status, market_book.inplay,
                        runner.selection_id, runner.last_price_traded or ''
                    ))


class HistoricalListener(StreamListener):
    def _add_stream(self, unique_id, stream_type):
        if stream_type == 'marketSubscription':
            return HistoricalStream(self)


for root, dirs, files in os.walk('data'):
    for file in files:
        fn = '%s%s%s' % (root, os.sep, file)
        logger.debug(fn)
        if file.endswith('bz2') and file.replace('bz2', 'json') not in files:
            source_file = bz2.BZ2File(fn, "r")
            lines = ''
            for line in source_file:
                lines += line.decode()
            source_file.close()
            # os.remove(fn)
            with open(fn.replace('.bz2', '.json'), 'w') as f:
                f.write(lines)

logger.info('unpacking is done')


class Tester(object):
    def __init__(self, mId, folder):
        self.marketId = mId
        self.eventTypeId = '7'
        self.marketType = 'WIN'
        self.numberOfActiveRunners = 0
        self.settledTime = None
        self.winnerId = None
        self.predictedId = None
        self.threshold = 1.8
        self.startTime = None
        self.timeOfDecision = None
        self.folder = folder
        self.inPlay = False
        self.utc = tz.tzutc()
        self.london = tz.gettz('Europe/London')
        self.pt = datetime.datetime(1, 1, 1).replace(tzinfo=self.utc)

    def results(self):
        result = None if self.predictedId is None else self.predictedId == self.winnerId
        return '%s,%s,%s,%s,%s,%s,%s,%s,%s' % (self.startTime, self.folder,
                                               self.marketId, self.numberOfActiveRunners, self.settledTime,
                                               self.winnerId, self.predictedId, self.timeOfDecision, result)

    def receive_event(self, event):
        mcs = self.validate(event)
        pt = int(event['pt'])
        date = datetime.datetime.fromtimestamp(pt / 1000.0).replace(tzinfo=self.london).astimezone(self.utc)
        if self.startTime is not None and self.startTime > date:
            return True  # market is not open yet
        for mc in mcs:
            if 'marketDefinition' in mc:
                md = mc['marketDefinition']
                if 'inPlay' in md:
                    self.inPlay = md['inPlay']
                if 'numberOfActiveRunners' in md and self.numberOfActiveRunners == 0:
                    self.numberOfActiveRunners = int(md['numberOfActiveRunners'])
                    self.startTime = datetime.datetime.strptime(md['marketTime'], '%Y-%m-%dT%H:%M:%S.%fZ')\
                        .replace(tzinfo=self.utc)
                if md['status'] == 'CLOSED':
                    self.settledTime = md['settledTime']
                    w = [i for i in md['runners'] if i['status'] == 'WINNER']
                    if len(w) == 1:
                        self.winnerId = w[0]['id']
            elif self.predictedId is None and 'rc' in mc and self.inPlay:
                for rc in mc['rc']:
                    if 'ltp' in rc and 0 < rc['ltp'] < self.threshold:
                        self.predictedId = rc['id']
                        self.timeOfDecision = date
        return True

    def validate(self, event):
        pt = int(event['pt'])
        date = datetime.datetime.fromtimestamp(pt / 1000.0).replace(tzinfo=self.london).astimezone(self.utc)
        if date <= self.pt:
            logger.error('%s is out of order' % pt)
            # raise Exception('Out of order')
        self.pt = date

        if 'mc' in event:
            for mc in event['mc']:
                if 'marketDefinition' in mc and 'eventTypeId' in mc['marketDefinition']:
                    if mc['marketDefinition']['eventTypeId'] != self.eventTypeId:
                        raise Exception('eventTypeId')
                if 'marketDefinition' in mc and 'marketType' in mc['marketDefinition']:
                    if mc['marketDefinition']['marketType'] != self.marketType:
                        raise Exception('marketType')
                if mc['id'] != self.marketId:
                    raise Exception('%s id does not match %s' % (mc['id'], self.marketId))
        return event['mc']


o = open('output.csv', 'w')
o.write('StartTime,Folder, MarketId,Runners,SettledTime,Winner,Predicted,TimeOfDecision, Result\n')
for root, dirs, files in os.walk('data'):
    for file in files:
        if file.endswith('json'):
            fn = '%s%s%s' % (root, os.sep, file)
            parts = root.split('/')
            tester = Tester(file.replace(".json", ''), parts[4])
            logger.info('processing %s' % fn)
            with open(fn, 'r') as f:
                for line in f:
                    tester.receive_event(json.loads(line))
            o.write('%s\n' % tester.results())
o.close()
# create listener
# listener = HistoricalListener(max_latency=1e100)
# create historical stream, update directory to file location
# stream = trading.streaming.create_historical_stream(directory=fn, listener=listener)
# start stream
# stream.start(_async=False)
