import datetime as dt
from database import auto_session

SOURCES = ["Capterra", "GooglePlay", "AppleStore", "Trustpilot", "G2", "Twitter", "Facebook"]
COMPANIES = ["Paypal", "Monzo", "Monese", "Mongo", "Amazon", "AirPay", "Momo"]
STATUSES = ["running", "waiting", "finished", "completed with error"]
ETL_STEP_TYPES = ['crawl', 'preprocess', 'translate', 'nlp', 'load']

def now():
  return dt.datetime.today().strftime('%Y%m%d')


@auto_session
def dump_data(session=None, model=None, data=None):
    if session is None or model is None or data is None:
        return

    fields = data[0]
    for index, line in enumerate(data[1:]):
        assert len(line) == len(fields), f"record at {index + 1} inconsistent"

    for line in data[1:]:
        record = {}
        for i in range(len(fields)):
            record[fields[i]] = line[i]
        instance = model(**record)
        session.add(instance)

    session.commit()



class MessageMock():
    def __init__(self, data):
        self.data = data


class PubSubMessageMock():
    def __init__(self, data):
        self.data = data
    
    @property
    def message(self):
        return MessageMock(self.data)
