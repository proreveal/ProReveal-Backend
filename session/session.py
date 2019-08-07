import random
import string
from job_queue import JobQueue

class Session:
    @staticmethod
    def generate_code():
        return ''.join(random.choice(string.ascii_uppercase) for x in range(3))
        
    def __init__(self):
        self.code = Session.generate_code()
        self.sids = [] 

        # most recent to the end
        self.queries = []

        self.job_queue = JobQueue()
        self.alternate = False
        self.queue_mode = 'roundrobin'

    def to_json(self):
        return {
            'code': self.code,
            'engineType': 'remote',
            'queries': [q.to_json() for q in self.queries]
        }
    
    def leave_sid(self, sid):
        self.sids = [s for s in self.sids if s != sid]

    def enter_sid(self, sid):
        if sid not in self.sids:
            self.sids.append(sid)

    def get_query(self, query_id):
        for q in self.queries:
            if q.id == query_id:
                return q

        return None

    def add_query(self, query):
        self.queries.append(query)
        self.job_queue.append(query.get_jobs())
        self.job_queue.reschedule(self.queries, self.queue_mode)

    def pause_query(self, query):
        query.pause()
        self.job_queue.pause_by_query_id(query.id)
        self.job_queue.reschedule(self.queries, self.queue_mode)

    def resume_query(self, query):
        query.resume()
        self.job_queue.resume_by_query_id(query.id)
        self.job_queue.reschedule(self.queries, self.queue_mode)

    def remove_query(self, query):
        self.queries = [q for q in self.queries if q != query]        
        self.job_queue.remove_by_query_id(query.id)
        self.job_queue.reschedule(self.queries, self.queue_mode)