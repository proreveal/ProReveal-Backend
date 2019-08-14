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

        # most recent to the front
        self.queries = []

        self.job_queue = JobQueue()
        self.alternate = False

    def to_json(self):
        return {
            'code': self.code,
            'engineType': 'remote',
            'alternate': self.alternate,
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
        self.queries.insert(0, query)

        for i, q in enumerate(self.queries):
            q.order = i

        self.job_queue.append(query.get_jobs())
        self.job_queue.reschedule(self.alternate)

    def pause_query(self, query):
        query.pause()
        self.job_queue.pause_by_query_id(query.id)
        self.job_queue.reschedule(self.alternate)

    def resume_query(self, query):
        query.resume()
        self.job_queue.resume_by_query_id(query.id)
        self.job_queue.reschedule(self.alternate)

    def remove_query(self, query):
        self.queries = [q for q in self.queries if q != query]        
        self.job_queue.remove_by_query_id(query.id)
        self.job_queue.reschedule(self.alternate)

    def reschedule(self):
        self.job_queue.reschedule(self.alternate)

    def query_state_to_json(self):
        return {q.id: {
            'order': q.order,
            'state': q.state.value
         } for q in self.queries}