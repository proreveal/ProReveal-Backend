from functools import cmp_to_key
from query import JobState

class JobQueue:    
    def __init__(self):
        self.queue = []
            
    def append(self, jobs):
        self.queue += jobs

    def __len__(self):
        return len(self.queue)

    def peep(self):
        if len(self) > 0:
            return self.queue[0]

        return None

    def dequeue(self):
        if len(self) == 0:
            return None
        
        first = self.queue[0]
        self.queue = self.queue[1:]

        return first

    def remove_by_client_id(self, client_id):
        self.queue = [job for job in self.queue if job.client_id != client_id]

    def remove_by_query_id(self, query_id):
        self.queue = [job for job in self.queue if job.query.id != query_id]

    def pause_by_query_id(self, query_id):
        for job in self.queue:
            if job.query.id == query_id:
                job.pause()

        self.reorder()

    def resume_by_query_id(self, query_id):
        for job in self.queue:
            if job.query.id == query_id:
                job.resume()
        
        self.reorder()

    def reorder(self):
        def cmp(a, b):
            if a.state != b.state:
                if a.state == JobState.Paused:
                    return 1                
                return -1

            return 0

        self.queue.sort(key=cmp_to_key(cmp))

