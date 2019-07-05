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

    def remove_by_client_socket_id(self, client_socket_id):
        count = len(self.queue)
        self.queue = [job for job in self.queue if job.query.client_socket_id != client_socket_id]
        return count - len(self.queue)

    def remove_by_query_id(self, query_id):
        self.queue = [job for job in self.queue if job.query.id != query_id]

    def pause_by_query_id(self, query_id):
        for job in self.queue:
            if job.query.id == query_id:
                job.pause()

    def resume_by_query_id(self, query_id):
        for job in self.queue:
            if job.query.id == query_id:
                job.resume()

    def reschedule(self, queries, mode = 'roundrobin'):

        order = {}
        for i, query in enumerate(queries):
            order[query['id']] = i

        if mode == 'roundrobin':
            def cmp(a, b):
                if a.query.priority != b.query.priority:
                    return a.query.priority - b.query.priority 

                if a.state != b.state:
                    if a.state == JobState.Paused:
                        return 1
                    return -1

                if a.index != b.index:
                    return a.index - b.index

                ordera = order[a.query.id] if a.query.id in order else order[a.query.client_id]
                orderb = order[b.query.id] if b.query.id in order else order[b.query.client_id]

                return ordera - orderb

            self.queue.sort(key=cmp_to_key(cmp))
        else:
            def cmp(a, b):
                if a.query.priority != b.query.priority:
                    return a.query.priority - b.query.priority 

                if a.state != b.state:
                    if a.state == JobState.Paused:
                        return 1
                    return -1

                ordera = order[a.query.id] if a.query.id in order else order[a.query.client_id]
                orderb = order[b.query.id] if b.query.id in order else order[b.query.client_id]

                return ordera - orderb

            self.queue.sort(key=cmp_to_key(cmp))
            


