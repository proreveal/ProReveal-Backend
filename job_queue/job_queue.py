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

    def reschedule(self, alternate):
        if alternate:
            def cmp(a, b):
                if a.query.priority != b.query.priority:
                    return a.query.priority - b.query.priority 

                if a.state != b.state:
                    if a.state == JobState.Paused:
                        return 1
                    return -1

                a_index = a.index - a.query.num_processed_blocks
                b_index = b.index - b.query.num_processed_blocks

                if a_index != b_index:
                    return a_index - b_index

                a_order = a.query.order
                b_order = b.query.order

                return a_order - b_order

            self.queue.sort(key=cmp_to_key(cmp))
        else:
            def cmp(a, b):
                if a.query.priority != b.query.priority:
                    return a.query.priority - b.query.priority 

                if a.state != b.state:
                    if a.state == JobState.Paused:
                        return 1
                    return -1

                a_order = a.query.order
                b_order = b.query.order

                return a_order - b_order

            self.queue.sort(key=cmp_to_key(cmp))
            


