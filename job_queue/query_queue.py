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
        
    

        