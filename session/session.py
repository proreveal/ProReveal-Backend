import random
import string

class Session:
    @staticmethod
    def generate_code():
        return ''.join(random.choice(string.ascii_uppercase) for x in range(3))
        
    def __init__(self):
        self.code = Session.generate_code()
        self.sids = []        

        self.alternate = False

    def to_json(self):
        return {
            'code': self.code,
            'engineType': 'remote'
        }
    
    def leave_sid(self, sid):
        self.sids = [s for s in self.sids if s != sid]

    def enter_sid(self, sid):
        if sid not in self.sids:
            self.sids.append(sid)


