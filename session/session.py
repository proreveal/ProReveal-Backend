import random
import string

class Session:
    @staticmethod
    def generate_code():
        return ''.join(random.choice(string.ascii_uppercase) for x in range(3))
        
    def __init__(self):
        self.code = Session.generate_code()

    def json(self):
        return {
            'code': self.code
        }

