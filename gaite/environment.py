
class Env(object):

    def __init__(self):
        self.shared_state = dict()
        self.shared_state['env'] = None
        self.shared_state['verbosity'] = False
        self.shared_state['recruiter'] = None
        self.shared_state['start_date'] = None
        self.shared_state['end_date'] = None
