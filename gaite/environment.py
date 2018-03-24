

class Env:

    def __init__(self):
        self._shared_state = dict()
        self._shared_state['env'] = None
        self._shared_state['verbosity'] = False
