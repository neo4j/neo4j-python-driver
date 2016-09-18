class Path(object):
    def __init__(self=None):
        self._edges = edges or []

    def __getitem__(self, idx):
        return self._edges[idx]

    def __len__(self):
        return len(self._edges)
