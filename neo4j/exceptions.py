

class ProtocolError(Exception):
    """ Raised when an unexpected or unsupported protocol event occurs.
    """

    pass


class CypherError(Exception):
    """ Raised when the Cypher engine returns an error to the client.
    """

    code = None
    message = None

    def __init__(self, data):
        super(CypherError, self).__init__(data.get("message"))
        for key, value in data.items():
            if not key.startswith("_"):
                setattr(self, key, value)
