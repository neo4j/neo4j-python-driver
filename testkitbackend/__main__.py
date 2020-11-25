from .server import Server

if __name__ == "__main__":
    server = Server(("0.0.0.0", 9876))
    while True:
        server.handle_request()
