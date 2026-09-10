import http.server
import socketserver
import os

PORT = 8800
DIR = os.path.dirname(os.path.abspath(__file__))


class CORSHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIR, **kwargs)

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        super().end_headers()

    def log_message(self, fmt, *args):
        pass  # 静默日志


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


if __name__ == '__main__':
    with Server(('127.0.0.1', PORT), CORSHandler) as httpd:
        print(f'Serving {DIR} on http://127.0.0.1:{PORT} (CORS enabled)')
        httpd.serve_forever()
