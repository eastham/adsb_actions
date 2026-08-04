#!/usr/bin/env python3
"""Simple HTTP server with Range request support (required for PMTiles).
Usage: python src/hotspots/serve.py [directory] [port]
"""
import http.server
import os
import signal
import sys
import threading


class RangeHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def send_head(self):
        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            return super().send_head()

        # Serve pre-compressed .gz files with Content-Encoding so the browser
        # decompresses transparently (no client-side changes needed).
        if path.endswith(".gz"):
            inner = path[:-3]  # e.g. tracks.json.gz -> tracks.json
            ctype = self.guess_type(inner)
            file_size = os.path.getsize(path)
            f = open(path, "rb")
            self.send_response(200)
            self.send_header("Content-type", ctype)
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(file_size))
            self.end_headers()
            return f

        range_header = self.headers.get("Range")
        if not range_header:
            return super().send_head()

        # Parse "bytes=start-end"
        try:
            range_spec = range_header.replace("bytes=", "").strip()
            parts = range_spec.split("-")
            file_size = os.path.getsize(path)
            start = int(parts[0]) if parts[0] else 0
            end = int(parts[1]) if parts[1] else file_size - 1
            end = min(end, file_size - 1)
            length = end - start + 1
        except (ValueError, IndexError):
            return super().send_head()

        f = open(path, "rb")
        f.seek(start)

        self.send_response(206)
        ctype = self.guess_type(path)
        self.send_header("Content-type", ctype)
        self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
        self.send_header("Content-Length", str(length))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        # Return only the requested range
        remaining = length
        buf_size = 64 * 1024
        try:
            while remaining > 0:
                chunk = f.read(min(buf_size, remaining))
                if not chunk:
                    break
                self.wfile.write(chunk)
                remaining -= len(chunk)
        except BrokenPipeError:
            pass
        finally:
            f.close()
        return None

    def handle_one_request(self):
        # Browsers routinely abandon in-flight responses (panning a map cancels
        # pending tile fetches), which surfaces as a broken pipe / reset while
        # writing the body. Swallow it: the connection is already gone, and the
        # default behavior is to dump a full traceback for every such request.
        try:
            super().handle_one_request()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def log_error(self, format, *args):
        # Suppress broken-pipe noise from browsers closing connections early.
        if len(args) > 0 and "Broken pipe" in str(args[-1]):
            return
        super().log_error(format, *args)

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()


class QuietHTTPServer(http.server.HTTPServer):
    def handle_error(self, request, client_address):
        # Client disconnects aren't server errors; don't print a traceback.
        if isinstance(sys.exc_info()[1], (BrokenPipeError, ConnectionResetError)):
            return
        super().handle_error(request, client_address)


if __name__ == "__main__":
    # Serve from project root (not maps subdir) so that relative paths like
    # ../../tiles/traffic in the HTML resolve correctly.
    directory = sys.argv[1] if len(sys.argv) > 1 else "."
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8080
    os.chdir(directory)
    print(f"Serving {os.getcwd()} on http://localhost:{port}")
    server = QuietHTTPServer(("", port), RangeHTTPRequestHandler)

    # Explicitly shut down on SIGINT/SIGTERM. Relying on KeyboardInterrupt
    # bubbling out of serve_forever() is unreliable: a Ctrl-C arriving while
    # blocked in the poll loop often gets lost, so the first ^C does nothing.
    # A handler that calls shutdown() from another thread stops it cleanly.
    def _stop(signum, frame):
        print("\nStopped.")
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    server.serve_forever()
