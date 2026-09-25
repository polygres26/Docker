#!/usr/bin/env python3
import socket, sys, threading, time

LISTEN_PORT = int(sys.argv[1]); TARGET_HOST = sys.argv[2]; TARGET_PORT = int(sys.argv[3]); LOGFILE = sys.argv[4]

log = open(LOGFILE, "a")

def pump(src, dst, label):
    while True:
        try:
            data = src.recv(65536)
        except Exception:
            break
        if not data:
            break
        log.write(f"=== {label} ({len(data)} bytes) t={time.time()} ===\n")
        for off in range(0, len(data), 16):
            chunk = data[off:off+16]
            hexstr = ' '.join(f'{b:02x}' for b in chunk)
            log.write(f"{off:04x}  {hexstr}\n")
        log.write("\n")
        log.flush()
        try:
            dst.sendall(data)
        except Exception:
            break

def handle(client_sock):
    target_sock = socket.create_connection((TARGET_HOST, TARGET_PORT))
    t1 = threading.Thread(target=pump, args=(client_sock, target_sock, "CLIENT->TARGET"))
    t2 = threading.Thread(target=pump, args=(target_sock, client_sock, "TARGET->CLIENT"))
    t1.start(); t2.start()
    t1.join(); t2.join()
    client_sock.close(); target_sock.close()

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("0.0.0.0", LISTEN_PORT)); srv.listen(5)
print(f"bolt proxy listening on {LISTEN_PORT} -> {TARGET_HOST}:{TARGET_PORT}", flush=True)
while True:
    c, _ = srv.accept()
    threading.Thread(target=handle, args=(c,)).start()
