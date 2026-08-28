import socket, threading, sys, time
LISTEN_PORT = int(sys.argv[1]); TARGET_HOST = sys.argv[2]; TARGET_PORT = int(sys.argv[3]); LOGFILE = sys.argv[4]
lock = threading.Lock()
log = open(LOGFILE, "a", buffering=1)
def hexdump(label, data):
    with lock:
        log.write(f"\n=== {label} ({len(data)} bytes) t={time.time():.3f} ===\n")
        for i in range(0, len(data), 16):
            chunk = data[i:i+16]
            hexs = " ".join(f"{b:02x}" for b in chunk)
            asci = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
            log.write(f"{i:04x}  {hexs:<48}  {asci}\n")
        log.flush()
def pipe(src, dst, label):
    try:
        while True:
            data = src.recv(65536)
            if not data: break
            hexdump(label, data)
            dst.sendall(data)
    except Exception as e:
        with lock: log.write(f"\n[{label} pipe closed: {e}]\n")
    finally:
        try: dst.shutdown(socket.SHUT_WR)
        except Exception: pass
def handle(client_sock, addr):
    target_sock = socket.create_connection((TARGET_HOST, TARGET_PORT))
    t1 = threading.Thread(target=pipe, args=(client_sock, target_sock, "CLIENT->TARGET"))
    t2 = threading.Thread(target=pipe, args=(target_sock, client_sock, "TARGET->CLIENT"))
    t1.start(); t2.start(); t1.join(); t2.join()
    client_sock.close(); target_sock.close()
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("0.0.0.0", LISTEN_PORT)); srv.listen(5)
print(f"proxy listening on {LISTEN_PORT} -> {TARGET_HOST}:{TARGET_PORT}", flush=True)
while True:
    c, a = srv.accept()
    threading.Thread(target=handle, args=(c, a), daemon=True).start()
