import subprocess, socket, signal, sys
exe = sys.argv[1]
for args in (["-1"], ["65536"], ["12oops"], ["0", "invalid"], ["0", "127.0.0.1", "extra"]):
    p = subprocess.run([exe, *args], capture_output=True, timeout=5)
    assert p.returncode != 0, args
for sig in (signal.SIGINT, signal.SIGTERM):
    p = subprocess.Popen([exe, "0"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        line = p.stdout.readline().strip()
        assert line.startswith("READY 127.0.0.1:"), line
        port = int(line.split(":")[1].split()[0])
        with socket.create_connection(("127.0.0.1", port), timeout=3) as s:
            s.sendall(b"cli works\n")
            assert s.recv(100) == b"CLI WORKS\n"
        p.send_signal(sig)
        out, err = p.communicate(timeout=5)
        assert p.returncode == 0 and "STOPPED" in out, (out, err)
    finally:
        if p.poll() is None:
            p.kill()
            p.wait()
print("CLI: 5 invalid-input checks and 2 signal shutdown/network checks passed")
