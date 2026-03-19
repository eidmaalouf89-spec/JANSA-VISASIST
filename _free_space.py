#!/usr/bin/env python3
"""Emergency disk space cleanup for session filesystem."""
import os, shutil

session = "/sessions/upbeat-nice-darwin"

# Clean __pycache__
for root, dirs, files in os.walk(os.path.join(session, "mnt")):
    for d in list(dirs):
        if d == "__pycache__":
            p = os.path.join(root, d)
            shutil.rmtree(p, ignore_errors=True)
            print(f"Removed {p}")

# Clean .cache, .npm-global
for d in [".cache", ".npm-global", ".local"]:
    p = os.path.join(session, d)
    if os.path.isdir(p):
        shutil.rmtree(p, ignore_errors=True)
        print(f"Removed {p}")

# Clean tmp
tmp = os.path.join(session, "tmp")
if os.path.isdir(tmp):
    for item in os.listdir(tmp):
        p = os.path.join(tmp, item)
        try:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            else:
                os.remove(p)
        except:
            pass
    print("Cleaned tmp")

# Check space
try:
    st = os.statvfs(session)
    print(f"Free: {st.f_bavail * st.f_frsize / 1024 / 1024:.1f} MB")
except:
    print("Could not check free space")
