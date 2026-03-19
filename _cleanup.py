#!/usr/bin/env python3
"""Temporary cleanup script — run with: python3 _cleanup.py"""
if __name__ == "__main__":
    import os, shutil
    xlsx = "/sessions/beautiful-kind-babbage/mnt/JANSA VISASIST/output/master_dataset.xlsx"
    if os.path.exists(xlsx):
        os.remove(xlsx)
        print("Removed xlsx")
    base = "/sessions/beautiful-kind-babbage/mnt/JANSA VISASIST"
    for root, dirs, files in os.walk(base):
        for d in list(dirs):
            if d == "__pycache__":
                p = os.path.join(root, d)
                shutil.rmtree(p, ignore_errors=True)
                print(f"Removed {p}")
    for d in ["/sessions/beautiful-kind-babbage/.cache", "/sessions/beautiful-kind-babbage/.npm-global"]:
        if os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)
            print(f"Removed {d}")
    tmp = "/sessions/beautiful-kind-babbage/tmp"
    for item in os.listdir(tmp):
        p = os.path.join(tmp, item)
        try:
            if os.path.isdir(p): shutil.rmtree(p, ignore_errors=True)
            else: os.remove(p)
        except: pass
    print("Cleaned tmp")
    st = os.statvfs("/sessions/beautiful-kind-babbage")
    print(f"Free: {st.f_bavail * st.f_frsize / 1024 / 1024:.1f} MB")
