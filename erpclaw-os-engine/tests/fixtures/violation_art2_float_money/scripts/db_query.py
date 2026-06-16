import os, sys
sys.path.insert(0, os.path.join(os.path.expanduser(os.environ.get("ERPCLAW_HOME", "~/.openclaw/erpclaw")), "lib"))
from erpclaw_lib.response import ok, err

def main():
    ok({"message": "ok"})

if __name__ == "__main__":
    main()
