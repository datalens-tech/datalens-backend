#!/usr/bin/env python3

import os
from pathlib import Path


def main():
    tmp = Path("/tmp")
    tmp_sorted = sorted(Path(tmp).iterdir(), key=os.path.getmtime, reverse=True)
    for pth in tmp_sorted:
        if pth.is_dir() and pth.name.startswith("ssh-"):
            for child in pth.iterdir():
                if child.name.startswith("agent."):
                    print(str(child))
                    return


if __name__ == '__main__':
    main()
