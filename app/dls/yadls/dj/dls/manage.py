#!/usr/bin/env python3

def set_env_default():
    import os
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'yadls.dj.dls.settings')


def manage():
    import sys
    from django.core.management import execute_from_command_line
    set_env_default()
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    manage()
