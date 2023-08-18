/etc/docker/daemon.json:
  file.managed:
    - source: salt://docker/daemon.json
    - makedirs: True
