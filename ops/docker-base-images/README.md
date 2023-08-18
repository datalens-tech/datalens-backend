
## About ##

Collection of docker layers to build apps upon.


## Layers ##


### base_python ###

python3.7-stretch + yandex repositories


### phusion/baseimage ###

https://github.com/phusion/baseimage-docker

  * ubuntu 18.04
  * entrypoint (init, a small python script)
  * syslog
  * runit
  * ... other stuff.


### base_ubuntu_runit ###

phusion/baseimage + yandex repositories + (python3.8 or python3.9 or python3.10 or whatever)

Also sets LC_ALL and LANG


### bi_base_mess ###

base_ubuntu_runit + bi-specific stuff

  * logs
    * syslog config
    * logrotate with configs
    * push-client with a runit service
    * juggler checks
    * ...
  * mssql and oracle libs
  * postgresql and clickhouse repositories


Used in BI apps: DLS, bi-api, bi-...


### bi_initdb ###

ubuntu + oracle & mssql commandline clients
