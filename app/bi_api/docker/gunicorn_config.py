# higher than the supposed default nginx default of 75 seconds
keepalive = 80
timeout = 90

# # Configuration in commandline:
# import os
# log_config = os.environ['GUNICORN_LOG_CONFIG']
# bind = "[::]:" + os.environ.get('GUNICORN_LISTEN_PORT', '80')
# max_requests = int(os.environ.get('GUNICORN_MAX_RQ', '5000'))
# max_requests_jitter = int(os.environ.get('GUNICORN_MAX_RQ_JITTER', '500'))
# worker_class = 'aiohttp.GunicornWebWorker'
# workers = int(os.environ.get('GUNICORN_WORKERS_COUNT', '1'))

# # Other notable configuration:
# worker_connections = 1000
# threads = 2
# proc_name = 'app01'
# backlog = 2048
# accesslog = 'access.log'
# errorlog = 'error.log'
# user = 'ubuntu'
# group = 'ubuntu'
