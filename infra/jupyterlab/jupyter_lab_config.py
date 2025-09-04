import os

c.ServerApp.ip = '0.0.0.0'
c.ServerApp.port = 8888
c.ServerApp.open_browser = False
c.ServerApp.allow_root = True
c.ServerApp.token = ''
c.ServerApp.password = ''
c.ServerApp.allow_origin = '*'
c.ServerApp.disable_check_xsrf = True
c.LabApp.extensions_in_dev_mode = True
c.LanguageServerManager.language_servers = {
    'pylsp': {
        'version': 2,
        'argv': ['pylsp'],
        'languages': ['python'],
        'mime_types': ['text/x-python', 'text/python'],
        'settings': {
            'pylsp': {
                'plugins': {
                    'pycodestyle': {'enabled': False},
                    'mccabe': {'enabled': False},
                    'pyflakes': {'enabled': False},
                    'flake8': {'enabled': True},
                    'pylint': {'enabled': True},
                    'autopep8': {'enabled': False},
                    'yapf': {'enabled': False},
                    'black': {'enabled': True},
                    'mypy': {'enabled': True},
                }
            }
        }
    }
}
c.GitConfig.actions = ['add', 'commit', 'pull', 'push']
os.environ.setdefault('TRINO_HOST', 'trino')
os.environ.setdefault('TRINO_PORT', '8080')
os.environ.setdefault('CLICKHOUSE_HOST', 'clickhouse')
os.environ.setdefault('CLICKHOUSE_PORT', '8123')
os.environ.setdefault('POSTGRES_HOST', 'postgres')
os.environ.setdefault('POSTGRES_PORT', '5432')
os.environ.setdefault('REDIS_HOST', 'redis')
os.environ.setdefault('REDIS_PORT', '6379')
os.environ.setdefault('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
os.environ.setdefault('MINIO_ENDPOINT', 'http://minio:9000')
c.ServerApp.max_buffer_size = 1024 * 1024 * 1024  # 1GB
c.ZMQChannelsHandler.kernel_ws_protocol = None
c.LabApp.collaborative = True
c.Application.log_level = 'INFO'
