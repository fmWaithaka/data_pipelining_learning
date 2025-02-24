import os

DB_DETAILS = {
    'dev': {
        'SOURCE_DB': {
            'DB_TYPE': 'mysql',
            'DB_HOST': 'host.docker.internal',
            'DB_NAME': 'retail_db',
            'DB_USER': os.environ.get('SOURCE_DB_USER', 'retail_user'),
            'DB_PASS': os.environ.get('SOURCE_DB_PASS', 'itversity')
        },
        'TARGET_DB': {
            'DB_TYPE': 'postgres',
            'DB_HOST': 'host.docker.internal',
            'DB_NAME': 'retail_db',
            'DB_USER': os.environ.get('TARGET_DB_USER', 'retail_user'),
            'DB_PASS': os.environ.get('TARGET_DB_PASS', 'itversity')
        }
    }
}
