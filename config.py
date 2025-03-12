import os

DB_DETAILS = {
    'dev': {
        'RETAIL_DB': {
            'DB_TYPE': 'mysql',
            # 'DB_HOST': 'host.docker.internal',
            'DB_HOST': '127.0.0.1',
            'DB_NAME': 'retail_db',
            'DB_USER': os.environ.get('RETAIL_DB_USER'),
            'DB_PASS': os.environ.get('RETAIL_DB_PASS')
        },
        'CUSTOMER_DB': {
            'DB_TYPE': 'postgres',
            # 'DB_HOST': 'host.docker.internal',
            'DB_HOST': '127.0.0.1',
            'DB_NAME': 'retail_db',
            'DB_USER': os.environ.get('CUSTOMER_DB_USER'),
            'DB_PASS': os.environ.get('CUSTOMER_DB_PASS')
        }
    }
}
