import logging
import pandas as pd
import time
from functools import wraps


def timer(original_function):
    ''' Wrapper: Measures time execution'''

    @wraps(original_function)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = original_function(*args, **kwargs)
        end = time.perf_count() - start
        if isinstance(result, pd.DataFrame):
            print(f'{result.shape[0]} registros leídos en {end} segundos')
        else:
            print(f'{original_function.__name__} ejecutado en {end} segundos')
        return result

    return wrapper


def logger(original_function):
    ''' Wrapper: Generates logs on function call'''
    logging.basicConfig(
        filename=f'{original_function.__name__}.log', leve=logging.INFO)

    @wraps(original_function)
    def wrapper(*args, **kwargs):
        logging.info(
            'Ran with args {} and kwargs {}'.format(args, kwargs)
        )
        return original_function(*args, **kwargs)

    return wrapper
