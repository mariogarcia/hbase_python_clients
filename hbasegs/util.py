from datetime import datetime


def time_it(func):
    def wrapper(*args, **kwargs):
        started_at  = datetime.now()

        result = func(*args, **kwargs)

        finished_at = datetime.now()
        elapsed_time= finished_at - started_at
        seconds     = elapsed_time.seconds

        print("secs: {}".format(seconds))
        return result
    return wrapper