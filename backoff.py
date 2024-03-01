import random
from time import sleep
from botocore.exceptions import ClientError


def backoff_with_jitter(base, cap, retries):
    backoff = min(cap, base * 2 ** retries)
    jittered_backoff = backoff / 2 + random.uniform(0.0, backoff / 2)
    print(f"backing off {jittered_backoff} seconds.")
    sleep(jittered_backoff)


def aws_client_backoff(
    base: float = 0.5,
    cap: int = 120,
    max_retry: int = 6
):
    def decorator(func):
        def wrapper(*args, **kwargs):
            retry_count = 0
            done = False
            while not done and retry_count <= max_retry:
                try:
                    if retry_count:
                        print("retrying...")
                    func(*args, **kwargs)
                    done = True
                except ClientError as exception:
                    retry_count += 1
                    if exception.response['Error']['Code'] == "ThrottlingException":
                        print(f"Throttling occurred, backing off...")
                        backoff_with_jitter(base, cap, retry_count)
                    else:
                        print(f"exception.response['Error']['Code'], occured...")
            if not done:
                raise ClientError(f"Unable to process {func.__name__} with {max_retry} retries\n"
                                  f"args:{args}, \n"
                                  f"kwargs:{kwargs}")
        return wrapper
    return decorator
