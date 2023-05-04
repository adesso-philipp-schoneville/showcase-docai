from utils.logging import logger


def cloud_function_template(data=None):
    logger.info(f"function was called with {data}")
    return data
