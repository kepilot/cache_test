from logging import Logger

from starlette.requests import Request


async def get_log(request: Request) -> Logger:
    """Retrieve de api logger"""
    log: Logger = request.app.state.logger
    return log
