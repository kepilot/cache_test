from fastapi import APIRouter, Request, Response
from fastapi.routing import APIRoute

from typing import Callable
from fastapi.responses import JSONResponse


class GeneralExceptionRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            try:
                response: Response = await original_route_handler(request)
                return response
            except Exception as e:
                if isinstance(e, ValueError):
                    return JSONResponse(
                        content={"detail": "value_error!"}, status_code=403
                    )
                if isinstance(e, KeyError):
                    return JSONResponse(
                        content={"detail": "key_error!"}, status_code=405
                    )

        return custom_route_handler


router = APIRouter(route_class=GeneralExceptionRoute)


@router.get("/test/value_error")
async def test_value():
    raise ValueError("test")


@router.get("/test/key_error")
async def test_key():
    raise KeyError("test")