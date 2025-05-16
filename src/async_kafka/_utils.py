import sys
from collections.abc import Callable, Coroutine, Generator, Mapping
from concurrent.futures import Future
from functools import wraps
from typing import Any, ParamSpec, TypeVar

import anyio.from_thread

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

_P = ParamSpec('_P')
_T = TypeVar('_T')
_K = TypeVar('_K')
_V = TypeVar('_V')


def async_to_sync(
    func: Callable[_P, Coroutine[Any, Any, _T]],
) -> Callable[_P, _T]:
    @wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        return anyio.from_thread.run(lambda: func(*args, **kwargs))

    return wrapper


async def wrap_concurrent_future(future: Future[_T]) -> _T:
    if future.done() or future.exception():
        return future.result()
    event = anyio.Event()
    future.add_done_callback(lambda _: event.set())
    await event.wait()
    return future.result()


class FuturesDict(dict[_K, Coroutine[Any, Any, _V]]):
    def __await__(self) -> Generator[Any, Any, dict[_K, _V]]:
        return self.__await().__await__()

    async def __await(self) -> dict[_K, _V]:
        result: dict[_K, _V] = {}
        for key, coro in self.items():
            result[key] = await coro
        return result

    @classmethod
    def from_concurrent_futures(cls, futures: Mapping[_K, Future[_V]]) -> Self:
        return cls({key: wrap_concurrent_future(value) for key, value in futures.items()})
