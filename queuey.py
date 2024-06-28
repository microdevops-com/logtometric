# Thanks to:
#https://lemanchet.fr/articles/adding-asyncio-support-to-a-threaded-task-queue.html
#https://www.youtube.com/watch?v=x1ndXuw7S0s
#https://github.com/NicolasLM/spinach/commit/56e9343b294e1799b7be365917677ac7814f1f2d

import asyncio
from collections import deque
from concurrent.futures import Future
from typing import Tuple, Optional, Any, Deque


class Queuey:
    def __init__(self):
        self._items: Deque[Any] = deque()
        self._getters: Deque[Future] = deque()
        self._processed = 0
        self._values = []

    def _get_noblock(self) -> Tuple[Optional[Any], Optional[Future]]:
        if self._items:
            return self._items.popleft(), None

        else:
            fut = Future()
            self._getters.append(fut)
            return None, fut

    def _put_noblock(self, item: Any) -> Optional[Future]:
        self._items.append(item)
        if self._getters:
            self._getters.popleft().set_result(self._items.popleft())

    def sget(self) -> Any:
        item, fut = self._get_noblock()
        if fut:
            item = fut.result()
        return item

    async def get(self) -> Any:
        item, fut = self._get_noblock()
        if fut:
            item = await asyncio.wait_for(asyncio.wrap_future(fut), None)
        return item

    def put(self, item: Any) -> None:
        self._put_noblock(item)

    def len_getters(self) -> int:
        return len(self._getters)

    def len_setters(self) -> int:
        return len(self._getters)
