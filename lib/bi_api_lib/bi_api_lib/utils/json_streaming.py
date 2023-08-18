from __future__ import annotations

from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from aiohttp import web


class FakeJSONList:
    """ A helper for making a valid JSON list by streamed pieces """

    start_piece = '[\n'
    delimiter_piece = ',\n'
    end_piece = '\n]\n'

    def __init__(self) -> None:
        self.events: List[str] = []
        self.started = False
        self.had_events = False
        self.ended = False

    def feed(self, event: str) -> int:
        self.events.append(event)
        return len(self.events)

    def flush(self) -> str:
        assert not self.ended

        result_pieces: List[str] = []

        if not self.started:
            result_pieces.append(self.start_piece)
            self.started = True

        events = self.events
        self.events = []
        for event in events:
            if self.had_events:
                result_pieces.append(self.delimiter_piece)
            else:
                self.had_events = True
            result_pieces.append(event)

        return ''.join(result_pieces)

    def finalize(self) -> str:
        assert not self.ended
        result = self.flush() + self.end_piece
        self.ended = True
        return result


class FakeJSONListResponse:
    """ FakeJSONList wrapper to feed an aiohttp.web.StreamResponse with chunks """

    min_events = 10
    encoding = 'utf-8'

    def __init__(self, response: 'web.StreamResponse') -> None:
        self.response = response
        self.fake_json_list = FakeJSONList()

    async def write(self, data: str) -> None:
        return await self.response.write(data.encode(self.encoding))

    async def feed(self, event: str) -> None:
        size = self.fake_json_list.feed(event)
        if size >= self.min_events:
            return await self.write(self.fake_json_list.flush())
        return None

    async def finalize(self) -> None:
        return await self.write(self.fake_json_list.finalize())
