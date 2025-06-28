import orjson
from fastapi.responses import JSONResponse

class PrettyJSONResponse(JSONResponse):
    def render(self, content: any) -> bytes:
        return orjson.dumps(
            content,
            option=orjson.OPT_INDENT_2
        )
