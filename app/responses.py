import json
from fastapi.responses import JSONResponse

class PrettyJSONResponse(JSONResponse):
   def render(self, content):
       return json.dumps(
           content,
           ensure_ascii=False,
           allow_nan=False,
           indent=2,
           separators=(",", ": ")
       ).encode("utf-8")
