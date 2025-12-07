from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from srs_logic import get_next_phrase, process_answer

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/next_phrase")
async def api_next_phrase(user_id: int = 1):
    phrase = get_next_phrase(user_id)
    if phrase is None:
        return JSONResponse({"status": "NO_PHRASE"})
    return phrase  # dict: {phrase_id, phrase, target_word, state_counts, mode, ...}

@app.post("/api/answer")
async def api_answer(data: dict):
    # data: {user_id, phrase_id, result: "again"/"hard"/"good"/"easy"}
    process_answer(**data)
    return {"status": "OK"}
