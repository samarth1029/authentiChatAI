"""
main code for FastAPI setup
"""
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from app.api import Api
from app.models.models import AppDetails

description = """
API for serving as a chatbot for product verfication🚀
"""

tags_metadata = [
    {
        "name": "default",
        "description": "endpoints for details of app",
    },
]

app = FastAPI(
    title="AuthentiChatAI Cloudbuild",
    description=description,
    version="0.1",
    docs_url="/docs",
)


@app.get(
    "/",
)
def root():
    return {
        "message": "authenti-chatai-cloudbuild using Fast API in Python. Go to <IP>:8000/docs for API-explorer.",
        "errors": None,
    }


@app.get("/appinfo/", tags=["default"])
def get_app_info() -> AppDetails:
    return AppDetails(**Api().get_app_details())


if __name__ == "__main__":
    uvicorn.run("app.main:app", port=8080, reload=True, debug=True, workers=3)
