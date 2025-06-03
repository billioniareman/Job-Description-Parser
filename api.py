from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import json
from typing import List
import data
import os

app = FastAPI()

@app.get("/recommend-engineers")
def recommend_engineers_api(
    input_client: List[str] = Query(..., description="List of client names"),
    input_skills: List[str] = Query(..., description="List of skills (repeat or comma-separated)"),
    top_n: int = 5
):
    # Flatten comma-separated skills if needed
    skills_flat = []
    for skill in input_skills:
        skills_flat.extend([s.strip() for s in skill.split(",") if s.strip()])
    with open('tmt_engineers_with_projects.json', 'r', encoding='utf-8') as f:
        engineers = json.load(f)
    recommendations = data.recommend_engineers(input_client, skills_flat, engineers, top_n=top_n)
    return JSONResponse(content=recommendations)

@app.on_event("startup")
def ensure_tmt_engineers_with_projects():
    if not os.path.exists('tmt_engineers_with_projects.json'):
        from tmadata import TMAData
        # You may want to set your mongo_uri here or use an environment variable
        mongo_uri = os.environ.get('MONGO_URI', 'mongodb+srv://admin:4sZf4uIsrlO6GCoV@staging-cluster.olgilw6.mongodb.net/user_management')
        tma_data = TMAData(mongo_uri)
        tma_data.get_tmt_engineers_with_projects('tmt_engineers_with_projects.json')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
