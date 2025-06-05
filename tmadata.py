from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.objectid import ObjectId
import json
import datetime

def convert_mongo_types(obj):
    """Recursively convert ObjectId and datetime to string for JSON serialization."""
    if isinstance(obj, dict):
        return {k: convert_mongo_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_mongo_types(i) for i in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    else:
        return obj




class TMAData:
    def __init__(self, mongo_uri):
        self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db1 = self.client['job_management']
        self.collection = self.db1['projects']
        self.db2 = self.client['user_management']
        self.collection2 = self.db2['engineerbasicinfos']
        print("Connected to MongoDB successfully")

    def get_tmt_engineers_with_projects(self, filename='tmt_engineers_with_projects.json'):
        """
        For each engineer in the TMT department, find all projects where they are a talent_manager_associate.
        Output: For each engineer, include their info, a list of unique client_names, and a unique set of all their skills (from all their projects).
        """
        try:
            engineers = list(self.collection2.find({'department_name': 'tmt'}))
            result = []
            for engineer in engineers:
                engineer_id = engineer.get('_id')
                engineer_name = f"{engineer.get('first_name', '').strip()} {engineer.get('last_name', '').strip()}".strip()
                engineer_email = engineer.get('email', '')
                engineer_contact = engineer.get('mobile_number', '')
                # Find all projects where this engineer is a talent_manager_associate
                projects = list(self.collection.find({'talent_manager_associate.talent_associate_id': engineer_id}))
                client_names = set()
                all_skills = set()
                for project in projects:
                    client_names.add(project.get('client_name', ''))
                    primary_skills = [s.get('skill') for s in project.get('primary_skills', [])]
                    secondary_skills = [s.get('skill') for s in project.get('secondary_skills', [])]
                    all_skills.update(primary_skills)
                    all_skills.update(secondary_skills)
                result.append({
                    'engineer_id': str(engineer_id),
                    'engineer_name': engineer_name,
                    'engineer_email': engineer_email,
                    'engineer_contact': engineer_contact,
                    'clients': list(client_names),
                    'skills': list(all_skills)
                })
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=4)
            print(f"TMT engineers with projects and skills saved to {filename}")
        except Exception as e:
            print(f"Error joining engineers with projects: {e}")


# if __name__ == "__main__":
#     mongo_uri = "mongodb+srv://admin:4sZf4uIsrlO6GCoV@staging-cluster.olgilw6.mongodb.net/user_management"  # <-- replace with your actual Mongo URI
#     tma_data = TMAData(mongo_uri)
#     tma_data.get_tmt_engineers_with_projects('tmt_engineers_with_projects.json')
#     print("TMA data processing completed.")