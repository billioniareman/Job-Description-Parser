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

    def get_tmt_engineers(self):
        """
        Returns a list of dicts with keys: name, email, contact for all engineers in the TMT department.
        """
        try:
            engineers = list(self.collection2.find({'department_name': 'tmt'}))
            result = []
            for engineer in engineers:
                name = f"{engineer.get('first_name', '').strip()} {engineer.get('last_name', '').strip()}".strip()
                email = engineer.get('email', '')
                contact = engineer.get('mobile_number', '')
                result.append({
                    'name': name,
                    'email': email,
                    'contact': contact
                })
            return result
        except Exception as e:
            print(f"Error retrieving TMT engineers: {e}")
            return []

    def save_tmt_engineers_json(self, filename='tmt_engineers.json'):
        """
        Save the TMT engineers list to a clean JSON file.
        """
        try:
            data = self.get_tmt_engineers()
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            print(f"TMT engineers data saved to {filename}")
        except Exception as e:
            print(f"Error saving TMT engineers to JSON: {e}")

    def get_joined_project_engineer_data(self, filename='joined_project_engineer.json'):
        """
        Join projects and engineers collections. For each project, retrieve client_name, no_requirements, unique skills (from both primary and secondary), no_of_rounds,
        and for each talent_manager_associate, find the engineer whose _id matches talent_associate_id. Save the result as a clean JSON file.
        """
        try:
            projects = list(self.collection.find())
            result = []
            for project in projects:
                client_name = project.get('client_name', '')
                no_requirements = project.get('no_requirements', '')
                primary_skills = [s.get('skill') for s in project.get('primary_skills', [])]
                secondary_skills = [s.get('skill') for s in project.get('secondary_skills', [])]
                # Combine and deduplicate skills
                skills = list(set(primary_skills + secondary_skills))
                no_of_rounds = project.get('no_of_rounds', '')
                tma_list = project.get('talent_manager_associate', [])
                for tma in tma_list:
                    talent_associate_id = tma.get('talent_associate_id')
                    if not talent_associate_id:
                        continue
                    try:
                        engineer = self.collection2.find_one({'_id': talent_associate_id if isinstance(talent_associate_id, ObjectId) else ObjectId(talent_associate_id)})
                    except Exception:
                        continue
                    if not engineer:
                        continue
                    
                    result.append({
                        'client_name': client_name,
                        'no_requirements': no_requirements,
                        'skills': skills,
                        'no_of_rounds': no_of_rounds,
                        'engineer_id': str(engineer.get('_id')),
                        'engineer_name': f"{engineer.get('first_name', '').strip()} {engineer.get('last_name', '').strip()}".strip(),
                        'engineer_email': engineer.get('email', ''),
                        'engineer_contact': engineer.get('mobile_number', '')
                    })
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=4)
            print(f"Joined project and engineer data saved to {filename}")
        except Exception as e:
            print(f"Error joining and saving data: {e}")

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
