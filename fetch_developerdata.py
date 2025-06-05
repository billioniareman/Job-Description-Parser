import psycopg2
from datetime import datetime, date
from decimal import Decimal
from pymongo import MongoClient
from bson import ObjectId
import json

pg_host = "ss-stag-dev-db-paij5iezee.supersourcing.com"
pg_dbname = "job_interaction_staging"
pg_user = "bluerangZbEbusr"
pg_pass = "Year#2015eba"

def sanitize_postgres_row(row_dict):
    for key, value in row_dict.items():
        if isinstance(value, (datetime, date)):
            row_dict[key] = value.isoformat()
        elif isinstance(value, Decimal):
            row_dict[key] = float(value)
    return row_dict

def get_postgresql_data_by_query(host, dbname, user, password, sql_query):
    """
    Connects to Postgres and runs the given SQL query.
    Returns a list of sanitized dicts.
    """
    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password
    )
    cursor = conn.cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]
    data = [
        sanitize_postgres_row(dict(zip(cols, row)))
        for row in rows
    ]
    cursor.close()
    conn.close()
    return data

sql_table = """
      SELECT * FROM public.job_interactions
    """

class Developer:
    def __init__(self, mongo_uri):
        self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.db = self.client['user_management']
        self.collection = self.db['engineerbasicinfos']

    def get_developer(self):
        developers = []
        # Only fetch engineers with a non-empty resume
        engineers = list(self.collection.find({
            'resume': {'$exists': True, '$ne': None, '$ne': ''}
        }))
        # Fetch all job_interactions once
        job_interactions = get_postgresql_data_by_query(
            pg_host, pg_dbname, pg_user, pg_pass, sql_table
        )
        for engineer in engineers:
            engineer_id = str(engineer.get('_id'))
            first_name = engineer.get('first_name', '').strip()
            last_name = engineer.get('last_name', '').strip()
            engineer_name = f"{first_name} {last_name}".strip()
            engineer_email = engineer.get('email', '')
            engineer_contact = engineer.get('mobile_number', '')

            # Find all projects where this engineer is a job_interaction
            projects = [ji for ji in job_interactions if str(ji.get('engineer_id')) == engineer_id]
            if projects:
                exp_val = projects[0].get('engineer_experience_years')
                experience = int(exp_val) if exp_val not in (None, '') else 0
            else:
                experience = 0
            client_names = set()
            all_skills = set()
            for project in projects:
                client_name = project.get('client_name')
                if client_name:  # Only add non-empty client names
                    client_names.add(client_name)
                # Safely iterate engineer_primary_skill if it's a list
                engineer_primary_skill = project.get('engineer_primary_skill')
                if isinstance(engineer_primary_skill, list):
                    for skill in engineer_primary_skill:
                        skill_name = skill.get('skill_name')
                        if skill_name:
                            all_skills.add(skill_name)
            developers.append({
                'engineer_id': engineer_id,
                'engineer_name': engineer_name,
                'engineer_email': engineer_email,
                'engineer_contact': engineer_contact,
                'clients': list(client_names),
                'skills': list(all_skills),
                'experience': experience,
            })
        return developers

    def to_json(self, developers, filename='developers.json'):
        """ 
            Saves the list of developers with their projects and skills to a JSON file.
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(developers, f, ensure_ascii=False, indent=4)
        print(f"TMT engineers with projects and skills saved to {filename}")

# Example usage:
# if __name__ == "__main__":
#     mongo_uri = "your_mongo_uri"
#     developer = Developer(mongo_uri)
#     developers = developer.get_developer()
#     developer.to_json(developers)
