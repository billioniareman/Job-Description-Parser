import json


def jaccard_similarity(list1, list2):
    set1 = set(x.lower().strip() for x in list1 if x)
    set2 = set(x.lower().strip() for x in list2 if x)
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union != 0 else 0

def recommend_engineers(input_client, input_skills, developers, top_n=5, w_client=0.7, w_skill=0.3):
    reccommendations = []
    input_skills_set = set(s.lower().strip() for s in input_skills if s)
    input_client_set = set(c.lower().strip() for c in input_client if c)

    for dev in developers:
        dev_clients = [c.lower().strip() for c in dev.get('clients', []) if c]
        dev_skills = [s.lower().strip() for s in dev.get('skills', []) if s]
        client_sim = jaccard_similarity(input_client_set, dev_clients)
        skill_sim = jaccard_similarity(input_skills_set, dev_skills)
        matched_clients = list(input_client_set & set(dev_clients))
        matched_skills = list(input_skills_set & set(dev_skills))
        skill_match_percentage = (len(matched_skills) / len(input_skills_set) * 100) if input_skills_set else 0

        reccommendations.append({
            'engineer_id': dev.get('engineer_id'),
            'engineer_name': dev.get('engineer_name'),
            'score': round((w_client * client_sim) + (w_skill * skill_sim), 2),
            'matched_clients': matched_clients,
            'matched_skills': matched_skills,
            'experience': dev.get('experience', 0),
            'engineer_email': dev.get('engineer_email', ''),
            'skill_match_percentage': round(skill_match_percentage, 2),
        })
    return sorted(reccommendations, key=lambda x: x['score'], reverse=True)[:top_n]


def recommend_tma(input_client, input_skills, engineers, top_n=3, w_client=0.7, w_skill=0.3):
    recommendations = []
    input_skills_set = set(s.lower().strip() for s in input_skills if s)
    input_client_set = set(c.lower().strip() for c in input_client if c)
    for eng in engineers:
        eng_clients = [c.lower().strip() for c in eng.get('clients', []) if c]
        eng_skills = [s.lower().strip() for s in eng.get('skills', []) if s]
        client_sim = jaccard_similarity(input_client_set, eng_clients)
        skill_sim = jaccard_similarity(input_skills_set, eng_skills)
        matched_clients = list(input_client_set & set(eng_clients))
        matched_skills = list(input_skills_set & set(eng_skills))
        skill_match_percentage = (len(matched_skills) / len(input_skills_set) * 100) if input_skills_set else 0
        recommendations.append({
            'engineer_id': eng.get('engineer_id'),
            'engineer_name': eng.get('engineer_name'),
            'score': round((w_client * client_sim) + (w_skill * skill_sim), 2),
            'matched_clients': matched_clients,
            'matched_skills': matched_skills,
            'skill_match_percentage': round(skill_match_percentage, 2)
        })
    return sorted(recommendations, key=lambda x: x['score'], reverse=True)[:top_n]
