import requests

from app2.core.config import load_settings


class FootballApiClient:
    def __init__(self):
        settings = load_settings()
        self.base_url = "https://api.football-data.org/v4"
        self.headers = {"X-Auth-Token": settings.football_api_key}

    def fetch_competitions(self):
        url = f"{self.base_url}/competitions"
        response = requests.get(url, headers=self.headers)
        status_code = response.status_code
        try:
            payload = response.json()
        except Exception:
            payload = None
        return status_code, payload

    def fetch_areas(self):
        url = f"{self.base_url}/areas"
        response = requests.get(url, headers=self.headers)
        status_code = response.status_code
        try:
            payload = response.json()
        except Exception:
            payload = None
        return status_code, payload

    def fetch_competition_teams(self, competition_id: int, season: int):
        url = f"{self.base_url}/competitions/{competition_id}/teams"
        response = requests.get(url, headers=self.headers, params={"season": season})
        status_code = response.status_code
        try:
            payload = response.json()
        except Exception:
            payload = None
        return status_code, payload

    def fetch_competition_scorers(self, competition_id: int, season: int, limit: int = 50):
        url = f"{self.base_url}/competitions/{competition_id}/scorers"
        response = requests.get(url, headers=self.headers, params={"season": season, "limit": limit})
        status_code = response.status_code
        try:
            payload = response.json()
        except Exception:
            payload = None
        return status_code, payload

    def fetch_competition_matches(self, competition_id: int, season: int):
        url = f"{self.base_url}/competitions/{competition_id}/matches"
        response = requests.get(url, headers=self.headers, params={"season": season})
        status_code = response.status_code
        try:
            payload = response.json()
        except Exception:
            payload = None
        return status_code, payload

    def fetch_competition_standings(self, competition_id: int, season: int):
        url = f"{self.base_url}/competitions/{competition_id}/standings"
        response = requests.get(url, headers=self.headers, params={"season": season})
        status_code = response.status_code
        try:
            payload = response.json()
        except Exception:
            payload = None
        return status_code, payload
