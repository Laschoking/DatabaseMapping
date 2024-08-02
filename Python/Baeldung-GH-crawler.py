import requests
from collections import defaultdict

# Constants
GITHUB_API_URL = "https://api.github.com"
GITHUB_TOKEN = "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"  # Replace with your GitHub token
REPO_OWNER = "eugenp"
REPO_NAME = "tutorials"
PATH = "core-java-modules"

# Headers for authentication
HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def get_commit_shas():
    """
    Get all commit SHAs related to a specific path.
    """
    commits_url = f"{GITHUB_API_URL}/repos/{REPO_OWNER}/{REPO_NAME}/commits"
    params = {"path": PATH, "per_page": 100}
    commit_shas = []

    while True:
        response = requests.get(commits_url, headers=HEADERS, params=params)
        response.raise_for_status()
        commits = response.json()
        if not commits:
            break
        commit_shas.extend([commit['sha'] for commit in commits])
        # Pagination
        if 'next' in response.links:
            commits_url = response.links['next']['url']
        else:
            break

    return commit_shas

def get_changed_files(commit_shas):
    """
    Retrieve all files that have been changed in the provided commits.
    """
    files_changed = defaultdict(int)

    for sha in commit_shas:
        commit_url = f"{GITHUB_API_URL}/repos/{REPO_OWNER}/{REPO_NAME}/commits/{sha}"
        response = requests.get(commit_url, headers=HEADERS)
        response.raise_for_status()
        commit = response.json()

        for file in commit.get("files", []):
            filename = file["filename"]
            if filename.startswith(PATH):
                files_changed[filename] += 1

    return files_changed

def main():
    commit_shas = get_commit_shas()
    files_changed = get_changed_files(commit_shas)

    # Filter files that have been changed at least twice
    changed_at_least_twice = [filename for filename, count in files_changed.items() if count >= 2]

    # Output results
    print("Files changed at least twice:")
    for filename in changed_at_least_twice:
        print(filename)

if __name__ == "__main__":
    main()
