import pathlib as Path
import requests
import random
import subprocess
import os
import json

# GitHub API token
GITHUB_TOKEN = "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"
HEADERS = {
    'Authorization': f'token {GITHUB_TOKEN}'
}


def search_java_repositories():
    url = "https://api.github.com/search/repositories"
    query = "language:Java"
    params = {
        "q": query,
        "sort": "stars",
        "order": "desc",
        "per_page": 100
    }
    response = requests.get(url, headers=HEADERS, params=params)
    return response.json()['items']


def get_commits(repo_full_name):
    url = f"https://api.github.com/repos/{repo_full_name}/commits"
    response = requests.get(url, headers=HEADERS)
    return response.json()


def run_refactoringminer(repo_full_name, commit_sha):
    json_dir = Path.Path("/home/kotname/Documents/Diplom/Code/DatabaseMapping/src/Git-Crawler/json-files/")
    json_file = json_dir.joinpath(f"{repo_full_name.replace('/', '_')}_{commit_sha}.json")
    repo_url = f"https://github.com/{repo_full_name}.git"
    cmd = [
        "./RefactoringMiner",
        "-gc",
        repo_url,
        commit_sha,
        "10",  # timeout
        "-json",
        str(json_file)
    ]
    try:
        result = subprocess.run(cmd, cwd=os.path.expanduser(
            '/home/kotname/Documents/Diplom/Code/RefactoringMiner-3.0.7/bin'), check=True)
        # Check for errors
        if result.returncode != 0:
            print(f"Error: RefactoringMiner returned non-zero exit code {result.returncode}")
            print(f"stderr: {result.stderr}")
            print(f"stdout: {result.stdout}")
        else:
            print(f"RefactoringMiner succeeded for {commit_sha}")
            print(f"stdout: {result.stdout}")

    except subprocess.CalledProcessError as e:
        print(f"Subprocess failed with error: {e}")
        print(f"stderr: {e.stderr}")
        print(f"stdout: {e.stdout}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    print(json_file)

    return json_file


def load_json_file(json_file):
    rename_refactorings = []

    # Load the JSON file
    with open(json_file, 'r') as file:
        data = json.load(file)

        # Check each commit in the file
        for commit in data.get('commits', []):
            for refactoring in commit.get('refactorings', []):
                refactoring_type = refactoring.get('type', '')

                # Check if 'Rename' is in the refactoring type
                if 'Rename' in refactoring_type:
                    original_name = None
                    renamed_name = None

                    # Search in leftSideLocations for the original variable/class/method name
                    for location in refactoring.get('leftSideLocations', []):
                        if location.get('codeElement') is not None:
                            original_name = location['codeElement']
                            break

                    # Search in rightSideLocations for the renamed variable/class/method name
                    for location in refactoring.get('rightSideLocations', []):
                        if location.get('codeElement') is not None:
                            renamed_name = location['codeElement']
                            break

                    # Remove the type suffix from names (e.g., ": int")
                    if original_name:
                        original_name = original_name.split(':')[0].strip()
                    if renamed_name:
                        renamed_name = renamed_name.split(':')[0].strip()

                    # Add to the list if both original and renamed names are found
                    if original_name and renamed_name:
                        rename_refactorings.append((original_name, renamed_name))

    return rename_refactorings


def main():
    # Step 1: Search for Java repositories
    java_repos = search_java_repositories()

    # Write the list of tuples to the output file
    with open("output_file.txt", 'w') as file:

        # Step 2: Select two random repositories
        selected_repos = random.sample(java_repos, 10)
        for repo in selected_repos:
            repo_full_name = repo['full_name']
            print(f"Processing repository: {repo_full_name}")

            # Step 3: Get all commits for the repository
            commits = get_commits(repo_full_name)

            # Step 4: Run RefactoringMiner for each commit
            for commit in commits:
                commit_sha = commit['sha']
                print(f"Processing commit: {commit_sha}")
                json_file = run_refactoringminer(repo_full_name, commit_sha)

                # Step 5: Load and process the JSON output
                refactoring_data = load_json_file(json_file)
                for original_name, renamed_name in refactoring_data:
                    file.write(f"({original_name}, {renamed_name})\n")

                print(refactoring_data)


if __name__ == "__main__":
    main()