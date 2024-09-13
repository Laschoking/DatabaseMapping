import requests
import os
from pathlib import Path
import sqlite3
import pandas as pd
# Constants
GITHUB_API_URL = "https://api.github.com"
SEARCH_URL = f"{GITHUB_API_URL}/search/repositories"
REPOS_PER_PAGE = 10  # Number of repositories per search result page
GITHUB_TOKEN = "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"  # Replace with your GitHub token

# Headers for authentication
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}"
}



def get_releases(repo_full_name):
    release_url = f"{GITHUB_API_URL}/repos/{repo_full_name}/releases"
    response = requests.get(release_url, headers=HEADERS)
    try:
        response.raise_for_status()
    except:
        #print(f"url does not exist{release_url}")
        return None
    return response.json()


def download_jar(asset_url, download_path):
    response = requests.get(asset_url, headers=HEADERS, stream=True)
    response.raise_for_status()
    with open("java_jars/" + download_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)


def main():
    db_path = Path("/home/kotname/Documents/Diplom/Code/DatabaseMapping/src/Evaluation/Evaluation.db")
    con = sqlite3.connect(db_path)
    non_ex_repos,no_release_repos = [],[]
    df = pd.read_sql_query("Select repository_url from java_core_projects ORDER BY size ASC LIMIT 1600 OFFSET 1600",con)

    java_projs = df.T.squeeze(axis=0)
    #java_projs.iat[0] = 'https://github.com/pH-7/Simple-Java-Calculator'
    java_releases = []
    for i in range(len(java_projs)):
        url = java_projs.iat[i]
        #print(url)
        repo_name = url.removeprefix('https://github.com/')
        releases = get_releases(repo_name)
        if releases == None:
            non_ex_repos.append(repo_name)
            continue
        if releases == []:
            no_release_repos.append(repo_name)
            continue
        jar_urls = []

        for release in releases:
            if len(jar_urls) >= 2:
                break
            for asset in release["assets"]:
                if asset["name"].endswith(".jar"):
                    jar_urls.append(asset["browser_download_url"])

        if len(jar_urls) >= 2:
            for jar in jar_urls:
                java_releases.append([repo_name,jar])

    print(f"repos that not existed: {len(non_ex_repos)}")
    print(f"repos that had no release: {len(no_release_repos)}")
    print('-------------------')
    print(f"repos with at least 2 releases: {len(java_releases)}")


    df = pd.DataFrame.from_records(java_releases,columns=['repo', 'release'])
    df.to_csv("java_releases.csv")
    '''        
            print(f"Found 2 JAR files for {repo_name}. Downloading...")
            os.makedirs(repo_name.replace("/", "_"), exist_ok=True)
            for j, jar_url in enumerate(jar_urls):
                download_path = f"{repo_name.replace('/', '_')}/release_{j + 1}.jar"
                download_jar(jar_url, download_path)
                print(f"Downloaded {jar_url} to {download_path}")
    '''

if __name__ == "__main__":
    main()