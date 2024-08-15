import requests
import os

# Constants
GITHUB_API_URL = "https://api.github.com"
SEARCH_URL = f"{GITHUB_API_URL}/search/repositories"
REPOS_PER_PAGE = 10  # Number of repositories per search result page
GITHUB_TOKEN = "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"  # Replace with your GitHub token

# Headers for authentication
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}"
}


def search_repositories():
    params = {
        "q": "language:java",
        "sort": "stars",
        "order": "desc",
        "per_page": REPOS_PER_PAGE
    }
    response = requests.get(SEARCH_URL, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()["items"]


def get_releases(repo_full_name):
    releases_url = f"{GITHUB_API_URL}/repos/{repo_full_name}/releases"
    response = requests.get(releases_url, headers=HEADERS)
    response.raise_for_status()
    return response.json()


def download_jar(asset_url, download_path):
    response = requests.get(asset_url, headers=HEADERS, stream=True)
    response.raise_for_status()
    with open("java_jars/" + download_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)


def main():
    repos = search_repositories()
    for i in range(400):
        for repo in repos:
            repo_full_name = repo["full_name"]
            # repo_full_name = "pH-7/Simple-Java-Calculator"
            releases = get_releases(repo_full_name)
            jar_urls = []

            for release in releases:
                if len(jar_urls) >= 2:
                    break
                for asset in release["assets"]:
                    if asset["name"].endswith(".jar"):
                        jar_urls.append(asset["browser_download_url"])
                        if len(jar_urls) >= 2:
                            break

            if len(jar_urls) >= 2:
                print(f"Found 2 JAR files for {repo_full_name}. Downloading...")
                os.makedirs(repo_full_name.replace("/", "_"), exist_ok=True)
                for j, jar_url in enumerate(jar_urls):
                    download_path = f"{repo_full_name.replace('/', '_')}/release_{j + 1}.jar"
                    download_jar(jar_url, download_path)
                    print(f"Downloaded {jar_url} to {download_path}")


if __name__ == "__main__":
    main()