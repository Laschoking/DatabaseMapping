import os
import time
import requests
from python_graphql_client import GraphqlClient

# Constants
GITHUB_API_URL = "https://api.github.com/graphql"
GITHUB_TOKEN = "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"  # Replace with your GitHub token
DESIRED_REPOS = 5  # Number of repositories needed with at least 2 JAR files

# Initialize GraphQL client
client = GraphqlClient(endpoint=GITHUB_API_URL)

# Headers for authentication
HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}"
}


def check_rate_limit():
    query = """
    {
      rateLimit {
        remaining
        resetAt
      }
    }
    """
    response = client.execute(query=query, headers=HEADERS)
    rate_limit = response["data"]["rateLimit"]
    remaining = rate_limit["remaining"]
    reset_time = rate_limit["resetAt"]
    return remaining, reset_time


def search_repositories(after_cursor=None):
    query = """
    query ($after: String) {
      search(query: "language:java stars:>100", type: REPOSITORY, first: 10, after: $after) {
        edges {
          node {
            ... on Repository {
              nameWithOwner
              releases(first: 10) {
                edges {
                  node {
                    releaseAssets(first: 10) {
                      edges {
                        node {
                          name
                          downloadUrl
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        pageInfo {
          endCursor
          hasNextPage
        }
      }
    }
    """
    variables = {"after": after_cursor}
    response = client.execute(query=query, variables=variables, headers=HEADERS)
    return response["data"]["search"]


def download_jar(asset_url, download_path):
    response = requests.get(asset_url, headers={"Authorization": f"token {GITHUB_TOKEN}"}, stream=True)
    response.raise_for_status()
    with open(download_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)


if __name__ == "__main__":
    collected_repos = 0
    after_cursor = None

    while collected_repos < DESIRED_REPOS:
        remaining, reset_time = check_rate_limit()
        if remaining == 0:
            wait_time = (reset_time - time.time() + 1)
            print(f"Rate limit exceeded. Waiting for {wait_time} seconds.")
            time.sleep(wait_time)
            continue

        search_results = search_repositories(after_cursor)
        after_cursor = search_results["pageInfo"]["endCursor"]
        has_next_page = search_results["pageInfo"]["hasNextPage"]

        for repo_edge in search_results["edges"]:
            if collected_repos >= DESIRED_REPOS:
                break

            repo = repo_edge["node"]
            repo_full_name = repo["nameWithOwner"]
            releases = repo["releases"]["edges"]
            jar_urls = []

            for release_edge in releases:
                release = release_edge["node"]
                for asset_edge in release["releaseAssets"]["edges"]:
                    asset = asset_edge["node"]
                    if asset["name"].endswith(".jar"):
                        jar_urls.append(asset["downloadUrl"])
                        if len(jar_urls) >= 2:
                            break
                if len(jar_urls) >= 2:
                    break

            if len(jar_urls) >= 2:
                print(f"Found 2 JAR files for {repo_full_name}. Downloading...")
                os.makedirs(repo_full_name.replace("/", "_"), exist_ok=True)
                for i, jar_url in enumerate(jar_urls):
                    download_path = f"{repo_full_name.replace('/', '_')}/release_{i + 1}.jar"
                    download_jar(jar_url, download_path)
                    print(f"Downloaded {jar_url} to {download_path}")
                collected_repos += 1
                print(f"Collected {collected_repos}/{DESIRED_REPOS} repositories.")

        if not has_next_page:
            print("No more pages left to search. Stopping the script.")
            break