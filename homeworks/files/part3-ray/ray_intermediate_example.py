import ray
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import subprocess
import sys

import ray
import subprocess
import sys

@ray.remote
def extract_links(start_url, base_url, depth=2):
    # Install dependencies inside the Ray worker
    subprocess.run([sys.executable, "-m", "pip", "install", "beautifulsoup4", "requests", "lxml"])

    import requests
    from bs4 import BeautifulSoup

    def _extract_links(elements, base_url, max_results=100):
        links = []
        for e in elements:
            url = e["href"]
            if "https://" not in url:
                url = base_url + url
            if base_url in url:
                links.append(url)
        return set(links[:max_results])

    if depth == 0:
        return set()

    page = requests.get(start_url)
    soup = BeautifulSoup(page.content, "html.parser")
    elements = soup.find_all("a", href=True)
    links = _extract_links(elements, base_url)

    for url in links:
        new_links = ray.get(extract_links.remote(url, base_url, depth - 1))
        links = links.union(new_links)
    return links


if __name__ == "__main__":
    ray.init(address="ray://127.0.0.1:10001")  # Connected via port-forward
    result = ray.get(extract_links.remote("https://github.com/mmRoshani/cloud-computing-2025", "https://github.com/mmRoshani/cloud-computing-2025", depth=1))
    print("Links found:")
    for link in sorted(result):
        print(link)
