import os
import re
import time
import socket
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Queue, Manager, Value
from threading import Lock
import hashlib
import shutil

# DOMAIN = "ku.ac.th"
DOMAIN = [
    "www.nekopost.net",
    "www.so-manga.com",
    "www.kingsmanga.net",
    "www.go-manga.com",
    "www.mangakimi.com",
    "www.up-manga.com",
    "www.oremanga.net",
    "www.inu-manga.com",
    "www.flash-manga.com",
    "www.slow-manga.com",
]

START_URLS = [
    "https://www.nekopost.net/",
    "https://www.so-manga.com/manga/",
    "https://www.kingsmanga.net/",
    "https://www.go-manga.com/",
    "https://www.mangakimi.com/",
    "https://www.up-manga.com/",
    "https://www.oremanga.net/",
    "https://www.inu-manga.com/",
    "https://www.flash-manga.com/",
    "https://www.slow-manga.com/",
]


# START_URL = "https://ku.ac.th"
OUTPUT_DIR = "html"

HEADERS = {
    'User-Agent': 'Phongsathon/1.0',
    'From': 'phongsathon.r@ku.th',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'th-TH,th;q=0.9',
    'Connection': 'keep-alive'
}

# HEADERS = {'User-Agent': 'Bubee'}
MAX_THREADS = 10
FETCH_DELAY = 1
DOMAIN_COOLDOWN = 5
ANALYZE_PROCESS = int(MAX_THREADS / 5)
MAX_FETCHES_PER_DOMAIN = 2

# log error
LOG_DIR = "./logs"
DNS_LOG = f"{LOG_DIR}/dns_log.txt"
ROBOTS_LOG = f"{LOG_DIR}/robots_log.txt"
FETCH_LOG = f"{LOG_DIR}/fetch_log.txt"
SAVE_LOG = f"{LOG_DIR}/save_log.txt"
PROCESS_LOG = f"{LOG_DIR}/process_log.txt"

manager = Manager()

robots = list()
sitemap = list()

url_queue = Queue()
result_queue = Queue() # manager.Queue()

visited = manager.list()  # Shared list across processes
ip_last_access = defaultdict(float)
dns_cache = {}

active_fetches = defaultdict(int)  # Tracks active fetches per domain
fetch_lock = Lock()  # Ensures thread-safe access to `active_fetches`

count_download = Value('i', 0)  # 'i' means an integer type

def load_domain_from_html():
    try:
        domains = list(os.walk(OUTPUT_DIR))[0][1]
        print(domains)
        for domain in domains:
            url = f"https://{domain}".replace('\\', '/')
            url_queue.put(url)
            print(url)
        print(f"[Loaded] {url_queue.qsize} domain from {OUTPUT_DIR}")
    except Exception as e:
        print(f"[Error] Failed to load domain from {OUTPUT_DIR}: {e}")

def load_url_from_log():
    url_pattern = r"https?://[^\s]+"

    with open(FETCH_LOG, "r", encoding="utf-8") as file:
        text = file.read()

    urls = re.findall(url_pattern, text)

    return urls

# --------------------------
# DNS Resolver
# --------------------------
def resolve_domain(domain):
    if domain in dns_cache:
        return dns_cache[domain]
    try:
        ip_address = socket.gethostbyname(domain)
        dns_cache[domain] = ip_address
        # print(f"[Resolved] {domain} -> {ip_address}")
        return ip_address
    except socket.gaierror:
        with open(DNS_LOG, 'a', encoding='utf-8') as log:
            log.write(f"[DNS Error] Could not resolve domain: {domain}\n")
        # print(f"[DNS Error] Could not resolve domain: {domain}")
        return None

# --------------------------
# URL Fetcher (Multi-thread)
# --------------------------
def fetch_url(url):
    scheme = urlparse(url).scheme
    domain = urlparse(url).netloc
    try:
        ip = resolve_domain(domain)
        if not ip:
            return

        current_time = time.time()

        # Rate limiting per domain
        if current_time - ip_last_access[ip] < DOMAIN_COOLDOWN:
            time.sleep(DOMAIN_COOLDOWN - (current_time - ip_last_access[ip]))

        if ip not in robots:
            crawl_robots(url, ip)
            robots.append(ip)

        ip_last_access[ip] = time.time()
        response = requests.get(url, headers=HEADERS, timeout=10)

        if response.status_code == 200 and 'text/html' in response.headers['Content-Type']:
            # print(f"[Downloaded] {url}")
            result_queue.put((url, response.text))
        # else:
            # with open(FETCH_LOG, 'w', encoding='utf-8') as log:
            #     log.write(f"[Skipped] {url} - Content-Type: {response.headers['Content-Type']}\n")
            # print(f"[Skipped] {url} (Non-HTML or Error)")
    except Exception as e:
        with open(FETCH_LOG, 'a', encoding='utf-8') as log:
            log.write(f"[Error Fetching] {url}: {e}\n")
        # print(f"[Error Fetching] {url}: {e}")
    finally:
        with fetch_lock:
                active_fetches[domain] -= 1

# --------------------------
# Robots.txt Handling
# --------------------------
def crawl_robots(url, ip):
    robots_url = urljoin(url, '/robots.txt')
    try:
        response = requests.get(robots_url, headers=HEADERS, timeout=5)
        if response.status_code == 200:
            with open("list_robots.txt", "a") as f:
                f.write(f"{urlparse(url).netloc}\n")
            # robots.append(ip)
            # print(f"[Robots] {robots_url}")
    except Exception as e:
        with open(ROBOTS_LOG, 'a', encoding='utf-8') as log:
            log.write(f"[Error Fetching Robots.txt] {robots_url}: {e}\n")
        # print(f"[Error Fetching Robots.txt] {e}")

# --------------------------
# URL Analyzer (Multi-process)
# --------------------------
def analyze_and_save():
    while True:
        try:
            url, html = result_queue.get()
            if url is None:
                break

            if url in visited:
                continue

            links = extract_links(html, url)
            save_html(url, html)
            visited.append(url)

            for link in links:
                if link not in visited:
                    url_queue.put(link)
        except Exception as e:
            with open(PROCESS_LOG, 'a', encoding='utf-8') as log:
                log.write(f"Error analyze_and_save in process {os.getpid()}: {e}\n")
            # print(f"Error analyze_and_save in process {os.getpid()}: {e}")
        # finally:
        #     print(f"Process {os.getpid()} finished.")

def extract_links(html, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    links = set()
    for link in soup.find_all('a', href=True):
        full_url = urljoin(base_url, link['href'])
        if is_valid(full_url) and full_url not in visited:
            links.add(full_url)
    return links

def is_valid(url):
    if  urlparse(url).netloc in DOMAIN:
        if url.endswith('.html') or url.endswith('.htm') or url.endswith('.php'):
            return True
        if url.endswith(('.pdf', '.doc', '.docx', '.xls', '.xlsx',
                        '.ppt', '.pptx', '.zip', '.rar', '.tar',
                        '.gz', '.mp4', '.mp3')):
            return False
        return True
    return False

def save_html(url, content):
    try:
        parsed_url = urlparse(url)
        netloc = parsed_url.netloc  # Hostname
        path = parsed_url.path.strip('/')  # Path without leading/trailing slashes

        if not path:
            path = "dummy"

        directory = os.path.join("html", netloc, os.path.dirname(path))
        os.makedirs(directory, exist_ok=True)

        file_name = os.path.basename(path) or "dummy"
        # if not file_name == "dummy" and not file_name.endswith(('.htm', '.html', '.php')):
        #     if '.' not in file_name:
        #         file_name += ".html"

        if len(file_name) > 255:
            # hashed_name = hashlib.sha256(file_name.encode('utf-8')).hexdigest()[:20]
            file_name = f"{file_name[:20]}.html"

        file_path = os.path.join(directory, file_name)

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        # print(f"[Saved] {url} to {file_path}")
        
        with count_download.get_lock():
            count_download.value += 1

    except Exception as e:
        with open(SAVE_LOG, 'a', encoding='utf-8') as log:
            log.write(f"[Error] Failed to save {url}: {e}\n")
        # print(f"[Error] Failed to save {url}: {e}")
        raise

# --------------------------
# Main Crawler
# --------------------------

def main():

    # error_urls = load_url_from_log()

    if os.path.exists(LOG_DIR):
        shutil.rmtree(LOG_DIR)  # Delete the existing directory
    os.makedirs(LOG_DIR)  # Recreate the directory

    # load_domain_from_html()
    # count_download = 0

    # for url in error_urls:
    #     url_queue.put(url)

    for url in START_URLS:
        url_queue.put(url)
        print(f"Put url to queue : {url}")

    processes = []
    for _ in range(ANALYZE_PROCESS):
        # p = Process(target=analyze_and_save, args=(result_queue, visited))
        p = Process(target=analyze_and_save)
        p.start()
        processes.append(p)

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        try:
            while count_download.value < 20000:
                if not url_queue.empty():

                    urls_to_fetch = []
                    while len(urls_to_fetch) < MAX_THREADS and not url_queue.empty():
                        url = url_queue.get()
                        domain = urlparse(url).netloc
                        with fetch_lock:
                            if active_fetches[domain] < MAX_FETCHES_PER_DOMAIN:
                                urls_to_fetch.append(url)
                                active_fetches[domain] += 1
                            else:
                                url_queue.put(url)

                    executor.map(fetch_url, urls_to_fetch)
                    # analyze_and_save()
                else:
                    print("[Queue Empty] Waiting...")
                print("=====================================")
                print(f'Count Visited: {len(visited)}')
                print(f'Count URL Queue: {url_queue.qsize()}')
                print(f'Count Download: {count_download.value}')
                print("=====================================\n")

                for i, process in enumerate(processes):
                    if not process.is_alive():
                        with open(PROCESS_LOG, 'a', encoding='utf-8') as log:
                            log.write(f"Process {i} has stopped unexpectedly!")
                        print(f"Process {i} has stopped unexpectedly!")
                        new_process = Process(target=analyze_and_save)
                        new_process.start()
                        processes[i] = new_process
                        with open(PROCESS_LOG, 'a', encoding='utf-8') as log:
                            log.write(f"Process {i} restarted.")
                        print(f"Process {i} restarted.")
                        
                time.sleep(FETCH_DELAY)
        finally:
            executor.shutdown(wait=True)

    for _ in processes:
        result_queue.put((None, None))  # Terminate processes

    for p in processes:
        p.join()

    print(f"Completed: {len(visited)} pages downloaded.")

if __name__ == '__main__':
    main()
