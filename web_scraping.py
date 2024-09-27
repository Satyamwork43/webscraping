import os
import re
import csv
import time
import hashlib
import boto3
from collections import deque
from urllib.parse import urlparse, urlunparse
import markdownify
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime


class URLManager:
    def __init__(self, visited_file):
        self.visited_file = visited_file
        self.visited_urls = self.load_visited_urls()

    def load_visited_urls(self):
        if os.path.exists(self.visited_file):
            with open(self.visited_file, "r") as file:
                return set(line.strip() for line in file.readlines())
        return set()

    def add_visited_url(self, url):
        self.visited_urls.add(url)
        with open(self.visited_file, "a") as file:
            file.write(url + "\n")

    @staticmethod
    def clean_url(url):
        if url.endswith('.pdf'):
            return url  # Don't clean .pdf URLs
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.split('.')
        if len(path_parts) > 1:
            path_parts = path_parts[:-1]  # Remove the extension
        path = '.'.join(path_parts)
        return "https://" + urlunparse(('', parsed_url.netloc, path, '', '', '')).lstrip('//')


class S3Manager:
    def __init__(self, s3_bucket, txt_folder, pdf_folder, metadata_csv, failed_csv):
        self.s3_client = boto3.client("s3")
        self.s3_bucket = s3_bucket
        self.txt_folder = txt_folder
        self.pdf_folder = pdf_folder
        self.metadata_csv = metadata_csv
        self.failed_csv = failed_csv
        self.metadatalist = []
        self.failedurllist = []

    @staticmethod
    def get_checksum(data):
        return hashlib.md5(data.encode()).hexdigest()

    def save_metadata(self):
        with open('/tmp/metadata.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'parent_url', 'url', 'title', 'std_flag', 'sub_flag', 'interpretation_date', 
                'duplicate_std_numbers', 'archive_status', 'publication_id', 'output_filename', 
                'output_path', 'checksum', 'creation_date', 'roles'
            ])
            writer.writerows(self.metadatalist)

        self.s3_client.upload_file('/tmp/metadata.csv', self.s3_bucket, self.metadata_csv)

    def save_failed_urls(self):
        with open('/tmp/failed_urls.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['url', 'error_message'])
            writer.writerows(self.failedurllist)

        self.s3_client.upload_file('/tmp/failed_urls.csv', self.s3_bucket, self.failed_csv)

    def upload_txt_to_s3(self, url, content, parent_url, title="N/A"):
        try:
            checksum = self.get_checksum(content)
            file_name = re.sub(r'[\/:*?"<>|]', '_', url)[:100] + '.txt'
            file_path = f"{self.txt_folder}/{file_name}"
            file_data = f"{url}\n\n{content}"
            self.s3_client.put_object(Body=file_data, Bucket=self.s3_bucket, Key=file_path)

            metadata_entry = [
                parent_url, url, title, '', '', '', '', '', '', file_name, 
                file_path, checksum, datetime.now().isoformat(), ['student', 'counsellor']
            ]
            self.metadatalist.append(metadata_entry)
            print(f"Uploaded TXT: {file_name}")
        except Exception as e:
            self.failedurllist.append([url, str(e)])
            print(f"Failed to upload TXT: {url} - {e}")

    def upload_pdf_to_s3(self, url, pdf_content, parent_url, title="N/A"):
        try:
            file_name = os.path.basename(url)
            file_path = f"{self.pdf_folder}/{file_name}"
            self.s3_client.put_object(Body=pdf_content, Bucket=self.s3_bucket, Key=file_path)

            checksum = hashlib.md5(pdf_content).hexdigest()
            metadata_entry = [
                parent_url, url, title, '', '', '', '', '', '', file_name, 
                file_path, checksum, datetime.now().isoformat(), ['student', 'counsellor']
            ]
            self.metadatalist.append(metadata_entry)
            print(f"Uploaded PDF: {file_name}")
        except Exception as e:
            self.failedurllist.append([url, str(e)])
            print(f"Failed to upload PDF: {url} - {e}")


class WebScraper:
    def __init__(self, start_url, options, url_manager, s3_manager):
        self.driver = webdriver.Chrome(options=options)
        self.start_url = start_url
        self.url_manager = url_manager
        self.s3_manager = s3_manager
        self.queue = deque([(self.start_url, "")])  # Each queue item is a tuple (URL, parent_url)
        self.youtube_pattern = re.compile(r"^https?:\/\/(?:www\.)?youtube\.com\/watch\?")
        self.facebook_login_pattern = re.compile(r"^https?:\/\/(?:www\.)?facebook\.com\/login")
        self.pdf_pattern = re.compile(r"\.pdf$", re.IGNORECASE)

    def scrape(self):
        while self.queue:
            url, parent_url = self.queue.popleft()
            if url in self.url_manager.visited_urls:
                print(f"Skipping {url} - Already processed")
                continue
            if self.youtube_pattern.match(url) or self.facebook_login_pattern.match(url):
                print(f"Skipping {url} - YouTube or Facebook login detected")
                continue
            if self.pdf_pattern.search(url):
                print(f"Downloading PDF: {url}")
                self.download_pdf(url, parent_url)
                self.url_manager.add_visited_url(url)
                continue

            self.process_page(url, parent_url)

    def process_page(self, url, parent_url):
        try:
            self.driver.get(url)
            time.sleep(1)  # Delay to allow page loading
            WebDriverWait(self.driver, 1).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            body_content = self.driver.find_element(By.TAG_NAME, "body").get_attribute("outerHTML")
            markdown_content = markdownify.markdownify(str(body_content), escape_misc=False).strip()
            title = self.driver.title if self.driver.title else "N/A"
            self.s3_manager.upload_txt_to_s3(url, markdown_content, parent_url, title)
            self.extract_links(url)
        except Exception as e:
            print(f"Failed to load or process {url}: {e}")
            self.s3_manager.failedurllist.append([url, str(e)])
        finally:
            self.url_manager.add_visited_url(url)

    def extract_links(self, parent_url):
        links = self.driver.find_elements(By.TAG_NAME, "a")
        href_links = [link.get_attribute("href") for link in links if link.get_attribute("href")]
        for href in href_links:
            cleaned_href = self.url_manager.clean_url(href)
            if cleaned_href not in self.url_manager.visited_urls and cleaned_href not in [url for url, _ in self.queue]:
                self.queue.append((cleaned_href, parent_url))

    def download_pdf(self, url, parent_url):
        try:
            pdf_response = requests.get(url)
            if pdf_response.status_code == 200:
                self.s3_manager.upload_pdf_to_s3(url, pdf_response.content, parent_url)
            else:
                raise Exception(f"Failed to download PDF, status code: {pdf_response.status_code}")
        except Exception as e:
            print(f"Error downloading PDF: {e}")
            self.s3_manager.failedurllist.append([url, str(e)])


class MainApp:
    def __init__(self):
        # S3 setup
        self.s3_bucket = "my_bucket"
        self.txt_folder = "myfolder/txt"
        self.pdf_folder = "myfolder/pdf"
        self.metadata_csv = "metadata/metadata.csv"
        self.failed_csv = "metadata/failed_urls.csv"

        # Folder setup
        self.visited_file = "visited_urls.txt"

        # Instantiate components
        self.url_manager = URLManager(self.visited_file)
        self.s3_manager = S3Manager(self.s3_bucket, self.txt_folder, self.pdf_folder, self.metadata_csv, self.failed_csv)

        # WebDriver options
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')

        # Starting URL
        self.start_url = "https://example.com"

        self.scraper = WebScraper(self.start_url, options, self.url_manager, self.s3_manager)

    def run(self):
        self.scraper.scrape()
        self.s3_manager.save_metadata()
        self.s3_manager.save_failed_urls()


if __name__ == "__main__":
    app = MainApp()
    app.run()
