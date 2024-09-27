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

        # Ensure bucket and folders exist
        self.create_bucket_if_not_exists(self.s3_bucket)
        self.create_folder_if_not_exists(self.s3_bucket, self.txt_folder)
        self.create_folder_if_not_exists(self.s3_bucket, self.pdf_folder)

    def create_bucket_if_not_exists(self, bucket_name):
        try:
            response = self.s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' already exists.")
        except:
            print(f"Bucket '{bucket_name}' does not exist. Creating bucket...")
            self.s3_client.create_bucket(Bucket=bucket_name)

    def create_folder_if_not_exists(self, bucket_name, folder_name):
        if not folder_name.endswith('/'):
            folder_name += '/'
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=folder_name)
            print(f"Folder '{folder_name}' already exists.")
        except:
            print(f"Folder '{folder_name}' does not exist. Creating folder...")
            self.s3_client.put_object(Bucket=bucket_name, Key=(folder_name))

    @staticmethod
    def get_checksum(data):
        return hashlib.md5(data.encode()).hexdigest()

    def save_metadata(self):
        local_metadata_path = '/local/tmp/metadata.csv'
        os.makedirs('/local/tmp', exist_ok=True)  # Ensure the folder exists locally

        with open(local_metadata_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'parent_url', 'url', 'title', 'std_flag', 'sub_flag', 'interpretation_date', 
                'duplicate_std_numbers', 'archive_status', 'publication_id', 'output_filename', 
                'output_path', 'checksum', 'creation_date', 'roles'
            ])
            writer.writerows(self.metadatalist)

        self.s3_client.upload_file(local_metadata_path, self.s3_bucket, self.metadata_csv)

    def save_failed_urls(self):
        local_failed_path = '/local/tmp/failed_urls.csv'
        os.makedirs('/local/tmp', exist_ok=True)  # Ensure the folder exists locally

        with open(local_failed_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['url', 'error_message'])
            writer.writerows(self.failedurllist)

        self.s3_client.upload_file(local_failed_path, self.s3_bucket, self.failed_csv)

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
