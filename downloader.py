import requests

def download_file(url, filename):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Downloaded: {filename}")
    else:
        print(f"Failed to download: {filename}")

base_url = "https://openalex.s3.amazonaws.com/data/works/updated_date%3D2024-06-30/part_"
extension = ".gz"

for i in range(44):  # From part_000.gz to part_043.gz (inclusive)
    part_number = str(i).zfill(3)
    file_url = f"{base_url}{part_number}{extension}"
    file_name = f"part_{part_number}{extension}"
    download_file(file_url, file_name)
