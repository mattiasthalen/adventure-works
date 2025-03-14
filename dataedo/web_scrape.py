import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time

def download_html_files(url, output_dir):
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Set a user agent to be polite
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; DataedoHTMLDownloader/1.0)'
    }
    
    # Get the main page
    print(f"Accessing: {url}")
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to fetch the page: {response.status_code}")
        return
    
    # Parse the HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all links
    links = soup.find_all('a')
    
    # Keep track of downloaded files and filenames
    downloaded = set()
    used_filenames = set()
    
    # Download each linked page
    for link in links:
        href = link.get('href')
        
        # Skip if no href or if it's an anchor or navigation
        if not href or href.startswith('#') or href == '/' or href == '../':
            continue
        
        # Create absolute URL
        file_url = urljoin(url, href)
        
        # Skip if already downloaded
        if file_url in downloaded:
            continue
        
        print(f"Downloading: {file_url}")
        
        # Download the file
        try:
            file_response = requests.get(file_url, headers=headers)
            file_response.raise_for_status()  # Raise an exception for 4XX/5XX responses
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {file_url}: {e}")
            continue
        
        # Parse the URL to extract a suitable filename
        parsed_url = urlparse(file_url)
        path = parsed_url.path
        
        # Generate a unique filename
        # Use the last non-empty part of the path
        path_parts = [p for p in path.split('/') if p]
        if path_parts:
            filename = path_parts[-1]
            # Add .html extension if needed
            if not filename.endswith('.html'):
                filename += '.html'
        else:
            # Fallback filename
            filename = f"page_{len(downloaded)}.html"
        
        # Handle duplicate filenames
        base_filename = filename
        counter = 1
        while filename in used_filenames:
            name_parts = base_filename.rsplit('.', 1)
            if len(name_parts) > 1:
                filename = f"{name_parts[0]}_{counter}.{name_parts[1]}"
            else:
                filename = f"{base_filename}_{counter}"
            counter += 1
        
        # Save the file
        file_path = os.path.join(output_dir, filename)
        with open(file_path, 'wb') as f:
            f.write(file_response.content)
        print(f"Saved: {filename}")
        
        # Mark as downloaded
        downloaded.add(file_url)
        used_filenames.add(filename)
        
        # Be nice to the server - small delay between requests
        time.sleep(0.5)

    print(f"Downloaded {len(downloaded)} files to {output_dir}")

if __name__ == "__main__":
    url = "https://dataedo.com/samples/html/AdventureWorks/doc//AdventureWorks_2/tables/"
    output_dir = "./dataedo"
    download_html_files(url, output_dir)