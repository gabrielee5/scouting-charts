from tqdm import tqdm
import time
import requests
from pathlib import Path

def download_with_progress(url, save_path):
    """
    Download a file with a progress bar.
    
    Args:
        url (str): URL to download from
        save_path (str): Path where to save the file
    """
    # Send a streaming GET request
    response = requests.get(url, stream=True)
    # Get the total file size
    total_size = int(response.headers.get('content-length', 0))
    
    # Open the local file to write the downloaded data
    save_path = Path(save_path)
    with open(save_path, 'wb') as file, \
         tqdm(
            desc=f'Downloading {save_path.name}',
            total=total_size,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
        ) as progress_bar:
        for data in response.iter_content(chunk_size=1024):
            size = file.write(data)
            progress_bar.update(size)

# Example of a simple progress bar for iteration
def process_items_with_progress(items):
    """
    Process a list of items with a progress bar.
    
    Args:
        items (list): List of items to process
    """
    for _ in tqdm(items, desc="Processing", unit="items"):
        # Simulate some work
        time.sleep(0.1)

# Example of manual progress bar
def manual_progress():
    """
    Example of manually controlling a progress bar.
    """
    with tqdm(total=100, desc="Loading", unit="%") as pbar:
        for i in range(10):
            # Simulate some work
            time.sleep(0.1)
            # Update progress bar by 10%
            pbar.update(10)

# Example usage
if __name__ == "__main__":
    # Example 1: Download file with progress
    url = "https://example.com/large-file.zip"
    download_with_progress(url, "large-file.zip")
    
    # Example 2: Process items with progress
    items = list(range(50))
    process_items_with_progress(items)
    
    # Example 3: Manual progress bar
    manual_progress()