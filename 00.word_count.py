import re
from collections import Counter
from bs4 import BeautifulSoup
from pathlib import Path
import json
import multiprocessing as mp
import os

keywords_file = "keywords/all_keywords.txt"
with open(keywords_file, "r") as f:
    keywords = f.read().splitlines()
# formulate the keywords
keywords = [keyword.lower().strip() for keyword in keywords]

def parse_10K_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    html_content = html_content.replace(")", ",")
    html_content = html_content.replace("(", ",")
    # remove noncanonical characters
    html_content = html_content.replace("#", ",")
    
    soup = BeautifulSoup(html_content, 'html.parser')  
    # find header
    company_info = soup.find('acceptance-datetime')
    header = soup.find('header')
    if header is None:
        return None
    real_content_obj = header.find_next_sibling(text=True)
    if real_content_obj is None:
        return None
    else:
        real_content = real_content_obj
    return {
        "company_info": company_info,
        "10K": real_content
    }

# Preprocess the text
def preprocess_text(text):
    # Lowercase and remove non-alphabetical characters
    return re.sub(r'[^a-zA-Z\s]', '', text.lower())

def count_keywords(text, keywords):
    text = preprocess_text(text)
    keyword_counts = Counter()
    
    # Sort keywords by length, so phrases (with spaces) are matched before single words
    for keyword in sorted(keywords, key=lambda x: len(x.split()), reverse=True):
        keyword_lower = keyword.lower()
        keyword_counts[keyword] = len(re.findall(r'\b' + re.escape(keyword_lower) + r'\b', text))
    
    keyword_counts = dict(keyword_counts)
    keyword_counts["total_words"] = len(text.split())
    return keyword_counts

def count_keywords_in_file(file_path, keywords, output_dir=None):
    file_name = Path(file_path).name
    output_dir = Path(output_dir) if output_dir is not None else Path.cwd()
    if os.path.exists(output_dir / f"{file_name}_word_count.json"):
        print(f"Skipping {file_name}")
        return

    tenK_info = parse_10K_file(file_path)
    
    # Get the keyword frequency
    keyword_frequency = count_keywords(tenK_info["10K"], keywords)
    with open(output_dir / f"{file_name}_word_count.json", "w") as f:
        json.dump(keyword_frequency, f, indent=4)

def gen_results(year, quarter, file):
    result_dir = Path("result")
    output_dir = result_dir / year / f"QTR{quarter}"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Processing {file}")
    count_keywords_in_file(file, keywords, output_dir=output_dir)

data_dir = Path("data")
years = [str(i) for i in range(2006, 2024)]
n_quarters = 4

# Get all the 10-K files
tenK_files = []
for year in years:
    year_dir = data_dir / year
    for i in range(1, n_quarters + 1):
        quarter_dir = year_dir / f"QTR{i}"
        for file in quarter_dir.iterdir():
            file_name = file.name
            if "_10-K" in file_name or "_10K" in file_name:
                tenK_files.append((year, i, file))


if __name__ == "__main__":
    # Multiprocessing
    pool = mp.Pool(mp.cpu_count())
    pool.starmap(gen_results, tenK_files)
    pool.close()
    pool.join()
