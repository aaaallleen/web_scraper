import requests
import itertools
import time
import json
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm import tqdm

# Constants
BIBLES = ['n_testament', 'o_testament']
LANGS = ["zh", "tru", "tay", "sed"]
LANG_CODE = {"zh": "tcv2019", "tru": "tru", "tay": "tay", "sed": "sed"}
LANG_TDTAG = {"zh": "bstw", "tru": "nor", "tay": "nor", "sed": "bstwre"}
SLEEP_TIME = 0.5

def fetch_url(url):
    """Fetch content from a given URL."""
    try:
        response = requests.get(url)
        time.sleep(SLEEP_TIME)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        print(f"Failed to retrieve the page {url}. Error: {e}")
        return None

def parse_page(response, lang1, lang2):
    """Parse the page content and extract bilingual text."""
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Remove unnecessary tags
    for tag in soup.find_all(['a', 'font']):
        tag.decompose()
    
    td_lang1 = soup.find_all('td', class_=LANG_TDTAG[lang1])
    td_lang2 = soup.find_all('td', class_=LANG_TDTAG[lang2])
    
    assert len(td_lang1) == len(td_lang2), f"Length mismatch: {lang1}: {len(td_lang1)}, {lang2}: {len(td_lang2)}"
    
    lines = []
    sent1, sent2 = "", ""
    
    for text1, text2 in zip(td_lang1, td_lang2):
        text1 = text1.get_text(separator=" ", strip=True)
        text2 = text2.get_text(separator=" ", strip=True)
        
        if text1 == text2 == "併於上節":
            continue
        elif text1 == "併於上節":
            sent2 += text2
        elif text2 == "併於上節":
            sent1 += text1
        else:
            if sent1 or sent2:
                lines.append((sent1, sent2))
            sent1, sent2 = text1, text2
    
    lines.append((sent1, sent2))
    
    # Remove spaces for Chinese text
    if lang1 == 'zh':
        lines = [(s1.replace(' ', ''), s2) for s1, s2 in lines]
    elif lang2 == 'zh':
        lines = [(s1, s2.replace(' ', '')) for s1, s2 in lines]
    
    return lines

def process_url(url, lang1, lang2):
    """Process a single URL for a language pair."""
    url = url.replace("{lang1}", LANG_CODE[lang1]).replace("{lang2}", LANG_CODE[lang2])
    response = fetch_url(url)
    if response:
        return parse_page(response, lang1, lang2)
    return []

def write_to_file(filename, lines):
    """Write extracted lines to a file."""
    with open(filename, 'a', encoding='utf-8') as file:
        for line in lines:
            file.write(f"{line[0]}\t{line[1]}\n")

def main():
    for bible in BIBLES:
        print(f"Processing {bible}...")
        
        # Load URLs
        with open(f'{bible}.json', 'r') as f:
            links = json.load(f)
        urls = list(links.values())
        
        # Process language combinations
        lang_combinations = list(itertools.combinations(LANGS, 2))
        for lang1, lang2 in tqdm(lang_combinations, desc="Language pairs"):
            filename = f'{lang1}-{lang2}-{bible}.tsv'
            
            for url in tqdm(urls, desc=f"URLs for {lang1}-{lang2}", leave=False):
                lines = process_url(url, lang1, lang2)
                if lines:
                    write_to_file(filename, lines)

if __name__ == "__main__":
    main()