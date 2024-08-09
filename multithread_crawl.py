import requests
import itertools
import time
import json
from pathlib import Path
from bs4 import BeautifulSoup
import urllib.parse
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch(url):
    try:
        response = requests.get(url)
        time.sleep(0.5) 
        if response.status_code == 200:
            return response
        else:
            print(f"Failed to retrieve the page {url}. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {url}. Exception: {e}")
        return None

def process_url(comb, url, bible, lang_code, lang_tdtag):
    lang1 = lang_code[comb[0]]
    lang2 = lang_code[comb[1]]
    url = url.replace("{lang1}", lang1).replace("{lang2}", lang2)
    
    res = fetch(url)
    if res is not None:
        soup = BeautifulSoup(res.content, 'html.parser')
        for a_tag in soup.find_all('a'):
            a_tag.decompose()

        for tag in soup.find_all('font', size='+2'):
            tag.decompose()
        
        td_lang1 = soup.find_all('td', class_=lang_tdtag[comb[0]])
        td_lang2 = soup.find_all('td', class_=lang_tdtag[comb[1]])

        assert len(td_lang1) == len(td_lang2), f"Length of {comb[0]}: {len(td_lang1)} and {comb[1]}: {len(td_lang2)} are not equal"
        lines = []
        sent1 = ""
        sent2 = ""
        for i in range(len(td_lang1)):
            text_lang1 = td_lang1[i].get_text(separator=" ", strip=True).replace('\n', '')
            text_lang2 = td_lang2[i].get_text(separator= " ", strip=True).replace('\n', '')
            if text_lang1 == "併於上節" and text_lang2 == "併於上節":
                continue
            elif text_lang1 == "併於上節" and text_lang2 != "併於上節":
                sent2 = sent2 + text_lang2
            elif text_lang1 != "併於上節" and text_lang2 == "併於上節":
                sent1 = sent1 + text_lang1
            else:
                if comb[0] == 'zh' or comb[1] == 'zh':
                    lines.append(
                        (sent1.replace(' ', ''), sent2) if comb[0] == 'zh' else (sent1, sent2.replace(' ', ''))
                    )
                else:
                    lines.append((sent1, sent2))
                sent1 = text_lang1
                sent2 = text_lang2
        if comb[0] == 'zh' or comb[1] == 'zh':
            lines.append(
                (sent1.replace(' ', ''), sent2) if comb[0] == 'zh' else (sent1, sent2.replace(' ', ''))
            )
        else:
            lines.append((sent1, sent2))
        
        with open(f'{comb[0]}-{comb[1]}-{bible}.tsv', 'a') as file:
            for line in lines:
                file.write(line[0] + '\t' + line[1] + '\n')

def main():
    urls = []
    bibles = ['n_testament', 'o_testament']
    langs = ["zh", "tru", "tay", "sed"]
    lang_code = {"zh": "tcv2019", "tru": "tru", "tay": "tay", "sed": "sed"}
    lang_tdtag = {"zh": "bstw", "tru": "nor", "tay": "nor", "sed": "bstwre"}

    for bible in bibles:
        with open(f'{bible}.json', 'r') as f:
            links = json.load(f)
        for title in links:
            urls.append(links[title])
        combs = list(itertools.combinations(langs, 2))
        
        with ThreadPoolExecutor(max_workers=10) as executor:  
            futures = []
            for comb in tqdm(combs):
                for url in tqdm(urls, leave=False):
                    futures.append(executor.submit(process_url, comb, url, bible, lang_code, lang_tdtag))
            
            for future in as_completed(futures):
                try:
                    future.result()  
                except Exception as exc:
                    print(f"An error occurred: {exc}")

if __name__ == "__main__":
    main()