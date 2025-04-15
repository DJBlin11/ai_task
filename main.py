import sys
import asyncio

# selectorEventLoopPolicy for windows
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import aiohttp
from bs4 import BeautifulSoup
import re
import csv
from urllib.parse import urlparse, urljoin
from datetime import datetime
from googleapiclient.discovery import build
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("API_KEY")
cse_id = os.getenv("CSE_ID")

if not api_key or not cse_id:
    raise ValueError("API ключи не найдены. Убедитесь, что они указаны в файле .env")

# basic headers to avoid 403 errors
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.6312.86 Safari/537.36"
    )
}

# filters out test emails
def is_test_email(email):
    test_emails = ['example@example.com', 'test@test.com', 'contact@domain.com']
    test_domains = ['example.com', 'test.com', 'domain.com']
    if any(email.lower() == test_email for test_email in test_emails):
        return True
    domain = email.split('@')[1]
    if domain.lower() in test_domains:
        return True
    return False

def is_user_email(email):
    user_domains = ['gmail.com', 'yahoo.com', 'hotmail.com']
    domain = email.split('@')[1]
    return domain.lower() in user_domains

#  clean emails from page text
def extract_emails(text):
    raw_emails = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text)
    cleaned = set()
    for email in raw_emails:
        email = email.lower().rstrip('.')
        if not is_test_email(email) and not is_user_email(email):
            cleaned.add(email)
    return list(cleaned)

def has_contact_form(soup):
    return bool(soup.find('form'))

def has_contact_button(soup):
    return bool(soup.find('a', string=re.compile('contact', re.I)))

# scraper function
async def scrape_page(session, url):
    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status != 200:
                return [], False
            text = await response.text()
            soup = BeautifulSoup(text, 'html.parser')
            emails = extract_emails(text)
            form_found = has_contact_form(soup)
            contact_button_found = has_contact_button(soup)
            return emails, form_found or contact_button_found
    except Exception as e:
        print(f"Ошибка при обработке {url}: {e}")
        return [], False

def google_search(query, api_key, cse_id, num_results=10, num_pages=5):
    service = build("customsearch", "v1", developerKey=api_key)
    results = []
    for start_index in range(1, num_pages * 10 + 1, 10):
        res = service.cse().list(q=query, cx=cse_id, num=num_results, start=start_index).execute()
        results.extend(res.get("items", []))
    return results

def get_unique_domains(results, limit=5):
    seen = set()
    domains = []
    for item in results:
        domain = urlparse(item['link']).netloc
        if domain in seen or "reddit.com" in domain:
            continue
        seen.add(domain)
        domains.append((domain, item['link']))
        if len(domains) >= limit:
            break
    return domains

async def main():
    query = "thai dishes recipes"
    results = google_search(query, api_key, cse_id, num_results=10, num_pages=5)
    domains = get_unique_domains(results, limit=5)

    seen_emails = set()
    all_rows = []

    async with aiohttp.ClientSession() as session:
        for domain, entry_url in domains:
            print(f"...Обработка домена: {domain}")
            checked_pages = set()
            urls_to_check = [f"https://{domain}/", entry_url]
            for suffix in ["/contact", "/about", "/advertising", "/contact-us", "/about-us"]:
                urls_to_check.append(urljoin(f"https://{domain}", suffix))

            for url in urls_to_check:
                if url in checked_pages:
                    continue
                checked_pages.add(url)
                emails, form_found = await scrape_page(session, url)
                if emails or form_found:
                    for email in emails or [""]:
                        if email not in seen_emails:
                            seen_emails.add(email)
                            all_rows.append({
                                'date': datetime.now().strftime("%Y-%m-%d %H:%M"),
                                'domain': domain,
                                'page': url,
                                'emails': email,
                                'form_found_page': "True" if form_found else ""
                            })

     # save to a CSV
    with open('google_scrape_results.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['date', 'domain', 'page', 'emails', 'form_found_page']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    print("Завершено! Результаты сохранены в 'google_scrape_results.csv'")

if __name__ == "__main__":
    asyncio.run(main())