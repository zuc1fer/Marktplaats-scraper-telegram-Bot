import asyncio
import csv
import io
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, Any, List, Set
from urllib.parse import quote_plus, urljoin, quote

import requests
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ConversationHandler
)

BOT_TOKEN = "8243880842:AAF7GDrgcQeSmssp1jBXkWm3jILG-RU5LWs"

BASE = "https://www.marktplaats.nl"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/140.0.0.0 Safari/537.36"
)
DEFAULT_PAGES = 500
THREADS_COUNT = 10

AWAITING_KEYWORDS, AWAITING_PROXIES, AWAITING_PAGES = range(3)

SKIP_PATTERNS = [
    r'\bop voorraad\b', r'\bleverbaar\b', r'\brefurbished\b', r'\bgarantie\b',
    r'\bshowroom\b', r'\bmontage\b', r'\binstallatie\b', r'\blegservice\b',
    r'\btickets\b', r'\bverhuur\b', r'\bte huur\b', r'\bdeal prijs\b',
    r'\bincl\.?\b', r'\binclusief\b', r'\bstuks\b', r'\bpartijen\b',
    r'\bpartijkoop\b', r'\bpartijverkoop\b', r'\bautomaat\b', r'\bwaardebonnen\b',
    r'\blegbordstelling\b', r'\bvitrine\b', r'\bwinkelkast\b', r'\bhoreca\b',
    r'\bmeubelrestauratie\b', r'\bgevraagd\b', r'\bgezocht\b', r'\brenovatie\b',
    r'\bvloer\b', r'\btrap\b', r'\bairco\b', r'\bvakantie\b', r'\bchalet\b',
    r'\bbed and breakfast\b', r'\bb&b\b', r'\bkantoor\b', r'\bbureau\b',
    r'\bgratis verzenden\b', r'\btotaal overzicht\b', r'\baanbieding\b',
    r'\bkoopje\b', r'\bop=op\b', r'\breparatie\b', r'\bverkoop\b',
    r'\bshop\b', r'\bstore\b', r'\bjuwelier\b', r'\bbv\b', r'\bvof\b',
    r'\bgroup\b', r'\bgroep\b', r'\bservice\b', r'\bhandel\b',
    r'\bgroothandel\b', r'\batelier\b', r'\bonderneming\b', r'\bspecialist\b',
    r'\bmodelbouw\b', r'\bbikesland\b',
    r'\.nl\b', r'\.com\b', r'\.be\b', r'\.eu\b', r'\.org\b',
    r'\bwww\.\b', r'\bhttp\b', r'\bwebsite\b', r'\bwebshop\b',
    r'\bonline\b', r'\be-commerce\b', r'\bgrote aantallen\b',
    r'\bwholesale\b', r'\bretail\b', r'\bbedrijf\b', r'\bcompany\b',
    r'\bltd\b', r'\blimited\b', r'\bcorp\b', r'\bcorporation\b',
    r'\bfabrikant\b', r'\bimporteur\b', r'\bdistributeur\b',
    r'\bvoordeel\b', r'\bactie\b', r'\bkorting\b', r'\bsale\b',
    r'\bpakket\b', r'\bset van\b', r'\bbulk\b', r'\bvoorraad\b',
    r'\blevertijd\b', r'\blevering\b', r'\bmagazijn\b', r'\bopslag\b',
    r'\bvakman\b', r'\binstallateur\b', r'\bmonteur\b', r'\btechniek\b',
    r'\bverhuurservice\b', r'\brentals?\b', r'\blease\b', r'\bleasing\b',
    r'\breclame\b', r'\bpromotie\b', r'\bsponsoring\b',
    r'\bzoekt\b', r'\bwil kopen\b', r'\bgroot aantal\b',
    r'\bper stuk\b', r'\bper set\b', r'\bminimum afname\b',
    r'\bcertificaat\b', r'\bgediplomeerd\b', r'\berkend\b',
    r'\ball-in\b', r'\bpakket deal\b', r'\bcombinatie\b',
    r'\btweedehands zaak\b', r'\bkringloop\b', r'\bopkoper\b',
]

SKIP_REGEX = re.compile('|'.join(SKIP_PATTERNS), re.IGNORECASE)


def parse_proxy_string(proxy_string: str) -> Optional[Dict[str, Any]]:
    if not proxy_string:
        return None
    try:
        parts = proxy_string.split(':')
        if len(parts) != 4:
            return None
        hostname, port, username, password = parts
        try:
            port_int = int(port)
        except ValueError:
            return None
        return {
            'hostname': hostname.strip(),
            'port': port_int,
            'username': username.strip(),
            'password': password.strip()
        }
    except:
        return None


def create_session(user_agent: str = DEFAULT_USER_AGENT, proxy_config: Optional[Dict[str, Any]] = None) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
    })
    if proxy_config:
        proxy_url = f"http://{proxy_config['username']}:{proxy_config['password']}@{proxy_config['hostname']}:{proxy_config['port']}"
        s.proxies = {'http': proxy_url, 'https': proxy_url}
        s.timeout = 20
    else:
        s.timeout = 15
    return s


def _extract_braced_object(text: str, start_pos: int) -> Optional[str]:
    i = start_pos
    n = len(text)
    stack = []
    in_str = False
    str_char = None
    escape = False
    while i < n:
        ch = text[i]
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == str_char:
                in_str = False
        else:
            if ch in ('"', "'"):
                in_str = True
                str_char = ch
            elif ch == "{":
                stack.append("{")
            elif ch == "}":
                if not stack:
                    return text[start_pos:i + 1]
                stack.pop()
                if not stack:
                    return text[start_pos:i + 1]
        i += 1
    return None


def find_config_object(text: str) -> Optional[Dict[str, Any]]:
    m = re.search(r'window\.__CONFIG__\s*=\s*{', text) or re.search(r'__CONFIG__\s*=\s*{', text)
    if not m:
        return None
    obj_start = text.find("{", m.end() - 1)
    if obj_start == -1:
        return None
    obj_text = _extract_braced_object(text, obj_start)
    if not obj_text:
        return None
    obj_text = obj_text.rstrip()
    if obj_text.endswith(";"):
        obj_text = obj_text[:-1]
    try:
        return json.loads(obj_text)
    except:
        cleaned = re.sub(r',\s*([}\]])', r'\1', obj_text)
        try:
            return json.loads(cleaned)
        except:
            return None


def normalize_phone(phone_raw: Optional[str]) -> str:
    if not phone_raw:
        return ""
    return re.sub(r"[^\d+]", "", phone_raw)


def parse_price_text(price_text: str) -> Optional[float]:
    if not price_text:
        return None
    txt = re.sub(r"[^\d\.,\-]", "", price_text.strip())
    if not txt:
        return None
    if "." in txt and "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
    else:
        if "." in txt and "," not in txt:
            txt = txt.replace(".", "")
        if "," in txt and "." not in txt:
            txt = txt.replace(",", ".")
    try:
        return float(txt)
    except:
        return None


def extract_price_from_soup(soup: BeautifulSoup) -> Optional[float]:
    el = soup.select_one("div.ListingHeader-price") or soup.find(attrs={"class": re.compile(r"ListingHeader-price")})
    if el:
        raw = el.get_text(separator=" ", strip=True)
        return parse_price_text(raw)
    return None


def extract_ld_breadcrumb_name(soup: BeautifulSoup) -> Optional[str]:
    scripts = soup.find_all("script", {"type": "application/ld+json"})
    for s in scripts:
        raw = s.string or "".join(t for t in s.contents if isinstance(t, str)).strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except:
            try:
                first, last = raw.find("{"), raw.rfind("}")
                if first != -1 and last != -1 and last > first:
                    parsed = json.loads(raw[first:last+1])
                else:
                    continue
            except:
                continue
        items = parsed if isinstance(parsed, list) else [parsed] if isinstance(parsed, dict) else []
        for obj in items:
            if isinstance(obj, dict):
                if isinstance(obj.get("itemListElement"), list) and obj["itemListElement"]:
                    name = obj["itemListElement"][-1].get("name")
                    if name:
                        return name
                if isinstance(obj.get("@graph"), list):
                    for node in obj["@graph"]:
                        ile = node.get("itemListElement")
                        if isinstance(ile, list) and ile:
                            name = ile[-1].get("name")
                            if name:
                                return name
    return None


def listing_has_website_button(soup: BeautifulSoup) -> bool:
    if soup.select_one("i.hz-SvgIconWebsite"):
        return True
    for a in soup.select('a.SellerContactOptions-link'):
        text = a.get_text(separator=" ", strip=True).lower()
        if "website" in text:
            return True
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("https://admarkt.marktplaats.nl/bside/url/"):
            return True
    return False


def is_business_seller(seller_name: Optional[str]) -> bool:
    if not seller_name:
        return False
    
    if SKIP_REGEX.search(seller_name):
        return True
    
    name_lower = seller_name.lower()
    
    capitals = sum(1 for c in seller_name if c.isupper())
    if capitals > 5 and len(seller_name) > 10:
        return True
    
    if any(x in name_lower for x in ['bv ', ' bv', 'b.v.', 'vof ', ' vof', 'v.o.f.']):
        return True
    
    if re.search(r'\d{3,}', seller_name) or '@' in seller_name:
        return True
    
    if len(seller_name) > 8 and seller_name.isupper():
        return True
    
    return False


def extract_listing_from_html(html: str, url: str) -> Dict[str, Optional[Any]]:
    soup = BeautifulSoup(html, "html.parser")

    result = {
        "url": url,
        "listing_name": None,
        "seller_name": None,
        "location": None,
        "phone": None,
        "price": None,
    }

    listing_name = extract_ld_breadcrumb_name(soup)
    if listing_name:
        result["listing_name"] = listing_name

    cfg = find_config_object(html)
    if cfg:
        listing = cfg.get("listing", {}) or {}
        seller = listing.get("seller", {}) or {}

        result["seller_name"] = seller.get("name") or (
            listing.get("customDimensions") and next((d.get("value") for d in listing.get("customDimensions", []) if d.get("index") == "seller_name"), None)
        )

        loc = seller.get("location")
        if isinstance(loc, dict):
            result["location"] = loc.get("cityName") or loc.get("city") or None

        phone_raw = seller.get("phoneNumber") or seller.get("phone") or None
        if phone_raw:
            result["phone"] = normalize_phone(phone_raw)

        price_info = listing.get("priceInfo") or {}
        price_cents = price_info.get("priceCents")
        if price_cents is not None:
            try:
                result["price"] = int(price_cents) / 100.0
            except:
                pass

        if not result["listing_name"]:
            title = listing.get("title")
            if title:
                result["listing_name"] = title

    if not result["seller_name"]:
        el = soup.select_one("div.PhoneDialog-name")
        if el:
            result["seller_name"] = el.get_text(strip=True)

    if not result["location"]:
        el = soup.select_one("div.PhoneDialog-location")
        if el:
            loctxt = el.get_text(separator=" ", strip=True)
            loctxt = re.sub(r"^\W+", "", loctxt)
            parts = loctxt.split()
            result["location"] = parts[-1] if parts else loctxt

    if not result["phone"]:
        el = soup.select_one("div.PhoneDialog-phone")
        if el:
            pr = el.get_text(strip=True)
            result["phone"] = normalize_phone(pr)

    if not result["price"]:
        price_numeric = extract_price_from_soup(soup)
        if price_numeric:
            result["price"] = price_numeric

    if not result["listing_name"]:
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            result["listing_name"] = h1.get_text(strip=True)
        else:
            meta = soup.find("meta", {"property": "og:title"}) or soup.find("meta", {"name": "twitter:title"})
            if meta and meta.get("content"):
                result["listing_name"] = meta.get("content")

    skip_reasons = []
    
    if listing_has_website_button(soup):
        skip_reasons.append("website_button")
    
    if is_business_seller(result.get("seller_name")):
        skip_reasons.append("business_seller")
    
    if skip_reasons:
        result["_skip"] = skip_reasons
        return result

    return result


def extract_listing_links_from_search(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.select("a.hz-Link.hz-Link--block.hz-Listing-coverLink")
    links = []
    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        full = urljoin(BASE, href)
        if full not in links:
            links.append(full)
    return links


def get_total_pages_for_keyword(session: requests.Session, keyword: str) -> Optional[int]:
    token = make_keyword_token(keyword)
    search_url = f"{BASE}/q/{token}/"
    
    try:
        html = fetch_url(session, search_url, delay=0.0, max_retries=2)
        if not html:
            return None
        
        soup = BeautifulSoup(html, "html.parser")
        pagination_span = soup.select_one("span.hz-PaginationControls-pagination-amountOfPages")
        if not pagination_span:
            return None
        
        text = pagination_span.get_text(strip=True)
        match = re.search(r'van\s+(\d+)', text, re.IGNORECASE)
        if match:
            return int(match.group(1))
        
        return None
    except Exception:
        return None


def fetch_url(session: requests.Session, url: str, delay: float = 0.0, max_retries: int = 3) -> Optional[str]:
    for attempt in range(max_retries):
        try:
            if delay:
                time.sleep(delay)
            r = session.get(url, timeout=session.timeout)
            r.raise_for_status()
            return r.text
        except:
            if attempt == max_retries - 1:
                return None
            time.sleep(1)
    return None


def is_dutch_mobile(phone_number):
    if not phone_number:
        return False
    phone_str = str(phone_number)
    if 'e+' in phone_str.lower():
        try:
            phone_str = str(int(float(phone_str)))
        except:
            return False
    clean_number = re.sub(r'[\s\-\(\)]', '', phone_str)
    return bool(
        re.match(r'^06\d{8}$', clean_number) or
        re.match(r'^\+316\d{8}$', clean_number) or
        re.match(r'^316\d{8}$', clean_number)
    )


def normalize_phone_number(phone_number):
    if not phone_number:
        return None
    phone_str = str(phone_number)
    if 'e+' in phone_str.lower():
        try:
            phone_str = str(int(float(phone_str)))
        except:
            return None
    clean_number = re.sub(r'[\s\-\(\)]', '', phone_str)
    if re.match(r'^06\d{8}$', clean_number):
        return '+31' + clean_number[1:]
    if re.match(r'^\+316\d{8}$', clean_number):
        return clean_number
    if re.match(r'^316\d{8}$', clean_number):
        return '+' + clean_number
    return clean_number


def create_whatsapp_web_link(phone: str, seller_name: str, listing_name: str, price: Optional[float]) -> Optional[str]:
    if not phone:
        return None
    
    clean = re.sub(r'[^\d+]', '', phone)
    if clean.startswith('+'):
        clean = clean[1:]
    elif clean.startswith('0'):
        clean = '31' + clean[1:]
    
    seller = seller_name if seller_name else "verkoper"
    ad = listing_name if listing_name else "advertentie"
    price_str = f"€{price:.2f}" if price else "prijs onbekend"
    
    message = f"lorem ipsum {seller} {ad} {price_str}"
    
    encoded_message = quote(message)
    
    return f"https://wa.me/{clean}/?text={encoded_message}"


def gather_listing_links_parallel(session_template: requests.Session, keyword: str, pages: int, 
                                   max_links: int, delay: float, workers: int,
                                   proxy_pool: Optional[List[Dict[str, Any]]]) -> List[str]:
    token = make_keyword_token(keyword)
    collected: List[str] = []
    seen: Set[str] = set()
    
    search_urls = []
    for page in range(1, pages + 1):
        search_url = f"{BASE}/q/{token}/" if page == 1 else f"{BASE}/q/{token}/p/{page}/"
        search_urls.append(search_url)
    
    def pick_proxy(i: int) -> Optional[Dict[str, Any]]:
        if not proxy_pool:
            return None
        return proxy_pool[i % len(proxy_pool)]
    
    with ThreadPoolExecutor(max_workers=min(workers, len(search_urls))) as ex:
        futures = {}
        for idx, url in enumerate(search_urls):
            sess = create_session(session_template.headers.get('User-Agent'), pick_proxy(idx))
            futures[ex.submit(fetch_url, sess, url, delay)] = url
        
        for fut in as_completed(futures):
            html = None
            try:
                html = fut.result()
            except:
                pass
            
            if not html:
                continue
            
            links = extract_listing_links_from_search(html)
            for l in links:
                if l not in seen:
                    seen.add(l)
                    collected.append(l)
                    if len(collected) >= max_links:
                        return collected
    
    return collected


def run_single_keyword(keyword: str, pages: int, max_links: int, workers: int, delay: float, 
                       user_agent: str, proxy_pool: Optional[List[Dict[str, Any]]] = None,
                       global_seen_phones: Set[str] = None,
                       global_seen_urls: Set[str] = None,
                       progress_callback=None) -> Dict[str, List]:
    if global_seen_phones is None:
        global_seen_phones = set()
    if global_seen_urls is None:
        global_seen_urls = set()
        
    base_session = create_session(user_agent, proxy_pool[0] if proxy_pool else None)
    
    if progress_callback:
        progress_callback(f"🔍 Searching for: '{keyword}'\n📄 Gathering listing links from {pages} page(s)...")
    
    listing_urls = gather_listing_links_parallel(base_session, keyword, pages, max_links, delay, workers, proxy_pool)
    
    if not listing_urls:
        return {"full": [], "phones": [], "links": [], "stats": {"skipped": {"website_button": 0, "business_seller": 0, "no_phone": 0, "duplicate": 0}}}

    if progress_callback:
        progress_callback(f"✅ Found {len(listing_urls)} listings\n🔄 Scraping with {workers} workers...")

    results_full: List[Dict[str, Any]] = []
    results_phones: List[str] = []
    results_links: List[str] = []
    
    skipped = {"website_button": 0, "business_seller": 0, "no_phone": 0, "duplicate": 0}

    def pick_proxy(i: int) -> Optional[Dict[str, Any]]:
        if not proxy_pool:
            return None
        return proxy_pool[i % len(proxy_pool)]

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {}
        for idx, url in enumerate(listing_urls):
            sess = create_session(user_agent, pick_proxy(idx))
            futures[ex.submit(fetch_url, sess, url, delay * 0.5)] = url

        done = 0
        for fut in as_completed(futures):
            url = futures[fut]
            done += 1
            html = None
            try:
                html = fut.result()
            except:
                pass

            if not html:
                continue

            data = extract_listing_from_html(html, url)

            if isinstance(data, dict) and data.get("_skip"):
                skip_reasons = data["_skip"]
                for reason in skip_reasons:
                    skipped[reason] = skipped.get(reason, 0) + 1
                continue

            phone = data.get("phone")
            
            if not phone or not is_dutch_mobile(phone):
                skipped["no_phone"] = skipped.get("no_phone", 0) + 1
                continue
            
            normalized_phone = normalize_phone_number(phone)
            if not normalized_phone:
                skipped["no_phone"] = skipped.get("no_phone", 0) + 1
                continue
            
            if normalized_phone in global_seen_phones or url in global_seen_urls:
                skipped["duplicate"] += 1
                continue
            
            global_seen_phones.add(normalized_phone)
            global_seen_urls.add(url)
            
            results_phones.append(normalized_phone)
            results_links.append(url)
            
            whatsapp_link_web = create_whatsapp_web_link(
                normalized_phone,
                data.get('seller_name'),
                data.get('listing_name'),
                data.get('price')
            )
            
            listing_name = data.get('listing_name', '')
            if listing_name and len(listing_name) > 80:
                listing_name = listing_name[:80] + '...'
            
            full_entry = {
                'listing_name': listing_name,
                'seller_name': data.get('seller_name'),
                'location': data.get('location'),
                'phone': normalized_phone,
                'price': data.get('price'),
                'url': url,
                'whatsapp': whatsapp_link_web
            }
            results_full.append(full_entry)
            
            if progress_callback and (done % 50 == 0 or done == len(listing_urls)):
                progress_callback(f"⏳ Progress: {done}/{len(listing_urls)} | Valid: {len(results_full)}")

    return {
        "full": results_full,
        "phones": results_phones,
        "links": results_links,
        "stats": {"skipped": skipped}
    }


def make_keyword_token(keyword: str) -> str:
    return quote_plus(keyword)


def create_csv_file(data: List, fieldnames: List[str]) -> io.BytesIO:
    string_buffer = io.StringIO()
    
    if len(fieldnames) == 1:
        writer = csv.writer(string_buffer)
        writer.writerow(fieldnames)
        for item in data:
            writer.writerow([item])
    else:
        writer = csv.DictWriter(string_buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    csv_string = string_buffer.getvalue()
    bytes_buffer = io.BytesIO(csv_string.encode('utf-8'))
    bytes_buffer.seek(0)
    bytes_buffer.name = 'data.csv'
    
    return bytes_buffer


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_message = """
🤖 **Marktplaats Scraper Bot**

Welcome! This bot scrapes Marktplaats listings and extracts contact information.

📋 **How to use:**

1️⃣ Enter your search keywords (one per line or comma-separated)
2️⃣ Upload proxy file (optional - txt file with format: host:port:user:pass)
3️⃣ Configure pages to scrape (default: 500)
4️⃣ Receive 3 CSV files with results

🎯 **What gets filtered:**
• Business sellers and commercial listings
• Listings with website buttons
• Invalid/non-mobile phone numbers
• Duplicate entries

📊 **Output files:**
• `phones.csv` - Phone numbers only
• `links.csv` - Listing URLs
• `all_info.csv` - Complete data with WhatsApp links

Ready to start? Click the button below! 👇
    """
    
    keyboard = [
        [InlineKeyboardButton("🚀 Start Scraping", callback_data="start_scraping")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        welcome_message,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "start_scraping":
        context.user_data['use_proxy'] = False
        context.user_data['proxy_pool'] = None
        
        keyboard = [
            [InlineKeyboardButton("✅ Yes, use proxies", callback_data="use_proxy_yes")],
            [InlineKeyboardButton("❌ No, direct connection", callback_data="use_proxy_no")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "🔐 **Proxy Configuration**\n\nDo you want to use proxies?",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    elif query.data == "use_proxy_yes":
        context.user_data['use_proxy'] = True
        await query.edit_message_text(
            "📤 **Upload Proxy File**\n\n"
            "Please upload a text file with proxies.\n"
            "Format: `host:port:username:password` (one per line)\n\n"
            "Or type /skip to continue without proxies.",
            parse_mode='Markdown'
        )
        return AWAITING_PROXIES
    
    elif query.data == "use_proxy_no":
        context.user_data['use_proxy'] = False
        context.user_data['proxy_pool'] = None
        await query.edit_message_text(
            "🔍 **Enter Keywords**\n\n"
            "Please enter your search keywords.\n"
            "You can enter multiple keywords (one per line or comma-separated)\n\n"
            "Example:\n`guitar, piano\nviolin`",
            parse_mode='Markdown'
        )
        return AWAITING_KEYWORDS


async def receive_proxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.document:
        document = update.message.document
        file = await context.bot.get_file(document.file_id)
        file_content = await file.download_as_bytearray()
        
        proxy_text = file_content.decode('utf-8')
        proxy_pool = []
        
        for line in proxy_text.split('\n'):
            line = line.strip()
            if line and not line.startswith('#'):
                proxy_config = parse_proxy_string(line)
                if proxy_config:
                    proxy_pool.append(proxy_config)
        
        if proxy_pool:
            context.user_data['proxy_pool'] = proxy_pool
            await update.message.reply_text(
                f"✅ Loaded {len(proxy_pool)} proxies successfully!\n\n"
                "🔍 **Enter Keywords**\n\n"
                "Please enter your search keywords.\n"
                "You can enter multiple keywords (one per line or comma-separated)\n\n"
                "Example:\n`guitar, piano\nviolin`",
                parse_mode='Markdown'
            )
            return AWAITING_KEYWORDS
        else:
            await update.message.reply_text(
                "❌ No valid proxies found in file.\n"
                "Please upload a valid proxy file or type /skip to continue without proxies."
            )
            return AWAITING_PROXIES
    else:
        await update.message.reply_text(
            "❌ Please upload a text file with proxies or type /skip."
        )
        return AWAITING_PROXIES


async def skip_proxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['use_proxy'] = False
    context.user_data['proxy_pool'] = None
    
    await update.message.reply_text(
        "🔍 **Enter Keywords**\n\n"
        "Please enter your search keywords.\n"
        "You can enter multiple keywords (one per line or comma-separated)\n\n"
        "Example:\n`guitar, piano\nviolin`",
        parse_mode='Markdown'
    )
    return AWAITING_KEYWORDS


async def receive_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    
    keywords = []
    for line in text.split('\n'):
        for keyword in line.split(','):
            keyword = keyword.strip()
            if keyword:
                keywords.append(keyword)
    
    if not keywords:
        await update.message.reply_text(
            "❌ No valid keywords found. Please try again."
        )
        return AWAITING_KEYWORDS
    
    context.user_data['keywords'] = keywords
    context.user_data['keyword_index'] = 0
    context.user_data['keyword_pages_map'] = {}
    
    progress_msg = await update.message.reply_text(
        f"🔍 Detecting available pages for keyword 1/{len(keywords)}...\n"
        "This may take a moment..."
    )
    
    await process_next_keyword_pages(update, context, progress_msg)
    
    return AWAITING_PAGES


async def process_next_keyword_pages(update: Update, context: ContextTypes.DEFAULT_TYPE, msg=None):
    keywords = context.user_data['keywords']
    keyword_index = context.user_data['keyword_index']
    
    if keyword_index >= len(keywords):
        await start_scraping_process(update, context)
        return
    
    keyword = keywords[keyword_index]
    proxy_pool = context.user_data.get('proxy_pool')
    
    proxy = proxy_pool[keyword_index % len(proxy_pool)] if proxy_pool else None
    session = create_session(DEFAULT_USER_AGENT, proxy)
    total_pages = get_total_pages_for_keyword(session, keyword)
    
    page_info = f"📊 **Keyword {keyword_index + 1}/{len(keywords)}: `{keyword}`**\n\n"
    
    if total_pages:
        page_info += f"✅ Available pages: **{total_pages}**\n"
        page_info += f"📄 Default: **{DEFAULT_PAGES}** pages\n\n"
        
        if total_pages < DEFAULT_PAGES:
            page_info += f"⚠️ Only {total_pages} pages available\n\n"
        
        page_info += f"How many pages to scrape for this keyword?\n"
        page_info += f"(Enter number 1-{total_pages} or /default for {min(DEFAULT_PAGES, total_pages)})"
    else:
        page_info += f"⚠️ Unable to detect available pages\n\n"
        page_info += f"How many pages to scrape for this keyword?\n"
        page_info += f"(Enter number or /default for {DEFAULT_PAGES})"
    
    context.user_data['current_keyword_max_pages'] = total_pages
    
    if msg:
        await msg.edit_text(page_info, parse_mode='Markdown')
    else:
        await update.message.reply_text(page_info, parse_mode='Markdown')


async def receive_pages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    
    keywords = context.user_data['keywords']
    keyword_index = context.user_data['keyword_index']
    keyword = keywords[keyword_index]
    max_pages = context.user_data.get('current_keyword_max_pages')
    
    if text == "/default":
        if max_pages:
            pages = min(DEFAULT_PAGES, max_pages)
        else:
            pages = DEFAULT_PAGES
    else:
        try:
            pages = int(text)
            if pages < 1:
                await update.message.reply_text(
                    "❌ Please enter a positive number or /default"
                )
                return AWAITING_PAGES
            
            if max_pages and pages > max_pages:
                await update.message.reply_text(
                    f"⚠️ Only {max_pages} pages available for '{keyword}'.\n"
                    f"Using {max_pages} pages instead."
                )
                pages = max_pages
                
        except ValueError:
            await update.message.reply_text(
                "❌ Please enter a valid number or /default"
            )
            return AWAITING_PAGES
    
    context.user_data['keyword_pages_map'][keyword] = pages
    
    await update.message.reply_text(
        f"✅ Set {pages} pages for '{keyword}'"
    )
    
    context.user_data['keyword_index'] += 1
    
    if context.user_data['keyword_index'] < len(keywords):
        progress_msg = await update.message.reply_text(
            f"🔍 Detecting pages for next keyword..."
        )
        await process_next_keyword_pages(update, context, progress_msg)
        return AWAITING_PAGES
    else:
        await show_config_summary_and_start(update, context)
        return ConversationHandler.END


async def show_config_summary_and_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keywords = context.user_data['keywords']
    keyword_pages_map = context.user_data['keyword_pages_map']
    
    summary = "🔧 **Final Configuration:**\n\n"
    
    for keyword in keywords:
        pages = keyword_pages_map.get(keyword, DEFAULT_PAGES)
        summary += f"• `{keyword}`: {pages} pages\n"
    
    summary += f"\n⚙️ Workers: {THREADS_COUNT}\n"
    summary += f"🔗 Proxy: {'Enabled' if context.user_data.get('use_proxy') else 'Disabled'}\n"
    summary += "\n🚀 Starting scraper...\n"
    
    await update.message.reply_text(summary, parse_mode='Markdown')
    
    await run_scraper(update, context)


async def start_scraping_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_config_summary_and_start(update, context)


async def run_scraper(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keywords = context.user_data['keywords']
    keyword_pages_map = context.user_data['keyword_pages_map']
    proxy_pool = context.user_data.get('proxy_pool')
    
    progress_msg = await update.message.reply_text("🔄 Initializing scraper...")
    
    all_phones = []
    all_links = []
    all_full = []
    
    global_seen_phones = set()
    global_seen_urls = set()
    
    total_stats = {
        "website_button": 0,
        "business_seller": 0,
        "no_phone": 0,
        "duplicate": 0
    }
    
    start_time = time.time()
    
    async def update_progress(message):
        try:
            await progress_msg.edit_text(message, parse_mode='Markdown')
        except:
            pass
    
    for i, keyword in enumerate(keywords, 1):
        keyword_start = time.time()
        pages_for_kw = keyword_pages_map.get(keyword, DEFAULT_PAGES)
        
        await update_progress(
            f"📍 **Keyword {i}/{len(keywords)}: '{keyword}'**\n"
            f"📄 Scraping {pages_for_kw} pages...\n"
            f"⏳ Please wait..."
        )
        
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None,
            lambda: run_single_keyword(
                keyword=keyword,
                pages=pages_for_kw,
                max_links=1000,
                workers=THREADS_COUNT,
                delay=0.2,
                user_agent=DEFAULT_USER_AGENT,
                proxy_pool=proxy_pool,
                global_seen_phones=global_seen_phones,
                global_seen_urls=global_seen_urls,
                progress_callback=None
            )
        )
        
        keyword_time = time.time() - keyword_start
        
        all_phones.extend(results["phones"])
        all_links.extend(results["links"])
        all_full.extend(results["full"])
        
        kw_stats = results["stats"]["skipped"]
        for key in total_stats:
            total_stats[key] += kw_stats.get(key, 0)
        
        await update_progress(
            f"✅ **Keyword {i}/{len(keywords)} Complete**\n\n"
            f"🔍 Keyword: `{keyword}`\n"
            f"⏱ Time: {keyword_time:.1f}s\n"
            f"✅ Valid listings: {len(results['full'])}\n"
            f"📊 Total so far: {len(all_full)}"
        )
        
        await asyncio.sleep(1)
    
    total_time = time.time() - start_time
    
    stats_message = f"""
✅ **SCRAPING COMPLETE**

⏱ **Total time:** {total_time:.1f}s
📊 **Results:** {len(all_full)} valid listings
📱 **Phone numbers:** {len(all_phones)}
🔗 **Links:** {len(all_links)}

🚫 **Filtered:**
• Website button: {total_stats['website_button']}
• Business seller: {total_stats['business_seller']}
• No valid phone: {total_stats['no_phone']}
• Duplicates: {total_stats['duplicate']}

📤 Generating CSV files...
    """
    
    await progress_msg.edit_text(stats_message, parse_mode='Markdown')
    
    try:
        if all_phones:
            await update.message.reply_text("📤 Sending phones.csv...")
            phones_csv = create_csv_file(all_phones, ['phone'])
            await update.message.reply_document(
                document=phones_csv,
                filename='phones.csv',
                caption='📱 Phone numbers only'
            )
            await asyncio.sleep(0.5)
        
        if all_links:
            await update.message.reply_text("📤 Sending links.csv...")
            links_csv = create_csv_file(all_links, ['url'])
            await update.message.reply_document(
                document=links_csv,
                filename='links.csv',
                caption='🔗 Listing URLs only'
            )
            await asyncio.sleep(0.5)
        
        if all_full:
            await update.message.reply_text("📤 Sending all_info.csv...")
            full_fieldnames = ['listing_name', 'seller_name', 'location', 'phone', 'price', 'whatsapp', 'url']
            full_csv = create_csv_file(all_full, full_fieldnames)
            await update.message.reply_document(
                document=full_csv,
                filename='all_info.csv',
                caption='📊 Complete data with WhatsApp Web links'
            )
    
    except Exception as e:
        await update.message.reply_text(
            f"❌ Error sending files: {str(e)}\n\n"
            f"Data was collected successfully but file transfer failed.\n"
            f"Valid listings: {len(all_full)}"
        )
    
    final_summary = f"""
🎉 **ALL FILES SENT**

Thank you for using Marktplaats Scraper Bot!

Type /start to begin a new search.
    """
    
    await update.message.reply_text(final_summary, parse_mode='Markdown')


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "❌ Operation cancelled.\n\nType /start to begin again."
    )
    return ConversationHandler.END


def main():
    application = Application.builder().token(BOT_TOKEN).build()
    
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            CallbackQueryHandler(button_callback)
        ],
        states={
            AWAITING_PROXIES: [
                MessageHandler(filters.Document.ALL, receive_proxies),
                CommandHandler('skip', skip_proxies)
            ],
            AWAITING_KEYWORDS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_keywords)
            ],
            AWAITING_PAGES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_pages)
            ]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CallbackQueryHandler(button_callback)
        ]
    )
    
    application.add_handler(conv_handler)
    
    print("🤖 Bot started! Press Ctrl+C to stop.")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
