import requests
from bs4 import BeautifulSoup
import sqlite3
import datetime
import time
import random
import os
import json
import logging
import sys
import traceback
import re
from urllib.parse import urlparse, urljoin
from collections import Counter
from datetime import timezone
from dateutil import parser as date_parser  

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

def parse_relative_time(text):
    """
    Parse a relative time string (e.g., '50m ago', '3h ago', '1d ago', 'a few seconds ago')
    and return a datetime object in UTC.
    """
    now = datetime.datetime.now(timezone.utc)
    text = text.lower().strip()
    # Check for common relative patterns using regular expressions
    m = re.search(r'(\d+)\s*([mhdy])', text)
    if m:
        value = int(m.group(1))
        unit = m.group(2)
        if unit == 'm':  # minutes
            return now - datetime.timedelta(minutes=value)
        elif unit == 'h':  # hours
            return now - datetime.timedelta(hours=value)
        elif unit == 'd':  # days
            return now - datetime.timedelta(days=value)
        elif unit == 'y':  # years (approximate)
            return now - datetime.timedelta(days=365*value)
    # Handle a few seconds or just now
    if 'a few seconds ago' in text or 'just now' in text:
        return now
    raise ValueError(f"Unable to parse relative time: {text}")

class FinancialNewsScraper:
    def __init__(self, db_path='financial_news.db'):
        """Initialize the scraper with a database connection"""
        self.db_path = db_path
        self.setup_database()
        
        # Common financial news sources
        self.news_sources = {
            'CNBC': 'https://www.cnbc.com/finance/',
            'Bloomberg': 'https://www.bloomberg.com/markets',
            'Reuters Finance': 'https://www.reuters.com/business/finance/',
            'Yahoo Finance': 'https://finance.yahoo.com/',
            'MarketWatch': 'https://www.marketwatch.com/',
            'Business Insider Finance': 'https://www.businessinsider.com/finance',
            'Forbes': 'https://www.forbes.com/money/'
        }
        
        # User agents to rotate (to avoid being blocked)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]
    
    def setup_database(self):
        """Create the SQLite database and tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create articles table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            url TEXT UNIQUE,
            source TEXT,
            author TEXT,
            publish_date TEXT,
            content TEXT,
            summary TEXT,
            keywords TEXT,
            retrieved_date TEXT,
            category TEXT
        )
        ''')
        
        # Create search_terms table for tracking keywords
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS search_terms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            term TEXT UNIQUE,
            last_search TEXT
        )
        ''')
        
        conn.commit()
        conn.close()
        logging.info("Database setup complete")
    
    def get_random_user_agent(self):
        """Return a random user agent from the list"""
        return random.choice(self.user_agents)
    
    def get_soup(self, url):
        """Get the BeautifulSoup object for a URL with error handling"""
        headers = {'User-Agent': self.get_random_user_agent()}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {url}: {e}")
            return None
        
    def extract_article_links(self, source_name, source_url):
        """Extract article links from a news source"""
        logging.info(f"Scraping links from {source_name}: {source_url}")
        soup = self.get_soup(source_url)
        if not soup:
            return []
        
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            
            # Handle relative URLs
            if href.startswith('/'):
                parsed_url = urlparse(source_url)
                base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                href = base_url + href
            
            # Filter for article-like URLs
            if any(keyword in href.lower() for keyword in ['/article/', '/news/', '/story/', '/2023/', '/2024/', '/2025/']):
                if href not in links and not any(href.endswith(ext) for ext in ['.jpg', '.png', '.pdf']):
                    links.append(href)
        
        logging.info(f"Found {len(links)} potential article links from {source_name}")
        return links
    
    def extract_article_content(self, url):
        """Extract article content using BeautifulSoup with robust date extraction"""
        try:
            soup = self.get_soup(url)
            if not soup:
                return None
            
            # Extract title
            title = soup.title.text.strip() if soup.title else ''
            
            # Extract author (common patterns)
            author = 'Unknown'
            author_elements = soup.select('a[rel="author"], span.author, .byline, .author')
            if author_elements:
                author = author_elements[0].text.strip()
            
            # Extract publish date using robust parsing
            publish_date = None
            date_elements = soup.select('time, .date, .published, meta[property="article:published_time"]')
            if date_elements:
                if date_elements[0].name == 'meta':
                    date_text = date_elements[0].get('content', '').strip()
                else:
                    date_text = date_elements[0].text.strip()
                try:
                    if 'ago' in date_text.lower():
                        publish_date = parse_relative_time(date_text)
                    else:
                        publish_date = date_parser.parse(date_text)
                    # Ensure we have UTC awareness
                    if publish_date.tzinfo is None:
                        publish_date = publish_date.replace(tzinfo=timezone.utc)
                    else:
                        publish_date = publish_date.astimezone(timezone.utc)
                except Exception as e:
                    logging.warning(f"Could not parse date '{date_text}' from {url}: {e}")
                    publish_date = datetime.datetime.now(timezone.utc)
            else:
                publish_date = datetime.datetime.now(timezone.utc)
            
            # Extract main content using common selectors
            content_selectors = [
                'article', '.article-body', '.article-content', '.story-body',
                '.post-content', '.entry-content', '.content', '#content',
                '[itemprop="articleBody"]', '.body'
            ]
            content_element = None
            for selector in content_selectors:
                elements = soup.select(selector)
                if elements:
                    content_element = elements[0]
                    break
            if not content_element:
                content_element = soup.body
            paragraphs = content_element.find_all('p') if content_element else []
            content = '\n\n'.join([p.text.strip() for p in paragraphs])
            
            # Simple summary: first 2 paragraphs (max 500 characters)
            summary = '\n\n'.join([p.text.strip() for p in paragraphs[:2]])
            if len(summary) > 500:
                summary = summary[:497] + '...'
            
            # Extract keywords from meta tags if available
            keywords = []
            keyword_meta = soup.find('meta', attrs={'name': 'keywords'})
            if keyword_meta:
                keywords = [k.strip() for k in keyword_meta.get('content', '').split(',')]
            
            return {
                'title': title,
                'author': author,
                'publish_date': publish_date,
                'content': content,
                'summary': summary,
                'keywords': ','.join(keywords)
            }
        except Exception as e:
            logging.error(f"Error extracting content from {url}: {e}")
            return None
        
    def process_article(self, url, source, date_range=None):
        """
        Process an individual article URL.
        If date_range is provided (tuple of start and end datetime objects in UTC),
        only save the article if its publish_date falls within the range.
        """
        logging.debug(f"Processing article: {url}")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM articles WHERE url=?", (url,))
        if cursor.fetchone():
            logging.info(f"Article already exists in database: {url}")
            conn.close()
            return False
        
        try:
            article_data = self.extract_article_content(url)
            if not article_data:
                conn.close()
                return False
            
            if date_range is not None:
                start_date, end_date = date_range
                if not (start_date <= article_data['publish_date'] <= end_date):
                    logging.info(f"Article skipped due to date filter: {url} published on {article_data['publish_date']}")
                    conn.close()
                    return False
            
            data = {
                'title': article_data['title'],
                'url': url,
                'source': source,
                'author': article_data['author'],
                'publish_date': article_data['publish_date'].strftime('%Y-%m-%d %H:%M:%S'),
                'content': article_data['content'],
                'summary': article_data['summary'],
                'keywords': article_data['keywords'],
                'retrieved_date': datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'category': self.categorize_article(article_data['content'], article_data['title'])
            }
            
            cursor.execute('''
            INSERT INTO articles (title, url, source, author, publish_date, content, summary, keywords, retrieved_date, category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data['title'], data['url'], data['source'], data['author'], data['publish_date'],
                data['content'], data['summary'], data['keywords'], data['retrieved_date'], data['category']
            ))
            
            conn.commit()
            logging.info(f"Successfully saved article: {data['title']}")
            conn.close()
            return True
            
        except Exception as e:
            logging.error(f"Error processing article {url}: {e}")
            conn.close()
            return False
    
    def categorize_article(self, content, title):
        """Simple categorization of article content"""
        categories = {
            'stocks': ['stock', 'stocks', 'equities', 'nasdaq', 'dow jones', 'nyse', 's&p'],
            'cryptocurrency': ['bitcoin', 'ethereum', 'crypto', 'blockchain', 'token', 'cryptocurrency'],
            'economy': ['economy', 'gdp', 'inflation', 'recession', 'economic growth', 'fed', 'federal reserve'],
            'markets': ['market', 'trading', 'trader', 'bulls', 'bears', 'rally', 'correction'],
            'business': ['company', 'earnings', 'revenue', 'profit', 'ceo', 'startup', 'merger', 'acquisition'],
            'personal_finance': ['investing', 'retirement', 'mortgage', 'loan', 'credit', 'debt', 'saving'],
            'real_estate': ['housing', 'real estate', 'property', 'mortgage', 'commercial real estate']
        }
        full_text = (title + " " + content).lower()
        category_scores = {}
        for category, keywords in categories.items():
            score = sum(1 for keyword in keywords if keyword.lower() in full_text)
            if score > 0:
                category_scores[category] = score
        return max(category_scores, key=category_scores.get) if category_scores else 'general'
    
    def scrape_by_date_range(self, start_date_str=None, end_date_str=None):
        """Scrape articles with date range filtering (default last 7 days) using UTC."""
        total_new_articles = 0
        if not end_date_str:
            end_date = datetime.datetime.now(timezone.utc)
        else:
            end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_date = end_date.replace(hour=23, minute=59, second=59)
        if not start_date_str:
            start_date = datetime.datetime.now(timezone.utc) - datetime.timedelta(days=7)
        else:
            start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        date_range = (start_date, end_date)
        logging.info(f"Scraping articles from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"Scraping articles from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        all_sources = list(self.news_sources.items())
        total_sources = len(all_sources)
        for source_idx, (source_name, source_url) in enumerate(all_sources):
            try:
                logging.info(f"Scraping source: {source_name}")
                print(f"Scraping source: {source_name} ({total_sources - source_idx - 1} sources remaining)")
                links = self.extract_article_links(source_name, source_url)
                if links:
                    total_links = len(links)
                    print(f"Found {total_links} articles from {source_name}")
                    start_time = time.time()
                    new_added = 0
                    for idx, link in enumerate(links):
                        if self.process_article(link, source_name, date_range=date_range):
                            total_new_articles += 1
                            new_added += 1
                        processed = idx + 1
                        if processed > 1:
                            elapsed_time = time.time() - start_time
                            avg_time = elapsed_time / processed
                            remaining = total_links - processed
                            eta = remaining * avg_time
                            mins = int(eta // 60)
                            secs = int(eta % 60)
                            progress_msg = f"\rProgress: {processed}/{total_links} articles | ETA: {mins}m {secs}s | New articles: {new_added}    "
                            sys.stdout.write(progress_msg)
                            sys.stdout.flush()
                        time.sleep(random.uniform(0.5, 1.5))
                    print(f"\nCompleted {source_name}: Added {new_added} new articles")
            except Exception as e:
                logging.error(f"Error scraping {source_name}: {e}")
                print(f"Error scraping {source_name}: {e}")
        logging.info(f"Scraping completed. Added {total_new_articles} new articles.")
        self.analyze_articles_by_date_range(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        return total_new_articles
    
    def scrape_all_articles(self):
        """Scrape articles from all defined news sources without date filtering."""
        total_new_articles = 0
        all_sources = list(self.news_sources.items())
        total_sources = len(all_sources)
        for source_idx, (source_name, source_url) in enumerate(all_sources):
            try:
                logging.info(f"Scraping source: {source_name} (all articles)")
                print(f"Scraping source: {source_name} ({total_sources - source_idx - 1} sources remaining)")
                links = self.extract_article_links(source_name, source_url)
                if links:
                    total_links = len(links)
                    print(f"Found {total_links} articles from {source_name}")
                    start_time = time.time()
                    new_added = 0
                    for idx, link in enumerate(links):
                        if self.process_article(link, source_name, date_range=None):
                            total_new_articles += 1
                            new_added += 1
                        processed = idx + 1
                        if processed > 1:
                            elapsed_time = time.time() - start_time
                            avg_time = elapsed_time / processed
                            remaining = total_links - processed
                            eta = remaining * avg_time
                            mins = int(eta // 60)
                            secs = int(eta % 60)
                            progress_msg = f"\rProgress: {processed}/{total_links} articles | ETA: {mins}m {secs}s | New articles: {new_added}    "
                            sys.stdout.write(progress_msg)
                            sys.stdout.flush()
                        time.sleep(random.uniform(0.5, 1.5))
                    print(f"\nCompleted {source_name}: Added {new_added} new articles")
            except Exception as e:
                logging.error(f"Error scraping {source_name}: {e}")
                print(f"Error scraping {source_name}: {e}")
        logging.info(f"Full scraping completed. Added {total_new_articles} new articles.")
        return total_new_articles
    
    def analyze_articles_by_date_range(self, start_date, end_date):
        """
        Analyze articles collected within a date range: count articles by source,
        category, daily distribution, and list potential issues.
        """
        start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_dt = end_dt.replace(hour=23, minute=59, second=59)
        start_date_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        SELECT source, COUNT(*) as article_count
        FROM articles
        WHERE publish_date >= ? AND publish_date <= ?
        GROUP BY source
        ORDER BY article_count DESC
        ''', (start_date_str, end_date_str))
        source_counts = cursor.fetchall()
        cursor.execute('''
        SELECT id, title, url, source
        FROM articles
        WHERE publish_date >= ? AND publish_date <= ?
        AND (content IS NULL OR LENGTH(content) < 500)
        ''', (start_date_str, end_date_str))
        articles_with_missing_content = cursor.fetchall()
        cursor.execute('''
        SELECT DATE(publish_date) as pub_date, COUNT(*) as daily_count
        FROM articles
        WHERE publish_date >= ? AND publish_date <= ?
        GROUP BY pub_date
        ORDER BY pub_date
        ''', (start_date_str, end_date_str))
        daily_distribution = cursor.fetchall()
        cursor.execute('''
        SELECT category, COUNT(*) as category_count
        FROM articles
        WHERE publish_date >= ? AND publish_date <= ?
        GROUP BY category
        ORDER BY category_count DESC
        ''', (start_date_str, end_date_str))
        category_distribution = cursor.fetchall()
        daily_counts = {day: count for day, count in daily_distribution}
        date_range = []
        current_date = start_dt.date()
        while current_date <= end_dt.date():
            date_range.append(current_date.strftime('%Y-%m-%d'))
            current_date += datetime.timedelta(days=1)
        if daily_counts:
            average_daily_count = sum(count for _, count in daily_distribution) / len(daily_distribution)
            threshold = max(3, average_daily_count * 0.3)
            missing_days = [day for day in date_range if day not in daily_counts]
            days_with_few_articles = [(day, daily_counts[day]) for day in daily_counts if daily_counts[day] < threshold]
        else:
            missing_days = date_range
            days_with_few_articles = []
        conn.close()
        print("\n===== ARTICLE ANALYSIS REPORT =====")
        print(f"Date Range: {start_date} to {end_date}")
        print("\n1. ARTICLES BY SOURCE:")
        for source, count in source_counts:
            print(f"   - {source}: {count} articles")
        print("\n2. ARTICLES BY CATEGORY:")
        for category, count in category_distribution:
            print(f"   - {category}: {count} articles")
        print("\n3. DAILY DISTRIBUTION:")
        for day, count in daily_distribution:
            print(f"   - {day}: {count} articles")
        print("\n4. POTENTIAL ISSUES:")
        if missing_days:
            print(f"   - Missing days (no articles): {', '.join(missing_days)}")
        else:
            print("   - No missing days")
        if days_with_few_articles:
            print("   - Days with unusually few articles:")
            for day, count in days_with_few_articles:
                print(f"     * {day}: only {count} articles")
        if articles_with_missing_content:
            print(f"\n   - Articles with missing or very short content: {len(articles_with_missing_content)}")
            for i, (id, title, url, source) in enumerate(articles_with_missing_content[:5], 1):
                print(f"     * {title} - {source} (ID: {id})")
                print(f"       URL: {url}")
            if len(articles_with_missing_content) > 5:
                print(f"       ... and {len(articles_with_missing_content) - 5} more")
        logging.info(f"Article analysis completed. Found {sum(count for _, count in source_counts)} articles in date range.")
        return {
            'source_counts': source_counts,
            'missing_days': missing_days,
            'days_with_few_articles': days_with_few_articles,
            'articles_with_missing_content': articles_with_missing_content,
            'daily_distribution': daily_distribution,
            'category_distribution': category_distribution
        }
    
    def check_coverage_quality(self, start_date=None, end_date=None):
        """
        Evaluate the quality and coverage of collected articles.
        Returns a report on potential issues and recommendations for improvement.
        """
        if not end_date:
            end_date = datetime.datetime.now(timezone.utc)
        else:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_date = end_date.replace(hour=23, minute=59, second=59)
        if not start_date:
            start_date = end_date - datetime.timedelta(days=7)
        else:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        SELECT COUNT(*) FROM articles
        WHERE publish_date >= ? AND publish_date <= ?
        ''', (start_date_str, end_date_str))
        total_articles = cursor.fetchone()[0]
        cursor.execute('''
        SELECT source, COUNT(*) as article_count
        FROM articles
        WHERE publish_date >= ? AND publish_date <= ?
        GROUP BY source
        ORDER BY article_count
        ''', (start_date_str, end_date_str))
        source_counts = cursor.fetchall()
        low_coverage_sources = []
        if source_counts:
            total_sources = len(source_counts)
            avg_per_source = total_articles / total_sources if total_sources > 0 else 0
            threshold = max(3, avg_per_source * 0.3)
            low_coverage_sources = [(source, count) for source, count in source_counts if count < threshold]
        cursor.execute('''
        SELECT COUNT(*) 
        FROM articles
        WHERE publish_date >= ? AND publish_date <= ?
        AND (content IS NULL OR LENGTH(content) < 200)
        ''', (start_date_str, end_date_str))
        short_content_count = cursor.fetchone()[0]
        conn.close()
        print("\n===== COVERAGE QUALITY REPORT =====")
        print(f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"Total articles: {total_articles}")
        if low_coverage_sources:
            print("Sources with low coverage:")
            for source, count in low_coverage_sources:
                print(f" - {source}: {count} articles")
        else:
            print("All sources have satisfactory coverage.")
        print(f"Articles with very short content (<200 characters): {short_content_count}")
        return {
            'total_articles': total_articles,
            'low_coverage_sources': low_coverage_sources,
            'short_content_count': short_content_count
        }
    
    def search_by_term(self, term):
        """Search for articles containing a specific term"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''
        INSERT OR REPLACE INTO search_terms (term, last_search)
        VALUES (?, ?)
        ''', (term, now))
        cursor.execute('''
        SELECT id, title, url, source, publish_date, summary
        FROM articles
        WHERE title LIKE ? OR content LIKE ? OR summary LIKE ?
        ORDER BY publish_date DESC
        ''', (f'%{term}%', f'%{term}%', f'%{term}%'))
        results = cursor.fetchall()
        conn.commit()
        conn.close()
        return results
    
    def get_articles_by_category(self, category):
        """Get articles by category"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        SELECT id, title, url, source, publish_date, summary
        FROM articles
        WHERE category = ?
        ORDER BY publish_date DESC
        ''', (category,))
        results = cursor.fetchall()
        conn.close()
        return results
    
    def get_recent_articles(self, limit=20):
        """Get the most recent articles"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        SELECT id, title, url, source, publish_date, summary, category
        FROM articles
        ORDER BY retrieved_date DESC
        LIMIT ?
        ''', (limit,))
        results = cursor.fetchall()
        conn.close()
        return results
        
    def get_articles_by_date_range(self, start_date, end_date):
        """Get articles published within a specific date range (using UTC)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            start_date_obj = datetime.datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_date_obj = datetime.datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_date_obj = end_date_obj.replace(hour=23, minute=59, second=59)
            start_date_str = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            end_date_str = end_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('''
            SELECT id, title, url, source, publish_date, summary, category
            FROM articles
            WHERE publish_date >= ? AND publish_date <= ?
            ORDER BY publish_date DESC
            ''', (start_date_str, end_date_str))
            results = cursor.fetchall()
            conn.close()
            return results
        except ValueError as e:
            logging.error(f"Date format error: {e}")
            conn.close()
            return []
        
    def export_to_json(self, filename='financial_news_export.json', filter_query=None, filter_params=None):
        """Export the database to a JSON file with optional filtering"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if filter_query and filter_params:
            query = f'SELECT * FROM articles WHERE {filter_query}'
            cursor.execute(query, filter_params)
        else:
            cursor.execute('SELECT * FROM articles')
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append({key: row[key] for key in row.keys()})
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=4)
        conn.close()
        logging.info(f"Exported {len(result)} articles to {filename}")
        return len(result)
        
    def export_date_range_to_json(self, start_date, end_date, filename='date_range_export.json'):
        """Export articles within a date range to JSON"""
        try:
            start_date_obj = datetime.datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_date_obj = datetime.datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_date_obj = end_date_obj.replace(hour=23, minute=59, second=59)
            start_date_str = start_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            end_date_str = end_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            filter_query = "publish_date >= ? AND publish_date <= ?"
            filter_params = (start_date_str, end_date_str)
            return self.export_to_json(filename, filter_query, filter_params)
        except ValueError as e:
            logging.error(f"Date format error: {e}")
            return 0
        
def main():
    """Main function to run the scraper"""
    try:
        scraper = FinancialNewsScraper()
        print("\n=== Financial News Scraper ===")
        print("1. Scrape all news sources (last 7 days)")
        print("2. Scrape news for specific date range")
        print("3. Search for a term")
        print("4. Get recent articles")
        print("5. Get articles by category")
        print("6. Get articles by date range")
        print("7. Export database to JSON")
        print("8. Scrape entire website (all articles)")
        print("9. Analyze articles by date range")
        print("10. Check coverage quality")
        print("11. Exit")
        while True:
            choice = input("\nEnter your choice (1-11): ")
            if choice == '1':
                print("Scraping recent news (last 7 days). This may take several minutes...")
                new_articles = scraper.scrape_by_date_range()
                print(f"Added {new_articles} new articles to the database.")
            elif choice == '2':
                print("Scrape news for specific date range (format: YYYY-MM-DD)")
                start_date = input("Enter start date: ")
                end_date = input("Enter end date: ")
                print(f"Scraping news from {start_date} to {end_date}. This may take several minutes...")
                new_articles = scraper.scrape_by_date_range(start_date, end_date)
                print(f"Added {new_articles} new articles to the database.")
            elif choice == '3':
                term = input("Enter search term: ")
                results = scraper.search_by_term(term)
                print(f"\nFound {len(results)} articles containing '{term}':")
                for i, (id, title, url, source, date, summary) in enumerate(results, 1):
                    print(f"{i}. {title} - {source} ({date})")
                    print(f"   URL: {url}")
                    print(f"   Summary: {summary[:100]}...\n")
            elif choice == '4':
                limit = input("How many recent articles to display? (default: 20): ")
                limit = int(limit) if limit.isdigit() else 20
                results = scraper.get_recent_articles(limit)
                print(f"\nMost recent {len(results)} articles:")
                for i, (id, title, url, source, date, summary, category) in enumerate(results, 1):
                    print(f"{i}. {title} - {source} ({date})")
                    print(f"   Category: {category}")
                    print(f"   URL: {url}")
                    print(f"   Summary: {summary[:100]}...\n")
            elif choice == '5':
                categories = ['stocks', 'cryptocurrency', 'economy', 'markets', 'business', 'personal_finance', 'real_estate', 'general']
                print("Available categories:")
                for i, category in enumerate(categories, 1):
                    print(f"{i}. {category}")
                cat_choice = input("Enter category number: ")
                if cat_choice.isdigit() and 1 <= int(cat_choice) <= len(categories):
                    category = categories[int(cat_choice) - 1]
                    results = scraper.get_articles_by_category(category)
                    print(f"\nFound {len(results)} articles in category '{category}':")
                    for i, (id, title, url, source, date, summary) in enumerate(results, 1):
                        print(f"{i}. {title} - {source} ({date})")
                        print(f"   URL: {url}")
                        print(f"   Summary: {summary[:100]}...\n")
                else:
                    print("Invalid category choice.")
            elif choice == '6':
                print("Get articles by date range (format: YYYY-MM-DD)")
                start_date = input("Enter start date: ")
                end_date = input("Enter end date: ")
                results = scraper.get_articles_by_date_range(start_date, end_date)
                print(f"\nFound {len(results)} articles between {start_date} and {end_date}:")
                for i, (id, title, url, source, date, summary, category) in enumerate(results, 1):
                    print(f"{i}. {title} - {source} ({date})")
                    print(f"   Category: {category}")
                    print(f"   URL: {url}")
                    print(f"   Summary: {summary[:100]}...\n")
                if results:
                    export_choice = input("Would you like to export these results to JSON? (y/n): ").lower()
                    if export_choice in ['y', 'yes']:
                        filename = input(f"Enter filename (default: {start_date}_to_{end_date}_articles.json): ")
                        if not filename:
                            filename = f"{start_date}_to_{end_date}_articles.json"
                        count = scraper.export_date_range_to_json(start_date, end_date, filename)
                        print(f"Exported {count} articles to {filename}")
            elif choice == '7':
                filename = input("Enter export filename (default: financial_news_export.json): ")
                filename = filename if filename else 'financial_news_export.json'
                count = scraper.export_to_json(filename)
                print(f"Exported {count} articles to {filename}")
            elif choice == '8':
                print("Scraping entire website (all articles without date filtering). This may take several minutes...")
                new_articles = scraper.scrape_all_articles()
                print(f"Added {new_articles} new articles to the database.")
            elif choice == '9':
                print("Analyze articles by date range")
                print("Enter the date range (format: YYYY-MM-DD)")
                start_date = input("Enter start date (leave blank for 7 days ago): ")
                end_date = input("Enter end date (leave blank for today): ")
                if not start_date and not end_date:
                    end_date = datetime.datetime.now(timezone.utc)
                    start_date = end_date - datetime.timedelta(days=7)
                    start_date = start_date.strftime('%Y-%m-%d')
                    end_date = end_date.strftime('%Y-%m-%d')
                elif not start_date:
                    end_date_obj = datetime.datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    start_date = (end_date_obj - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
                elif not end_date:
                    start_date_obj = datetime.datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    end_date = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d')
                scraper.analyze_articles_by_date_range(start_date, end_date)
            elif choice == '10':
                print("Check coverage quality")
                print("Enter the date range to analyze (format: YYYY-MM-DD)")
                start_date = input("Enter start date (leave blank for 7 days ago): ")
                end_date = input("Enter end date (leave blank for today): ")
                scraper.check_coverage_quality(start_date if start_date else None, 
                                               end_date if end_date else None)
            elif choice == '11':
                print("Exiting Financial News Scraper.")
                break
            else:
                print("Invalid choice. Please enter a number between 1 and 11.")
    except Exception as e:
        print(f"Error in main function: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Critical error: {e}")
        traceback.print_exc()
        input("Press Enter to exit...")
