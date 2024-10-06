import csv
import time
import logging
from tqdm import tqdm
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from urllib.parse import urlparse
import logging
import os

# lisr range(200,600)

class UsaTodayCrawler(CrawlSpider):
    name = 'crawlUSA3'
    allowed_domains = ['usatoday.com']
    start_urls = ['https://www.usatoday.com']
    rules = (Rule(LinkExtractor(allow=()), callback='parse_page', follow=True),)

    custom_settings = {
        'CLOSESPIDER_PAGECOUNT': 20000,  # Set a limit to 20000 pages
        'CONCURRENT_REQUESTS_PER_DOMAIN': 16,  # Limit concurrent requests to the same domain
        'DEPTH_LIMIT': 16,  # Set the depth limit to 3
        
    }

    allowed_extensions = ['.html', '.htm', '.pdf', '.doc', '.docx', '.jpg', '.jpeg', '.png', '.gif']
    allowed_file_types = ['text/html', 'application/pdf', 'application/msword', 'image/jpeg', 'image/png', 'image/gif']

    def __init__(self, *args, **kwargs):
        super(UsaTodayCrawler, self).__init__(*args, **kwargs)
        self.total_extracted = []
        self.unique_urls = set()
        self.unique_inside_urls = set()
        self.unique_outside_urls = set()
        self.status_codes = {
            '200': 0,
            '301': 0,
            '401': 0,
            '403': 0,
            '404': 0,
            '406': 0,
            '429': 0
        }
        self.file_sizes = {
            '< 1KB': 0,
            '1KB ~ <10KB': 0,
            '10KB ~ <100KB': 0,
            '100KB ~ <1MB': 0,
            '>= 1MB': 0
        }
        self.content_types = {
            'text/html': 0,
            'image/gif': 0,
            'image/jpeg': 0,
            'image/png': 0,
            'application/pdf': 0
        }

        # Define the output files
        self.fetch_file = open('fetch_USAToday_new.csv', 'w', newline='')
        self.visit_file = open('visit_USAToday_new.csv', 'w', newline='')
        self.urls_file = open('urls_USAToday_new.csv', 'w', newline='')
        self.crawl_report = open('CrawlReport_USAToday_new.txt', 'w')

        # Create CSV writers
        self.fetch_writer = csv.writer(self.fetch_file)
        self.visit_writer = csv.writer(self.visit_file)
        self.urls_writer = csv.writer(self.urls_file)

        # Write headers for the CSV files
        self.fetch_writer.writerow(['URL', 'Status'])
        self.visit_writer.writerow(['URL', 'Size (KB)', '# of Outlinks', 'Content-Type'])
        self.urls_writer.writerow(['URL', 'Location'])

        self.start_time = time.time()
        self.failed_fetches = 0
        self.pbar = tqdm(total=self.custom_settings['CLOSESPIDER_PAGECOUNT'], desc="crawling", unit='pages')

    def parse_page(self, response):
        self.pbar.update(1)
        url = response.url
        status = response.status

        logging.info(f"Fetched URL: {url} with status: {status}")
        self.fetch_writer.writerow([url, status])

        if 300 <= status < 600:
            self.failed_fetches += 1

        content_type = response.headers.get('Content-Type', b'').decode('utf-8').split(';')[0]
        file_extension = url.split('.')[-1].lower()

        logging.info(f"Processing URL: {url}")
        logging.info(f"Content-Type: {content_type}")
        logging.info(f"File Extension: {file_extension}")

        # Update status codes
        if str(status) in self.status_codes:
            self.status_codes[str(status)] += 1

        # Update file sizes
        file_size_bytes = len(response.body)
        file_size_kb = file_size_bytes / 1024  # Convert bytes to kilobytes
        if file_size_kb < 1:
            self.file_sizes['< 1KB'] += 1
        elif 1 <= file_size_kb < 10:
            self.file_sizes['1KB ~ <10KB'] += 1
        elif 10 <= file_size_kb < 100:
            self.file_sizes['10KB ~ <100KB'] += 1
        elif 100 <= file_size_kb < 1024:
            self.file_sizes['100KB ~ <1MB'] += 1
        else:
            self.file_sizes['>= 1MB'] += 1

        # Update content types
        outlinks = []
        # if any(file_type in content_type for file_type in self.allowed_file_types):
        if any(file_type in content_type for file_type in self.allowed_file_types or file_extension in self.allowed_extensions):
            self.content_types[content_type] += 1

            outlinks = response.css('*::attr(href), *::attr(src)').extract()
            num_outlinks = len(outlinks)

            self.visit_writer.writerow([url, f"{file_size_kb:.2f} KB", num_outlinks, content_type.split(';')[0]])
            for outlink in outlinks:
                parsed_outlink = self.process_outlink(outlink)
                if parsed_outlink:
                    file_extension = os.path.splitext(outlink.split('?')[0])[1].lower()  # Remove query params before extracting extension
                    if file_extension in self.allowed_file_types:
                        yield response.follow(outlink, callback=self.parse_page, errback=self.errback)
        

    def process_outlink(self, outlink):
        self.unique_urls.add(outlink)
        self.total_extracted.append(outlink)
        
        outlink_location = self.get_outlink_location(outlink)
        if outlink_location == 'OK':
            self.unique_inside_urls.add(outlink)
        else:
            self.unique_outside_urls.add(outlink)
        self.urls_writer.writerow([outlink, outlink_location])
        return outlink

    def is_valid_outlink(self, outlink):
        return outlink.startswith('http') or outlink.startswith('https')

    def get_outlink_location(self, outlink):
        for domain in self.allowed_domains:
            if domain in outlink:
                return 'OK'
        return 'N_OK'

    def errback(self, failure):
        request = failure.request
        url = request.url
        self.failed_fetches += 1
        logging.error(f"Request failed: {url}")
        self.fetch_writer.writerow([url, 'Failed'])

    def closed(self, reason):
        self.fetch_file.close()
        self.visit_file.close()
        self.urls_file.close()
        self.pbar.close()

        end_time = time.time()
        duration = end_time - self.start_time

        logging.info(f"Time taken to run the spider: {duration} seconds")
        logging.info(f"Number of failed fetches: {self.failed_fetches}")

        # Write summary to crawl report
        self.crawl_report.write(f"Name: Shubham Gore\n")
        self.crawl_report.write(f"USC ID: 4084438877\n")
        self.crawl_report.write(f"News site crawled: usatoday.com\n")
        self.crawl_report.write(f"Number of threads: \n")
        self.crawl_report.write(f"Used Python so used CONCURRENT_REQUESTS_PER_DOMAIN : 64\n\n")

        self.crawl_report.write(f"Fetch Statistics\n")
        self.crawl_report.write(f"================\n")
        self.crawl_report.write(f"# fetches attempted: {self.custom_settings['CLOSESPIDER_PAGECOUNT']}\n")
        self.crawl_report.write(f"# fetches succeeded: {self.custom_settings['CLOSESPIDER_PAGECOUNT'] - self.failed_fetches}\n")
        self.crawl_report.write(f"# fetches failed or aborted: {self.failed_fetches}(including failed and aborted)\n\n")

        self.crawl_report.write(f"Outgoing URLs:\n")
        self.crawl_report.write(f"==============\n")
        self.crawl_report.write(f"Total URLs extracted: {len(self.total_extracted)}\n")
        self.crawl_report.write(f"# unique URLs extracted: {len(self.unique_urls)}\n")
        self.crawl_report.write(f"# unique URLs within News Site: {len(self.unique_inside_urls)}\n")
        self.crawl_report.write(f"# unique URLs outside News Site: {len(self.unique_outside_urls)}\n\n")

        self.crawl_report.write(f"Status Codes:\n")
        self.crawl_report.write(f"=============\n")
        for code, count in self.status_codes.items():
            self.crawl_report.write(f"{code} {self.get_status_message(code)}: {count}\n")

        self.crawl_report.write(f"\nFile Sizes:\n")
        self.crawl_report.write(f"===========\n")
        for size_range, count in self.file_sizes.items():
            self.crawl_report.write(f"{size_range}: {count}\n")

        self.crawl_report.write(f"\nContent Types:\n")
        self.crawl_report.write(f"==============\n")
        for content_type, count in self.content_types.items():
            self.crawl_report.write(f"{content_type}: {count}\n")

        self.crawl_report.close()

    def get_status_message(self, code):
        status_messages = {
            '200': 'OK',
            '301': 'Moved Permanently',
            '401': 'Unauthorized',
            '403': 'Forbidden',
            '404': 'Not Found',
            '406': ''
        }
        return status_messages.get(code, '')
