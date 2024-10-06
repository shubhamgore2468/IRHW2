from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from urllib.parse import urlparse
import csv
import time
import logging
from tqdm import tqdm

class UsaTodayCrawler(CrawlSpider):
    name = 'crawlUSA2'
    allowed_domains = ['www.usatoday.com', 'usatoday.com']
    start_urls = ['https://www.usatoday.com']
    rules = (Rule(LinkExtractor(allow=()), callback='parse_page', follow=True),)

    custom_settings = {
        'CLOSESPIDER_PAGECOUNT': 20000,  # Set a limit to 200 pages
        'CONCURRENT_REQUESTS_PER_DOMAIN': 64,  # Limit concurrent requests to the same domain
        'DEPTH_LIMIT': 16,
    }

    allowed_extensions = ['html', 'doc', 'docx', 'pdf', 'jpg', 'jpeg', 'png', 'gif', 'bmp']

    content_type_mapping = {
        'application/msword': 'doc',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
        'application/pdf': 'pdf',
        'image/jpeg': 'jpg',
        'image/png': 'png',
        'image/gif': 'gif',
        'image/bmp': 'bmp',
    }

    allowed_mime_types = [
        'text/html',
        'application/msword',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'application/pdf',
        'image/jpeg',
        'image/png',
        'image/gif',
        'image/bmp'
    ]

    def __init__(self, *args, **kwargs):
        super(UsaTodayCrawler, self).__init__(*args, **kwargs)

        # Define the output files
        self.fetch_file = open('fetch_USAToday.csv', 'w', newline='')
        self.visit_file = open('visit_USAToday.csv', 'w', newline='')
        self.urls_file = open('urls_USAToday.csv', 'w', newline='')

        # Create CSV writers
        self.fetch_writer = csv.writer(self.fetch_file)
        self.visit_writer = csv.writer(self.visit_file)
        self.urls_writer = csv.writer(self.urls_file)

        # Write headers for the CSV files
        self.fetch_writer.writerow(['URL', 'Status'])
        self.visit_writer.writerow(['URL', 'Size (Bytes)', '# of Outlinks', 'Content-Type'])
        self.urls_writer.writerow(['URL', 'Location'])

        self.start_time = time.time()
        self.failed_fetches = 0
        self.pbar = tqdm(total=200, desc="crawling", unit='pages')

    def add_errback(self, request, response, spider):
        """Attach the errback method to every request"""
        request.errback = self.errback
        return request

    def parse_page(self, response):
        url = response.url
        status = response.status

        logging.info(f"Fetched URL: {url} with status: {status}")
        self.fetch_writer.writerow([url, status])

        if 300 <= status < 600:
            self.failed_fetches += 1

        content_type = response.headers.get('Content-Type', b'').decode('utf-8')
        logging.info(f"Processing URL: {url}")
        logging.info(f"Content-Type: {content_type}")

        if any(mime_type in content_type for mime_type in self.allowed_mime_types):
            file_size = len(response.body)
            outlinks = response.css('*::attr(href)').extract()
            num_outlinks = len(outlinks)

            self.visit_writer.writerow([url, file_size, num_outlinks, content_type.split(';')[0]])

            parsed_url = urlparse(url)
            if any(parsed_url.netloc == domain for domain in self.allowed_domains):
                location = 'OK'
            else:
                location = 'N_OK'

            self.urls_writer.writerow([url, location])

            for outlink in outlinks:
                if outlink.startswith('http') or outlink.startswith('https'):
                    parsed_outlink = urlparse(outlink)
                    if any(parsed_outlink.netloc == domain for domain in self.allowed_domains):
                        outlink_location = 'OK'
                    else:
                        outlink_location = 'N_OK'
                    self.urls_writer.writerow([outlink, outlink_location])

                    logging.info(f"Following outlink: {outlink}")
                    yield response.follow(outlink, callback=self.parse_page, errback=self.errback)

        self.pbar.update(1)

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
        print(f"Number of failed fetches: {self.failed_fetches}")