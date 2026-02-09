from scraper_framework.adapters.registry import register
from scraper_framework.adapters.sites.books_toscrape import BooksToScrapeAdapter
from scraper_framework.adapters.sites.directory_generic import GenericDirectoryAdapter

def register_all() -> None:
    register(BooksToScrapeAdapter())
    register(GenericDirectoryAdapter())
