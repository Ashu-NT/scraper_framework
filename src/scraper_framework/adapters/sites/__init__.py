from scraper_framework.adapters.registry import register
from scraper_framework.adapters.sites.books_toscrape import BooksToScrapeAdapter
from scraper_framework.adapters.sites.directory_generic import GenericDirectoryAdapter
from scraper_framework.adapters.sites.dynamic_example import DynamicExampleAdapter
from scraper_framework.adapters.sites.dynamic_test import DynamicTestAdapter
from scraper_framework.adapters.sites.test_static import ScrapeStatic


def register_all() -> None:
    register(BooksToScrapeAdapter())
    register(GenericDirectoryAdapter())
    register(DynamicExampleAdapter())
    register(DynamicTestAdapter())
    register(ScrapeStatic())
