import unittest

from src.scraper_framework.core.models import Page, RequestSpec
from src.scraper_framework.parse.parsers import HtmlPageParser
from src.scraper_framework.adapters.sites.directory_generic import GenericDirectoryAdapter
from src.scraper_framework.parse.cards import HtmlCard


HTML = """
<html>
  <body>
    <div class="listing">
      <h3 class="name"><a href="/biz/1">Acme Plumbing</a></h3>
      <div class="address">Berlin</div>
      <a class="website" href="https://acme.example">Website</a>
      <a href="tel:+491234">Call</a>
      <span data-rating="4.6"></span>
      <span data-reviews="123"></span>
    </div>

    <div class="listing">
      <h3 class="name"><a href="/biz/2">Bravo Electric</a></h3>
      <div class="address">Munich</div>
    </div>
  </body>
</html>
"""


class TestHtmlParserAndAdapter(unittest.TestCase):
    def test_parser_finds_cards(self):
        adapter = GenericDirectoryAdapter()
        parser = HtmlPageParser()
        page = Page(url="https://example.com/search", status_code=200, content_type="text/html", raw=HTML)

        cards = parser.parse_cards(page, adapter)
        self.assertEqual(len(cards), 2)

    def test_adapter_extracts_fields(self):
        adapter = GenericDirectoryAdapter()
        page = Page(url="https://example.com/search", status_code=200, content_type="text/html", raw=HTML)

        # Use parser to get the first card
        parser = HtmlPageParser()
        cards = parser.parse_cards(page, adapter)
        card = cards[0]

        source_url = adapter.extract_source_url(card, page)
        self.assertEqual(source_url, "https://example.com/biz/1")

        self.assertEqual(adapter.extract_field(card, "name", page), "Acme Plumbing")
        self.assertEqual(adapter.extract_field(card, "address", page), "Berlin")
        self.assertEqual(adapter.extract_field(card, "website", page), "https://acme.example")
        self.assertEqual(adapter.extract_field(card, "phone", page), "+491234")
        self.assertEqual(adapter.extract_field(card, "rating", page), "4.6")
        self.assertEqual(adapter.extract_field(card, "reviews", page), "123")

    def test_next_request_none_when_no_pagination(self):
        adapter = GenericDirectoryAdapter()
        page = Page(url="https://example.com/search", status_code=200, content_type="text/html", raw=HTML)

        current = RequestSpec(url=page.url)
        nxt = adapter.next_request(page, current)
        self.assertIsNone(nxt)
