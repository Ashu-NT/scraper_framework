import unittest

from src.scraper_framework.core.models import Record
from src.scraper_framework.transform.normalizers import DefaultNormalizer


class TestDefaultNormalizer(unittest.TestCase):
    def setUp(self):
        self.n = DefaultNormalizer()

    def test_parse_rating_numeric(self):
        r = Record(id="1", source_url="u", scraped_at_utc="t", fields={"rating": "4.7"})
        out = self.n.normalize(r)
        self.assertAlmostEqual(out.fields["rating"], 4.7)

    def test_parse_rating_comma(self):
        r = Record(id="1", source_url="u", scraped_at_utc="t", fields={"rating": "4,7"})
        out = self.n.normalize(r)
        self.assertAlmostEqual(out.fields["rating"], 4.7)

    def test_parse_rating_stars(self):
        r = Record(id="1", source_url="u", scraped_at_utc="t", fields={"rating": "★★★★★"})
        out = self.n.normalize(r)
        self.assertAlmostEqual(out.fields["rating"], 5.0)

    def test_parse_reviews_int(self):
        r = Record(id="1", source_url="u", scraped_at_utc="t", fields={"reviews": "(1,234) reviews"})
        out = self.n.normalize(r)
        self.assertEqual(out.fields["reviews"], 1234)

    def test_clean_url(self):
        r = Record(id="1", source_url="u", scraped_at_utc="t", fields={"website": "  https://example.com  "})
        out = self.n.normalize(r)
        self.assertEqual(out.fields["website"], "https://example.com")
