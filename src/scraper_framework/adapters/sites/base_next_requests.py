from typing import Optional

from scraper_framework.core.models import Page, RequestSpec

# Click-based pagination adapter example selenium


def next_request(self, page: Page, current: RequestSpec) -> Optional[RequestSpec]:
    params = dict(current.params or {})

    max_click_pages = int(params.get("click_max_pages", 25))
    cursor = int(params.get("click_cursor", 0))

    stall_limit = int(params.get("click_stall_limit", 3))
    stall = int(params.get("click_stall_count", 0))

    if cursor >= max_click_pages:
        self.log.info("Click stop: reached click_max_pages=%d", max_click_pages)
        return None

    cards = getattr(page, "_cards_cache", [])
    hrefs = set()

    for c in cards:
        u = self.extract_source_url(c, page)
        if u:
            hrefs.add(u)

    seen_total = set(params.get("click_seen_hrefs", []))
    before = len(seen_total)
    seen_total.update(hrefs)
    after = len(seen_total)
    gained = after - before

    if gained == 0:
        stall += 1
    else:
        stall = 0

    if stall >= stall_limit:
        self.log.info("Click stop: cursor=%d unique_total=%d stall=%d/%d", cursor, after, stall, stall_limit)
        return None

    self.log.info("Click progress: cursor=%d unique_total=%d (+%d) stall=%d/%d", cursor, after, gained, stall, stall_limit)

    params.update(
        {
            "click_action": "once",
            "click_selector": params.get("click_selector") or "button.load-more",
            "click_cursor": cursor + 1,
            "click_stall_count": stall,
            "click_seen_hrefs": list(seen_total),
        }
    )

    return RequestSpec(
        url=current.url,
        headers=current.headers,
        params=params,
        method="GET",
        body=None,
    )
