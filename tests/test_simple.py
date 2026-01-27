#!/usr/bin/env python3
"""Minimal test for Pydantic validation."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from scraper_framework.config_models import ScraperConfig
    print("✅ Import successful!")

    # Test CSV config
    csv_config_data = {
        'job': {
            'id': 'test_csv',
            'name': 'Test CSV Job',
            'adapter': 'test_adapter',
            'start_url': 'https://example.com'
        },
        'sink': {
            'type': 'csv',
            'path': 'test.csv'
        }
    }

    config = ScraperConfig(**csv_config_data)
    print("✅ CSV config validation successful!")
    print(f"   Job: {config.job.name}")
    print(f"   Sink type: {config.sink.type}")
    print(f"   Sink path: {config.sink.path}")

    # Test Google Sheets config
    gs_config_data = {
        'job': {
            'id': 'test_gs',
            'name': 'Test Google Sheets Job',
            'adapter': 'test_adapter',
            'start_url': 'https://example.com'
        },
        'sink': {
            'type': 'google_sheets',
            'sheet_id': '123456',
            'tab': 'Sheet1'
        }
    }

    gs_config = ScraperConfig(**gs_config_data)
    print("✅ Google Sheets config validation successful!")
    print(f"   Job: {gs_config.job.name}")
    print(f"   Sink type: {gs_config.sink.type}")
    print(f"   Sheet ID: {gs_config.sink.sheet_id}")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()