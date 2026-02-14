#!/usr/bin/env python3
"""
Test script to demonstrate Pydantic configuration validation.
Run this to see validation in action with various invalid configs.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from scraper_framework.config_models import load_and_validate_config

def test_valid_config():
    """Test that valid configs pass validation."""
    print("Testing valid configuration...")
    try:
        config = load_and_validate_config('configs/jobs/example_static.yaml')
        print("Valid config loaded successfully!")
        print(f"   Job: {config.job.name}")
        print(f"   Sink: {config.sink.type}")
        return True
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def test_invalid_configs():
    """Test various invalid configurations."""
    import tempfile
    import yaml

    test_cases = [
        {
            "name": "Missing required field",
            "config": {
                "job": {
                    "name": "Test Job",
                    "adapter": "test",
                    "start_url": "https://example.com"
                    # Missing 'id'
                },
                "sink": {"type": "csv", "path": "test.csv"}
            }
        },
        {
            "name": "Invalid dedupe mode",
            "config": {
                "job": {
                    "id": "test",
                    "name": "Test Job",
                    "adapter": "test",
                    "start_url": "https://example.com",
                    "dedupe_mode": "INVALID_MODE"
                },
                "sink": {"type": "csv", "path": "test.csv"}
            }
        },
        {
            "name": "Invalid URL",
            "config": {
                "job": {
                    "id": "test",
                    "name": "Test Job",
                    "adapter": "test",
                    "start_url": "not-a-url"
                },
                "sink": {"type": "csv", "path": "test.csv"}
            }
        },
        {
            "name": "Enrich fields not in schema",
            "config": {
                "job": {
                    "id": "test",
                    "name": "Test Job",
                    "adapter": "test",
                    "start_url": "https://example.com",
                    "field_schema": ["name"]
                },
                "sink": {"type": "csv", "path": "test.csv"},
                "enrich": {
                    "enabled": True,
                    "fields": ["phone", "website"]  # Not in field_schema
                }
            }
        }
    ]

    print("\nTesting invalid configurations...")

    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}. {test_case['name']}")

        # Create temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(test_case['config'], f)
            temp_path = f.name

        try:
            load_and_validate_config(temp_path)
            print("Should have failed validation!")
        except ValueError as e:
            print("Correctly caught validation error:")
            print(f"   {str(e).split(':', 1)[1].strip()}")
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            os.unlink(temp_path)

if __name__ == "__main__":
    print("Configuration Validation Tests\n")

    success = test_valid_config()
    test_invalid_configs()

    if success:
        print("\nAll tests completed!")
    else:
        print("\nSome tests failed!")
        sys.exit(1)