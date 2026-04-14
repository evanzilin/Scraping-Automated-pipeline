"""
Example: run from repo root with PYTHONPATH including the parent of ``company_profile``::

    set PYTHONPATH=d:\\Email_Validation
    python company_profile/example_usage.py
"""

from __future__ import annotations

import logging
import os

from dotenv import load_dotenv

from company_profile import get_company_profile


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    domain = os.environ.get("EXAMPLE_DOMAIN", "stripe.com")
    row = get_company_profile(domain)
    print(row)


if __name__ == "__main__":
    main()
