#!/usr/bin/env python3
"""
Run email verification and duplicate check via ``check_email_dup``.

Configure workbook path and options in ``check_email_dup.py`` (``XLSX_PATH``, sheet, etc.).

For company enrichment by domain, run ``enrichment.py`` / ``python -m enrichment`` separately
or call ``enrichment.run_for_domain`` from your own script.
"""

from __future__ import annotations

import check_email_dup as dup


def main() -> None:
    dup.main()


if __name__ == "__main__":
    main()
