"""Duplicates: pages scanned more than once, found by what they say, and a person's decision."""

from lindley.duplicates.detect import DuplicateReport, find_duplicates

__all__ = ["DuplicateReport", "find_duplicates"]
