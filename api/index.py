#!/usr/bin/env python3
"""Vercel Python runtime entrypoint."""

from app import AppHandler, ensure_bootstrap


ensure_bootstrap()


class handler(AppHandler):
    """Compatibility handler expected by Vercel's Python runtime."""

