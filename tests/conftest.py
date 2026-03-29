"""
Root conftest.py — ensures the project root is on sys.path
so that 'import main', 'import classifier', etc. work from the tests/ folder.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
