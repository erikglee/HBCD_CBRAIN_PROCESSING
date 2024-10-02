# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'HBCD_CBRAIN_PROCESSING'
copyright = '2024, HBCD'
author = 'HBCD'
release = '1.0.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinxarg.ext'
]

templates_path = ['_templates']
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

# If your main document is not index.rst, uncomment and specify it
# master_doc = 'index'

import os
import sys
import subprocess
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('../../.'))
#import grab_cbrain_tool_details

autodoc_mock_imports = ["nibabel", "matplotlib", "numpy", "ants", "pandas", "scipy"]

def setup(app):
    # This will run 'generate_docs.py' before the documentation is built
    subprocess.call(["python", "grab_cbrain_tool_details.py"])