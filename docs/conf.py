# -*- coding: utf-8 -*-
import alabaster
from sprockets.clients import dynamodb

project = 'sprockets.clients.dynamodb'
copyright = 'AWeber Communications, Inc.'
version = dynamodb.__version__
release = '.'.join(str(v) for v in dynamodb.version_info[0:2])

needs_sphinx = '1.0'
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.viewcode',
]

templates_path = []
source_suffix = '.rst'
source_encoding = 'utf-8-sig'
master_doc = 'index'
exclude_patterns = []
pygments_style = 'sphinx'
html_theme = 'alabaster'
html_style = 'custom.css'
html_static_path = ['static']
html_theme_path = [alabaster.get_path()]
html_sidebars = {
    '**': ['about.html', 'navigation.html'],
}
html_theme_options = {
    'github_user': 'sprockets',
    'github_repo': 'sprockets.clients.dynamodb',
    'description': 'Amazon DynamoDB Connector',
    'github_banner': True,
    'travis_button': True,
    'codecov_button': True,
    'sidebar_width': '230px',
}

intersphinx_mapping = {
    'python': ('http://docs.python.org/3/', None),
    'tornado': ('http://tornadoweb.org/en/latest/', None),
    'tornadoaws': ('https://tornado-aws.readthedocs.org/en/latest/', None),
}
