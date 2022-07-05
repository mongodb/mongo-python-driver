# -*- coding: utf-8 -*-
#
# PyMongo documentation build configuration file
#
# This file is execfile()d with the current directory set to its containing dir.

import os
import sys

sys.path[0:0] = [os.path.abspath("..")]

import pymongo  # noqa

# -- General configuration -----------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.coverage",
    "sphinx.ext.todo",
    "sphinx.ext.intersphinx",
]


# Add optional extensions
try:
    import sphinxcontrib.shellcheck  # noqa

    extensions += ["sphinxcontrib.shellcheck"]
except ImportError:
    pass


# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The suffix of source filenames.
source_suffix = ".rst"

# The master toctree document.
master_doc = "index"

# General information about the project.
project = "PyMongo"
copyright = "MongoDB, Inc. 2008-present. MongoDB, Mongo, and the leaf logo are registered trademarks of MongoDB, Inc"
html_show_sphinx = False

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = pymongo.version
# The full version, including alpha/beta/rc tags.
release = pymongo.version

# List of documents that shouldn't be included in the build.
unused_docs = []

# List of directories, relative to source directory, that shouldn't be searched
# for source files.
exclude_trees = ["_build"]

# The reST default role (used for this markup: `text`) to use for all documents.
# default_role = None

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
# show_authors = False

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
add_module_names = True

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

# A list of ignored prefixes for module index sorting.
# modindex_common_prefix = []

# Options for link checking
# The anchors on the rendered markdown page are created after the fact,
# so those link results in a 404.
# wiki.centos.org has been flakey.
# sourceforge.net is giving a 403 error, but is still accessible from the browser.
linkcheck_ignore = [
    "https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.rst#requesting-an-immediate-check",
    "https://github.com/mongodb/libmongocrypt/blob/master/bindings/python/README.rst#installing-from-source",
    r"https://wiki.centos.org/[\w/]*",
    r"http://sourceforge.net/",
]

# -- Options for extensions ----------------------------------------------------
autoclass_content = "init"

doctest_path = [os.path.abspath("..")]

doctest_test_doctest_blocks = ""

doctest_global_setup = """
from pymongo.mongo_client import MongoClient
client = MongoClient()
client.drop_database("doctest_test")
db = client.doctest_test
"""

# -- Options for HTML output ---------------------------------------------------

# Theme gratefully vendored from CPython source.
html_theme = "pydoctheme"
html_theme_path = ["."]
html_theme_options = {"collapsiblesidebar": True, "googletag": False}

# Additional static files.
html_static_path = ["static"]

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
# html_title = None

# A shorter title for the navigation bar.  Default is the same as html_title.
# html_short_title = None

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
# html_logo = None

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
# html_favicon = None

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']

# Custom sidebar templates, maps document names to template names.
# html_sidebars = {}

# Additional templates that should be rendered to pages, maps page names to
# template names.
# html_additional_pages = {}

# If true, links to the reST sources are added to the pages.
# html_show_sourcelink = True

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
# html_use_opensearch = ''

# If nonempty, this is the file name suffix for HTML files (e.g. ".xhtml").
# html_file_suffix = ''

# Output file base name for HTML help builder.
htmlhelp_basename = "PyMongo" + release.replace(".", "_")


# -- Options for LaTeX output --------------------------------------------------

# The paper size ('letter' or 'a4').
# latex_paper_size = 'letter'

# The font size ('10pt', '11pt' or '12pt').
# latex_font_size = '10pt'

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, documentclass [howto/manual]).
latex_documents = [
    ("index", "PyMongo.tex", "PyMongo Documentation", "Michael Dirolf", "manual"),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
# latex_logo = None

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
# latex_use_parts = False

# Additional stuff for the LaTeX preamble.
# latex_preamble = ''

# Documents to append as an appendix to all manuals.
# latex_appendices = []

# If false, no module index is generated.
# latex_use_modindex = True


intersphinx_mapping = {
    "gevent": ("http://www.gevent.org/", None),
    "py": ("https://docs.python.org/3/", None),
}
