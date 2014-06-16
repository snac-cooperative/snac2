from setuptools import setup, find_packages
setup(
    name = "snac2",
    version = "0.2",
    packages = find_packages(),
    scripts = [],

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    # NOTE: this also requires CheshirePy from the cheshire source tree - yliu
    install_requires = ['SQLAlchemy', 'python-dateutil==1.5', 'nameparser', 'jellyfish', 'IPython==0.13.1', 'lxml', 'argparse', 'pytz', 'psycopg2'],

#    package_data = {
        # If any package contains *.txt or *.rst files, include them:
#        '': ['*.txt', '*.rst'],
        # And include any *.msg files found in the 'hello' package, too:
#        'hello': ['*.msg'],
#    }

    # metadata for upload to PyPI
    author = "Yiming Liu",
    author_email = "yliu@ischool.berkeley.edu",
    description = "SNAC merge tool",
    license = "BSD",
    keywords = "SNAC",
    url = "http://ischool.berkeley.edu",   # project home page, if any

    # could also include long_description, download_url, classifiers, etc.
)
