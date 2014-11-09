#!/usr/bin/env python

import IPython
import snac2.config.db
import snac2.config.app as app_config
#import arkplatform2.models_sql as model
from snac2.models import *
import snac2.viaf as viaf
import sys
from IPython.frontend.terminal.embed import InteractiveShellEmbed

if __name__ == "__main__":
    banner1="=== Welcome to SNAC Merge Tool Shell ==="
    banner2="using database %s\n" % snac2.config.db.get_db_uri()
    init_model(snac2.config.db.get_db_uri())
    viaf.config_cheshire(db=app_config.VIAF_INDEX_NAME)
    shell = InteractiveShellEmbed(banner1=banner1, banner2=banner2)
    shell.user_ns = {}
    shell()

