#!/usr/bin/env python

import snac2.config.app as app_config
import snac2.config.db as db_config
import snac2.models as models
import os, logging, os.path


if __name__ == "__main__":
    db_uri = db_config.get_db_uri()
    models.init_model(db_uri)
    models.create_model()
