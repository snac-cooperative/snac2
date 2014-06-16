#!/usr/bin/env python

import snac2.config.app as app_config
import snac2.config.db as db_config
import snac2.models as models
import os, logging, os.path, pickle

'''a collection of postprocessing functions'''

def dump_ark_ids():
    models.meta.Session.query
    
if __name__ == "__main__":
    import sys
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output_dir", help="directory to place output file")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-d", "--dump_ark_ids", action="store_true", help="create merge records; should be run before assembly")
    #group.add_argument("-a", "--assemble", action="store_true", help="assemble records into files; last step")
    args=parser.parse_args()
    db_uri = db_config.get_db_uri()
    models.init_model(db_uri)
    if args.dump_ark_ids:
        
   
