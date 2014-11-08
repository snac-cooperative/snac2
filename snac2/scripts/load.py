#!/usr/bin/env python

import snac2.config.app as app_config
import snac2.config.db as db_config
import snac2.models as models
import os, logging, os.path, re

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d %I:%M:%S %p', level=logging.INFO)

def load_collection(collection_path, collection_name=''):
    '''recursively descend and load all EACs within the given directory'''
    n = 0
    fail_n = 0
    for f in os.listdir(collection_path):
        try:
            filepath = os.path.join(collection_path,f)
            if os.path.isdir(filepath):
                logging.info("Entering %s" % (filepath))
                load_collection(filepath, collection_name=collection_name)
            else:
                record = models.OriginalRecord.get_by_path(filepath)
                if record:
                    logging.info("%d skipped since %s exists" % (n, filepath))
                else:
                    filehandle = open(filepath, "r")
                    xml = filehandle.read()
                    record = models.OriginalRecord.load_from_eac(xml)
                    if record and collection_name == "oac":
                        if is_oac_malformed_name(record.name):
                            logging.warning("record is OAC and has mangled name %s" % (record.name))
                            record = None # do nothing and skip record
                    elif is_super_long(record.name, filepath):
                        record = None
                    if record:
                        record.path = filepath
                        record.save()
                        models.commit()
                    else:
                        logging.warning("WARNING: failed to load %s" % (collection_path))
                        fail_n += 1
                    if n and n % 1000 == 0:
                        logging.info("%s: %d completed" %(collection_path, n))
                n += 1
        except Exception, e:
            logging.warning("%s: error parsing %s %s" %(collection_path, f, e))
            raise e
    logging.info("%d total loaded in %s", n, collection_path)
    if fail_n > 0:
        logging.info("%d failed in %s", fail_n, collection_path)

OAC_MALFORMED_NAME_PAT = re.compile(".*;.*;")
def is_oac_malformed_name(name):
    if name and len(name) >= 30 and OAC_MALFORMED_NAME_PAT.match(name):
        return True
    return False
    
def is_super_long(name, filepath):
    if name and len(name) >= 500:
        logging.warning("%s | %s is probably invalid", name, filepath)
        return True
    elif name and len(name) >= 300:
        logging.warning("%s | %s is way too long and probably not a real name", name, filepath)
        return True
    elif name and len(name) >= 200:
        logging.warning("%s | %s is long and may not be a real name", name, filepath)
    return False
    
if __name__ == "__main__":
    import sys
    db_uri = db_config.get_db_uri()
    models.init_model(db_uri)
    if len(sys.argv) < 2:
        print "usage: load.py [abbrv_of_collection]"
        sys.exit(-1)
    for c in sys.argv[1:]:
        if c in app_config.__dict__ and isinstance(app_config.__dict__[c], basestring):
            load_collection(app_config.__dict__[c], collection_name=c)
#    load_collection(app_config.vh)
#    load_collection(app_config.nwda)
#    load_collection(app_config.loc)
#    load_collection(app_config.oac)
#    load_collection(app_config.oclc_sample_100k)
#    load_collection(app_config.oclc)
#    load_collection(app_config.bl)
