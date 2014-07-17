#!/usr/bin/env python

import snac2.config.app as app_config
import snac2.config.db as db_config
import snac2.models as models
import os, logging, os.path

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d %I:%M:%S %p', level=logging.INFO)

def load_collection(collection_path):
    '''recursively descend and load all EACs within the given directory'''
    n = 0
    for f in os.listdir(collection_path):
        try:
            filepath = os.path.join(collection_path,f)
            if os.path.isdir(filepath):
                logging.info("Entering %s" % (filepath))
                load_collection(filepath)
            else:
                record = models.OriginalRecord.get_by_path(filepath)
                if record:
                    logging.info("%d skipped since %s exists" % (n, filepath))
                else:
                    filehandle = open(filepath, "r")
                    xml = filehandle.read()
                    record = models.OriginalRecord.load_from_eac(xml)
                    if record:
                        record.path = filepath
                        record.save()
                        models.commit()
                    if n % 1000 == 0:
                        logging.info("%s: %d completed" %(collection_path, n))
                n += 1
        except Exception, e:
            logging.warning("%s: error parsing %s %s" %(collection_path, f, e))
            raise e
            

if __name__ == "__main__":
    import sys
    db_uri = db_config.get_db_uri()
    models.init_model(db_uri)
    if len(sys.argv) < 2:
        print "usage: load.py [abbrv_of_collection]"
        sys.exit(-1)
    for c in sys.argv[1:]:
        if c in app_config.__dict__ and isinstance(app_config.__dict__[c], basestring):
            load_collection(app_config.__dict__[c])
#    load_collection(app_config.vh)
#    load_collection(app_config.nwda)
#    load_collection(app_config.loc)
#    load_collection(app_config.oac)
#    load_collection(app_config.oclc_sample_100k)
#    load_collection(app_config.oclc)
#    load_collection(app_config.bl)
