#!/usr/bin/env python

import snac2.config.app as app_config
import snac2.config.db as db_config
import snac2.models as models
import os, logging, os.path

def load_collection(collection_path):
    n = 0
    for f in os.listdir(collection_path):
        try:
            filepath = os.path.join(collection_path,f)
            if os.path.isdir(filepath):
                print "Entering %s" % (filepath)
                load_collection(filepath)
            else:
                record = models.OriginalRecord.get_by_path(filepath)
                if record:
                    filehandle = open(filepath, "r")
                    xml = filehandle.read()
                    record.record_data = xml
                    models.commit()
                    if n % 1000 == 0:
                        logging.info("%s: data load %d completed" %(collection_path, n))
                        print "%s: data load %d completed" %(collection_path, n)
                else:
                    print "Record does not exist"
                n += 1
        except Exception, e:
            raise e
            logging.warning("%s: error parsing %s %s" %(collection_path, f, e))

if __name__ == "__main__":
    db_uri = db_config.get_db_uri()
    models.init_model(db_uri)
#    load_collection(app_config.vh)
#    load_collection(app_config.nwda)
#    load_collection(app_config.loc)
#    load_collection(app_config.oac)
#    load_collection(app_config.oclc_sample_100k)
    load_collection(app_config.oclc)
    #load_collection(app_config.bl)
