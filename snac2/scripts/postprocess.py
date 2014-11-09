#!/usr/bin/env python

import snac2.config.app as app_config
import snac2.config.db as db_config
import snac2.models as models
import snac2.viaf as viaf
import snac2.utils as utils
import snac2.scripts.match as match

import os, logging, os.path, pickle, csv

'''a collection of postprocessing functions'''

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d %I:%M:%S %p', level=logging.INFO)

def reload_viaf(record_group):
    if record_group.viaf_id:
        viaf_record = viaf.query_cheshire_viaf_id(record_group.viaf_id)
        if viaf_record:
            x = viaf_record.decode('utf-8')
            y = record_group.viaf_record
            if x != y:
#                 logging.info("old version")
#                 logging.info(record_group.viaf_record)
#                 logging.info("new version")
#                 logging.info(viaf_record)
                record_group.viaf_record = viaf_record
                models.commit()
                logging.info("replaced viaf_id %s on record_group %d with new version" % (record_group.viaf_id, record_group.id))
            else:
                logging.info("no change to viaf_id %s on record_group %d" % (record_group.viaf_id, record_group.id))
        else:
            viaf_data = viaf.getEntityInformation(record_group.viaf_record)
            sources = viaf_data["sources"]
            sources = filter(None, sources)
            if sources:
                viaf_record = None
                for source in sources:
                    viaf_record  = viaf.query_cheshire_viaf_id(source.replace("|", " "), index="idnumber")
                    if viaf_record:
                        # check to see if we already have another record_group with this viaf_id
                        replacement_viaf_id = viaf.getEntityInformation(viaf_record)["recordId"]
                        existing_group = models.RecordGroup.get_by_viaf_id(replacement_viaf_id)
                        if existing_group:
                            for record in record_group.records:
                                record.record_group_id = existing_group.id
                            for candidate in record_group.maybe_candidates:
                                candidate.record_group_id = existing_group.id
                            record_group.g_type = "invalid"
                            logging.info("merged record_group %d into record_group %d because viaf %s -> viaf %s" % (record_group.id, existing_group.id, record_group.viaf_id, replacement_viaf_id))
                        else:
                            old_viaf_id = record_group.viaf_id
                            record_group.viaf_record = viaf_record
                            record_group.viaf_id = replacement_viaf_id
                            logging.info("replaced old viaf_id %s on record_group %d with new version %s using source %s" % (old_viaf_id, record_group.id, record_group.viaf_id, source))
                        models.commit()
                        break
                if not viaf_record:
                    logging.info("no viaf4 replacement found after full cycle for viaf_id %s on record_group %d" % (record_group.viaf_id, record_group.id))
            else:
                logging.info("no viaf4 replacement found for viaf_id %s on record_group" % (record_group.viaf_id, record_group.id))

    else:
        logging.info("no viaf_id for group %d" % (record_group.id))

def identify_problematics(record_group, output_path="/projects/snac-output/problematics.csv"):
    viaf_record = viaf.getEntityInformation(record_group.viaf_record)
    if viaf_record['mainHeadings']:
        headings = viaf_record['mainHeadings']
        with open(output_path, 'a+b') as outfile:
            writer = csv.writer(outfile)
            for record in record_group.records:
                x = utils.strip_accents(record.name)
                matches = 0
                for y in headings:
                    y = utils.strip_accents(y)
                    name_quality = match.compute_name_match_quality(x, y)
                    if name_quality >= match.STR_FUZZY_MATCH_THRESHOLD:
                        matches += 1
                        break
                if not matches:
                    logging.info("problematic id %d: %s for %s at %0.4f" % (record.id, x.encode('utf-8'), y.encode('utf-8'), name_quality))
                    writer.writerow((record.id, record_group.id, x.encode('utf-8'), y.encode('utf-8'), name_quality))
            

def postprocess_loop(process_func, batch_size=100, name_type='person', start_at_id=0, end_at_id=None, exclude_none_viaf=True):
    viaf.config_cheshire();
    unprocessed_records = True
    limit = batch_size
    offset = 0
    while unprocessed_records:
        #record_groups = models.RecordGroup.get_all_by_type(name_type=name_type, limit=limit, offset=offset, start_at_id=start_at_id, end_at_id=end_at_id, exclude_non_viaf=exclude_none_viaf)
        record_groups = models.meta.Session.query(models.RecordGroup).filter(models.RecordGroup.g_type=="invalid").all()
        if not record_groups:
            unprocessed_records = False
        else:
            for record_group in record_groups:
                logging.info("processing %d..." % (record_group.id))
                process_func(record_group)
            logging.info("retrieving next batch")
        offset = offset + len(record_groups)
        #models.commit()
        
    
if __name__ == "__main__":
    import sys
    db_uri = db_config.get_db_uri()
    models.init_model(db_uri)
    viaf.config_cheshire(db=app_config.VIAF_INDEX_NAME)
    start_at_id = 0
    end_at_id = None
    if len(sys.argv) > 2:
        start_at_id = int(sys.argv[2])
    if len(sys.argv) > 3:
        end_at_id = int(sys.argv[3])
    if sys.argv[1] == "reload":
        postprocess_loop(reload_viaf, start_at_id=start_at_id)
    elif sys.argv[1] == "problems":
        postprocess_loop(identify_problematics, start_at_id=start_at_id, end_at_id=end_at_id)
        
   
