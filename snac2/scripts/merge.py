#!/bin/python
import sys, string, datetime, cStringIO, uuid, urllib
import logging
import xml.dom.minidom
import lxml.etree as etree
import os.path, os
import re

import snac2.config.app as app_config
import snac2.config.db as db_config
import snac2.models as models
import snac2.viaf as viaf
import snac2.noid as noid
import snac2.utils as utils
import snac2.cpf

from xml.sax.saxutils import escape

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d %I:%M:%S %p', level=logging.INFO)

def output_all_records_loop(start_at, batch_size=1000, end_at=None):
    while True:
        logging.info( "retrieving new batch..." )
        merged_records = models.MergedRecord.get_all_assigned_starting_with(id=start_at, end_at=end_at, iterate=True, limit=batch_size)
        n = write_records(merged_records)
        if n == 0:
            # merged_records returns an iterator, which is always true.  need to check if the inner for loop ran at all to see if the loop should end
            break

def output_updated_records_loop(updated_since):
    merged_records = models.MergedRecord.get_all_new_or_changed_since(imported_at = updated_since, iterate=True)
    write_records(merged_records, record_processed=False)
    
def output_updated_records_based_on_directory_loop(dir, start_at=None):
    ark_ids = []
    files = os.listdir(dir)
    if start_at:
        files = files[start_at:]
    for f in files:
        f = f[:-4] # chop fileextension
        ark_id = noid.ARK_BASE_URI + f.replace("-", "/")
        ark_ids.append(ark_id)
    #records = models.MergedRecord.get_all_by_canonical_ids(ark_ids)
    #print len(records)
    while ark_ids:
#    for record_id in ark_ids:
        record_id = ark_ids[0]
        merged_record = models.MergedRecord.get_by_canonical_id(record_id)
        write_records([merged_record], record_processed=False)
        ark_ids.pop(0) # don't pop before the write, in case something is wrong
        
    
def write_records(merged_records, record_processed=True):
    num_written = 0
    for record in merged_records:
        num_written += 1
        doc = record.to_cpf()
        if not doc:
            logging.warning("failed %d" %(record.id))
            #continue
            raise ValueError(record.id)
        if not record.canonical_id:
            raise ValueError("This record has no assigned ARK id")
        fname = record.canonical_id.split("ark:/")[1].replace("/", "-")
        full_fname = os.path.join(app_config.merged, fname+".xml")
        wf = open(full_fname,"w")
        wf.write(doc.toxml(encoding="utf-8"))
        wf.flush()
        wf.close()
        logging.info("%d: %s" %(record.id, full_fname))
        if record_processed:
            record.processed = True
            models.commit()
    return num_written

def create_merged_records(start_at=0, is_fake=True, batch_size=1000, skip_canonical_id=False):
    record_groups = models.RecordGroup.get_all_with_no_merge_record(limit=batch_size)
    for record_group in record_groups:
        merged_record = models.MergedRecord.get_by_record_group_id(record_group.id)
        if merged_record:
            continue
        merged_record = models.MergedRecord(r_type=record_group.g_type, name=record_group.name, record_data="", valid=True)
        merged_record.save()
        merged_record.record_group_id = record_group.id
        logging.info("%d: %s" %(record_group.id, merged_record.name.encode('utf-8')))
        if not skip_canonical_id:
            canonical_id = merged_record.assign_canonical_id(is_fake=is_fake)
            if canonical_id:
                logging.info("minted id for %d: %s" %(merged_record.record_group_id, canonical_id))
                models.commit()
            else:
                logging.warning("failed to mint id for %d.  skipping record creation." %(merged_record.record_group_id))
        models.commit()
    return len(record_groups)

def create_merged_records_loop(start_at, is_fake=True, batch_size=1000, total_limit=None, skip_canonical_id=False):
    n = 0
    while True:
        num_created = create_merged_records(start_at=start_at, is_fake=is_fake, batch_size=batch_size, skip_canonical_id=skip_canonical_id)
        n += num_created
        if not num_created:
            break
        if total_limit and n > total_limit:
            break
    

def output_record_by_ark(ark_id):
    record = models.MergedRecord.get_by_canonical_id(canonical_id=ark_id)
    if record:
        doc = record.to_cpf()
        if not doc:
            logging.warning("failed %d" %(record.id))
        fname = record.canonical_id.split("ark:/")[1].replace("/", "-")
        full_fname = os.path.join(app_config.merged, fname+".xml")
        wf = open(full_fname,"w")
        wf.write(doc.toxml(encoding="utf-8"))
        wf.flush()
        wf.close()
        logging.info("%d: %s" %(record.id, full_fname))

            
def reassign_merged_records(batch_size=1000, from_file=None):
    canonical_ids = []
    if from_file:
        logging.info("assigning ids from file")
        canonical_ids = open(from_file, "rb").readlines()
        canonical_ids = [noid.create_full_ark_id(id.strip()) for id in canonical_ids]
        logging.info("%d ids loaded" % (len(canonical_ids)))
    while True:
        merged_records = models.MergedRecord.get_all_unassigned(limit=batch_size)
        if len(merged_records) <= 0:
            logging.info("No merged records %d" %(len(merged_records)))
            break
        for record in merged_records:
            if not from_file:
                record.assign_canonical_id(is_fake=True)
            else:
                if len(canonical_ids) <= 0:
                    raise ValueError("ran out of canonical_ids at record %d" % (record.id))
                canonical_id = canonical_ids.pop(0)
                while canonical_ids and canonical_id:
                    existing = models.MergedRecord.get_by_canonical_id(canonical_id)
                    if not existing:
                        record.canonical_id = canonical_id
                        logging.info("%s assigned to %d" %(canonical_id, record.id))
                        break
                    else:
                        logging.info("%s already used" %(canonical_id))
                        canonical_id = canonical_ids.pop(0)
                        continue
            models.commit()
            logging.info("%d: %s" %(record.id, record.canonical_id))
    #return merged_records

if __name__ == "__main__":
    import sys
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output_dir", help="override target directory from config/app.py [not implemented yet]")
    parser.add_argument("-i", "--id", help="only assemble for this specific ARK ID")
    parser.add_argument("-s", "--starts_at", help="start the assembly at this position", default=0, type=int)
    parser.add_argument("-e", "--ends_at", help="end the assembly at this position", type=int)
    parser.add_argument("-r", "--real", action="store_true", help="request real ARK IDs; do not use this unless in production mode")
    parser.add_argument("-n", "--no_id", action="store_true", help="do not assign canonical ids")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-m", "--merge", action="store_true", help="create merge records; should be run before assembly")
    group.add_argument("-a", "--assemble", action="store_true", help="assemble records into files; last step")
    #group.add_argument("-u", "--assemble_update", help="assemble new and updated records since YYYY-MM-DD")
    group.add_argument("-f", "--reassign_id_from_file",  help="reassign ids from given file")
    args=parser.parse_args()
    db_uri = db_config.get_db_uri()
    models.init_model(db_uri)
    if args.merge:
        create_merged_records_loop(args.starts_at, is_fake=not args.real, skip_canonical_id=args.no_id)
    elif args.assemble:
        if args.id:
            output_record_by_ark(args.id)
        else:
            logging.info( args.starts_at )
            output_all_records_loop(args.starts_at, batch_size=100, end_at=args.ends_at)
    elif args.reassign_id_from_file:
        reassign_merged_records(from_file=args.reassign_id_from_file)
 
