#!/usr/bin/env python

import snac2.config.app as app_config
import snac2.config.db as db_config
import snac2.models as models
import snac2.viaf as viaf
import snac2.utils as utils
import dateutil.parser, datetime
import os, logging, re
import nameparser

STR_FUZZY_MATCH_THRESHOLD  = 0.85
ACCEPT_THRESHOLD = 0.9
POSTGRES_NGRAM_THRESHOLD = 0.75

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d %I:%M:%S %p', level=logging.INFO)

def match_persons_loop():
    unprocessed_records = True
    while unprocessed_records:
        records = match_persons(100)
        if not records:
            unprocessed_records = False
        else:
            logging.info("retrieving next batch...")

def match_corporate_loop():
    unprocessed_records = True
    while unprocessed_records:
        records = match_corporate(100)
        if not records:
            unprocessed_records = False
        else:
            logging.info("retrieving next batch...")
            
def match_families_loop():
    unprocessed_records = True
    while unprocessed_records:
        records = match_families(100)
        if not records:
            unprocessed_records = False
        else:
            logging.info("retrieving next batch...")

def match_persons(batch_size=None):
    records = models.PersonOriginalRecord.get_all_unprocessed_records(limit=batch_size)
    for record in records:
        viaf_id = None
        viaf_record = None
        match_quality = 1
        candidate = None
        maybe_viaf_id = None
        maybe_postgres_id = None
        
        # exact match against in-db records
        record_group, viaf_id, viaf_record, match_quality = match_person_exact(record)
            
        # ngram
        if not record_group and not viaf_id:
            record_group, viaf_id, viaf_record, match_quality = match_person_ngram_viaf(record)
            if not match_quality:
                viaf_id = None
                viaf_record = None
                record_group = None
            elif match_quality > 0:
                #  there was a viaf match
                if match_quality <= ACCEPT_THRESHOLD:
                    maybe_viaf_id = viaf_id
                    viaf_id = None
                    viaf_record = None
                    record_group = None
                    logging.info("maybe: %s" % (maybe_viaf_id))
                else:
                    logging.info("accepted: %s" % (viaf_id))
            elif match_quality == -1:
                #no viaf match at all
                candidate, match_quality = match_person_ngram_postgres(record)
                if not match_quality:
                    pass
                elif match_quality == -1:
                    pass
                else:# match_quality <= ACCEPT_THRESHOLD:
                    # force all maybes for safety
                    maybe_postgres_id = str(candidate.record_group.id)
                    viaf_id = None
                    viaf_record = None
                    record_group = None
                    logging.info("maybe postgres: %s" % (maybe_postgres_id))
                # else:
#                     record_group = candidate.record_group
#                     logging.info("accepted postgres: %d" % (candidate.record_group.id))

        
        # truly a new record, create and add
        
        if not record_group:
            record_group = models.PersonGroup(name=record.name_norm)
            record_group.save()
            models.flush()
            logging.info("Creating new group for %d %s" % (record.id, record.name_norm))

        if record.id not in [r.id for r in record_group.records]:
            record = models.PersonOriginalRecord.get_by_id(record.id)
            record.record_group_id = record_group.id
            
            logging.info("Adding %d %s to group %d" % (record.id, record.name_norm, record_group.id))
        else:
            logging.info("Duplicate attempt to add %d %s to group %d" % (record.id, record.name_norm, record_group.id))
        if not record_group.viaf_record:
            if viaf_id:
                record_group.viaf_id = viaf_id
                record_group.viaf_record = viaf_record
        else:
            logging.info("Already a viaf for %d %s; not attempting again" % (record.id, record.name_norm))
        if maybe_viaf_id:
            candidate = models.MaybeCandidate(candidate_type="viaf", candidate_id=maybe_viaf_id, record_group_id=record_group.id)
            candidate.save()
            logging.info("Set %s as maybe for record_group %d" % (maybe_viaf_id, record_group.id))
        if maybe_postgres_id:
            candidate = models.MaybeCandidate(candidate_type="postgres", candidate_id=maybe_postgres_id, record_group_id=record_group.id)
            candidate.save()
            logging.info("Set %s as maybe for record_group %d" % (maybe_postgres_id, record_group.id))
        record.processed = True
        models.commit()
    return len(records)
        
DATE_NO_DATA = -1
DATE_MATCH_FAIL = 0
DATE_MATCH = 1
DATE_MATCH_WITHIN_MARGIN = 2
DATE_MATCH_EXACT = 3

def check_existence_dates_with_datetime(record, authority_to, authority_from):
    quality_to = DATE_NO_DATA
    quality_from = DATE_NO_DATA
        
    if record.from_date and authority_from:
        if record.from_date_type == models.RECORD_DATE_TYPE_ACTIVE:
            if record.from_date >= authority_from:
                quality_from = DATE_MATCH
            else:
                quality_from = DATE_MATCH_FAIL
        elif record.from_date_type == models.RECORD_DATE_TYPE_BIRTH:
            difference = abs(record.from_date.year-authority_from.year)
            if  difference == 0:
                quality_from = DATE_MATCH_EXACT
            elif difference < 10:
                quality_from = DATE_MATCH_WITHIN_MARGIN
            else:
                logging.info("existence rejected: %d for %d" % (record.from_date.year, authority_from.year))
                quality_from = DATE_MATCH_FAIL
                
    if record.to_date and authority_to:
        if record.to_date_type == models.RECORD_DATE_TYPE_ACTIVE:
            if record.to_date <= authority_to:
                quality_to = DATE_MATCH
            else:
                quality_to = DATE_MATCH_FAIL
        elif record.to_date_type == models.RECORD_DATE_TYPE_DEATH:
            difference = abs(record.to_date.year-authority_to.year)
            if  difference == 0:
                quality_to = DATE_MATCH_EXACT
            elif difference < 10:
                quality_to = DATE_MATCH_WITHIN_MARGIN
            else:
                logging.info("existence rejected: %d for %d" % (record.to_date.year, authority_to.year))
                quality_to = DATE_MATCH_FAIL             
            
    return quality_from, quality_to   

def check_existence_dates(record, authority_dates):
    authority_to = None
    authority_from = None
    try:
        authority_from = dateutil.parser.parse(authority_dates[0], default=datetime.date(1, 1, 1))
    except Exception, e:
        pass
    try:
        authority_to = dateutil.parser.parse(authority_dates[1], default=datetime.date(1, 12, 31))
    except:
        pass
    return check_existence_dates_with_datetime(record, authority_to, authority_from)
    

def match_exact(record, record_type):
    group = models.PersonGroup
    if record_type == "corporate":
        group = models.CorporateGroup
    elif record_type == "family":
        group = models.FamilyGroup
    record_group = group.get_by_name(record.name_norm)
    if record_group and record_group.viaf_record:
        viafInfo = viaf.getEntityInformation(record_group.viaf_record)
        authority_dates = viafInfo.get('dates')
        quality_date_from, quality_date_to = check_existence_dates(record, authority_dates)
        if quality_date_from != 0 and quality_date_to != 0:
            return record_group, record_group.viaf_id, record_group.viaf_record, 1
    viaf_records = viaf.get_viaf_records(record.name_norm, index="xmainname[5=100]", name_type=record_type)
    viafInfo = viaf.getEntityInformation(viaf_record)
    viaf_id = viafInfo['recordId']
    match = 1
    authority_dates = viafInfo.get('dates')
    if viaf_id:
        quality_date_from = -1
        quality_date_to = -1
        if authority_dates:
            quality_date_from, quality_date_to = check_existence_dates(record, authority_dates)
            if quality_date_from != 0 and quality_date_to != 0:
                logging.info("Found viaf %s for %d %s" % (viaf_id, record.id, record.name_norm))
                record_group = group.get_by_viaf_id(viaf_id)
                if record_group:
                    logging.info("Found record_group id %d for %s using viaf %s" % (record_group.id, record.name_norm, viaf_id))
                else:
                    logging.info("No record_group created yet for %s using viaf %s" % (record.name_norm, viaf_id))
            else:
                logging.info("existence date mismatch for %s using viaf %s: %d %d" % (record.name_norm, viaf_id, quality_date_from, quality_date_to))
                viaf_id = None,
                record_group = None
                viaf_record = None
                match = 0
        else:
            logging.info("Failed viaf record date check %s using viaf %s" % (record.name_norm, viaf_id))
            viaf_id = None,
            record_group = None
            viaf_record = None
            match = 0
    else:
        logging.info("No viaf for %d %s" % (record.id, record.name_norm))
        
    return record_group, viaf_id, viaf_record, match
    
def match_person_exact(record):
    record_group = models.PersonGroup.get_by_name(record.name_norm)
    if record_group and record_group.viaf_record:
        return record_group, record_group.viaf_id, record_group.viaf_record, 1
    print record.name_norm
    viaf_records = viaf.query_cheshire_viaf(record.name_norm, name_type="person", index="xmainname[5=100]", limit=1)
    viaf_record = None
    if viaf_records:
        viaf_record = viaf_records[0]
    viafInfo = viaf.getEntityInformation(viaf_record)
    viaf_id = viafInfo['recordId']
    match = 1
    authority_dates = viafInfo.get('dates')
    if viaf_id:
        quality_date_from = -1
        quality_date_to = -1
        if authority_dates:
            quality_date_from, quality_date_to = check_existence_dates(record, authority_dates)
            if quality_date_from != 0 and quality_date_to != 0:
                logging.info("Found viaf %s for %d %s" % (viaf_id, record.id, record.name_norm))
                record_group = models.PersonGroup.get_by_viaf_id(viaf_id)
                if record_group:
                    logging.info("Found record_group id %d for %s using viaf %s" % (record_group.id, record.name_norm, viaf_id))
                else:
                    logging.info("No record_group created yet for %s using viaf %s" % (record.name_norm, viaf_id))
            else:
                logging.info("existence date mismatch for %s using viaf %s: %d %d" % (record.name_norm, viaf_id, quality_date_from, quality_date_to))
                viaf_id = None,
                record_group = None
                viaf_record = None
                match = 0
        else:
            logging.info("Failed viaf record date check %s using viaf %s" % (record.name_norm, viaf_id))
            viaf_id = None,
            record_group = None
            viaf_record = None
            match = 0
    else:
        logging.info("No viaf for %d %s" % (record.id, record.name_norm))
    return record_group, viaf_id, viaf_record, match

def match_person_ngram_viaf(record):
    match, match_quality = viaf_match_ngram(record)
    if match_quality > 0:
        record_group = models.RecordGroup.get_by_viaf_id(match['recordId'])
        return record_group, match['recordId'], match['_raw'], match_quality
    else:
        return None, None, None, -1
        
def compute_name_match_quality(x, y):
    x = re.sub("([\(\[]).*?([\)\]])", "", x)
    y = re.sub("([\(\[]).*?([\)\]])", "", y)
    x=utils.normalize_name_without_punc_with_space(x)
    y=utils.normalize_name_without_punc_with_space(y)
    length = utils.computeJaroWinklerDistance(x, y) # the basic score, without matching any name scrutiny tests
    match_name = nameparser.HumanName(x) # use the name parser to try to separate out components
    query_name = nameparser.HumanName(y)
    if len(match_name) > 1 and len(query_name) > 1:
        # if the name has more than 1 component, parse last name and first name.
        # if first names exist, make sure they match, then check middle names and/or initials if they exist
        if len(match_name.last) > 1:
            if match_name.last == query_name.last:
                logging.info("name quality code executed for %s vs %s" % (x, y))
                if (len(match_name.first) > 1 and match_name.first == query_name.first):
                    # if we have more than just a first initial, they should match
                    if not match_name.middle or not query_name.middle:
                        # if one of them is missing a middle name, fail as maybe
                        pass
                    elif len(match_name.middle) > 1 and len(query_name.middle) > 1:
                        # there is a middle name on both and both are not initials
                        if match_name.middle == query_name.middle:
                            logging.info("name match: %s for %s as exact" % (x, y))
                            return 1
                        else:
                            return 0
                    elif not match_name.middle and not query_name.middle:
                        # no middle names at all
                        return 1
                    elif (len(match_name.middle)==1 and match_name.middle[0] == query_name.middle[0]):
                        logging.info("name match : %s for %s as abbrev" % (x, y))
                        return 0.99
                    elif (len(query_name.middle)==1 and query_name.middle[0] == match_name.middle[0]):
                        logging.info("name match : %s for %s as abbrev" % (x, y))
                        return 0.99
                    else:
                        length = utils.computeJaroWinklerDistance((match_name.first + " " + match_name.last).encode('utf-8'), (query_name.first + " " + query_name.last).encode('utf-8'))
                        logging.info("name match: %s for %s as name %0.4f" % (x, y, length))
                        return ACCEPT_THRESHOLD if length >= ACCEPT_THRESHOLD else length
                elif (len(match_name.first) == 1 or len(query_name.first) == 1):
                    # if first initials only, only accept if it's a long match on middle
                    if query_name.middle and len(query_name.middle) > 1:
                        if query_name.first and match_name.first and query_name.first[0] == match_name.first[0] and len(match_name.middle) > 1 and match_name.middle == query_name.middle:
                            logging.info("name match: %s for %s as middle exact" % (x, y))
                            return 1
    elif len(match_name) == 1 and len(query_name) == 1:
        # if only single names
        if match_name.first and match_name.first == query_name.first:
            return 1
    # default quality capped at ACCEPT_THRESHOLD; only a maybe if it got here
    return ACCEPT_THRESHOLD if length >= ACCEPT_THRESHOLD else length


def match_person_ngram_postgres(record):
    candidates = record.name_similar_trigram(threshold=POSTGRES_NGRAM_THRESHOLD)
    for candidate_tuple in candidates:
        candidate, ngram_quality = candidate_tuple
        quality_from, quality_to = check_existence_dates_with_datetime(record, candidate.to_date, candidate.from_date)
        if quality_to == DATE_MATCH_FAIL or quality_from == DATE_MATCH_FAIL:
            logging.info("Failed postgres ngram existence check")
            continue
        else:
            logging.info("passed postgres existence check %d %d" % (quality_from, quality_to))
        x = utils.strip_accents(record.name)
        y = utils.strip_accents(candidate.name)
        if record.r_type=="person":
            name_quality = compute_name_match_quality(x,y)
            if name_quality >= STR_FUZZY_MATCH_THRESHOLD:
                logging.info("postgres accepted as yes or maybe: %s for %s at %0.4f" % (x, y, name_quality))
                return candidate, name_quality
            else:
                logging.info("postgres rejected: %s %.4f" % (x, name_quality))
        elif record.r_type=="corporate":
            length = utils.computeJaroWinklerDistance(x, y)
            if length >= STR_FUZZY_MATCH_THRESHOLD:
                logging.info("postgres accepted: %s for %s at %0.4f" % (x, y, length))
                #possible_matches.append((m, length))
                return candidate, length
            else:
                logging.info("postgres rejected: %s %.4f" % (x, length))
        else:
            distance = utils.computeSimpleRelativeLength(x, y)
            if 1-distance >= STR_FUZZY_MATCH_THRESHOLD:
                logging.info("postgres accepted: %s for %s at %0.4f" % (x, y, 1-distance))
                #possible_matches.append((m, 1-distance))
                return m, 1-distance
            else:
                logging.info("postgres rejected: %s %.4f" % (x, 1-distance))
    return None, -1
        
def viaf_match_ngram(record):
    '''accepts a Record as input, and returns a VIAF record matching the given record and a quality value between 0 and 1'''
    viaf_matches = viaf.get_viaf_records(record.name_norm, index="mainnamengram", name_type="person")
    #possible_matches = []
    if viaf_matches:
        viaf_matches = viaf_matches[:10]
        #logging.info("----")
        #logging.info("query: %s" % (name.encode('utf-8')))
        for m in viaf_matches:
            if m.get('mainHeadings') and len(m['mainHeadings']) > 0:
                # check existence date
                quality_from, quality_to = check_existence_dates(record, m['dates'])
                logging.info("candidate: %s" % (m['mainHeadings']))
                if quality_to == DATE_MATCH_FAIL or quality_from == DATE_MATCH_FAIL:
                    logging.info("Failed ngram existence check")
                    continue
                else:
                    logging.info("passed existence check %d %d" % (quality_from, quality_to))
                
                if record.r_type=="person":
                    x = utils.strip_accents(m['mainHeadings'][0])
                    y = utils.strip_accents(record.name)
                    name_quality = compute_name_match_quality(x, y)
                    if name_quality >= STR_FUZZY_MATCH_THRESHOLD:
                        logging.info("accepted as yes or maybe: %s for %s at %0.4f" % (x, y, name_quality))
                        return m, name_quality
                        #possible_matches.append((m, name_quality))
                    else:
                        logging.info("rejected: %s %.4f" % (x, name_quality))
                elif record.r_type=="corporate":
                    length = utils.computeJaroWinklerDistance(x, y)
                    if length >= STR_FUZZY_MATCH_THRESHOLD:
                        logging.info("accepted: %s for %s at %0.4f" % (x, y, length))
                        #possible_matches.append((m, length))
                        return m, length
                    else:
                        logging.info("rejected: %s %.4f" % (x, length))
                else:
                    distance = utils.computeSimpleRelativeLength(x, y)
                    if 1-distance >= STR_FUZZY_MATCH_THRESHOLD:
                        logging.info("accepted: %s for %s at %0.4f" % (x, y, 1-distance))
                        #possible_matches.append((m, 1-distance))
                        return m, 1-distance
                    else:
                        logging.info("rejected: %s %.4f" % (x, 1-distance))

        return viaf.VIAF.get_empty_viaf_dict(), 0
        #NOTE: reranking actually decreases accuracy; use jaro as a sanity check on ngram instead
        
        #rerank_matches = []
        #viaf_matches = viaf_matches[:5] # rerank only top 5 matches
        #for m in viaf_matches:
        #    if m.get('mainHeadings') and len(m['mainHeadings']) > 0:
        #        distance = snac.entityfeatures.computeJaroWinklerDistance(m['mainHeadings'][0].encode('utf-8'), name.encode('utf-8'))
        #        rerank_matches.append((m, distance))
        #rerank_matches.sort(lambda x, y: cmp(x[1], y[1]), reverse=True)
        #if rerank_matches:
        #    return rerank_matches[0][0], rerank_matches[0][1]
        #else:
        #    return viaf.VIAF.get_empty_viaf_dict(), 0
#            return viaf_matches[0], 1
    else:
        return viaf.VIAF.get_empty_viaf_dict(), -1 # no VIAF matches at all


def match_families(batch_size=1000):
    records = models.FamilyOriginalRecord.get_all_unprocessed_records(limit=batch_size)
    for record in records:
        if not record.record_group:
            record_group = models.FamilyGroup(name=record.name_norm)
            record_group.save()
            models.flush()
            
        if record.id not in [r.id for r in record_group.records]:
            record = models.FamilyOriginalRecord.get_by_id(record.id)
            record.record_group_id = record_group.id
                        
        logging.info("Creating new group for %d %s" % (record.id, record.name_norm))
        record.processed = True
        models.commit()
    return len(records)

def match_corporate(batch_size=1000):
    records = models.CorporateOriginalRecord.get_all_unprocessed_records(limit=batch_size)
    for record in records:
        record_group, viaf_id, viaf_record, match_quality = match_exact(record, record_type="corporate")
        if not record_group:
            record_group = models.CorporateGroup(name=record.name_norm)
            record_group.save()
            models.flush()
            logging.info("Creating new group for %d %s" % (record.id, record.name_norm))

        if record.id not in [r.id for r in record_group.records]:
            record = models.CorporateOriginalRecord.get_by_id(record.id)
            record.record_group_id = record_group.id
            
            logging.info("Adding %d %s to group %d" % (record.id, record.name_norm, record_group.id))
        else:
            logging.info("Duplicate attempt to add %d %s to group %d" % (record.id, record.name_norm, record_group.id))
        if not record_group.viaf_record:
            if viaf_id:
                record_group.viaf_id = viaf_id
                record_group.viaf_record = viaf_record
        else:
            logging.info("Already a viaf for %d %s; not attempting again" % (record.id, record.name_norm))
        record.processed = True
        models.commit()
    return len(records)
        
if __name__ == "__main__":
    db_uri = db_config.get_db_uri()
    models.init_model(db_uri)
    match_persons_loop()
    match_corporate_loop()
    match_families_loop()
