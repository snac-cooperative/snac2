#!/usr/bin/env python

import snac2.config.app as app_config
import snac2.config.db as db_config
import snac2.models as models
import snac2.viaf as viaf
import snac2.utils as utils
import dateutil.parser, datetime
import lxml.etree as etree
import os, logging, re
import nameparser

STR_FUZZY_MATCH_THRESHOLD  = 0.85
ACCEPT_THRESHOLD = 0.9
CORP_ACCEPT_THRESHOLD = 0.95
POSTGRES_NGRAM_THRESHOLD = 0.75

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d %I:%M:%S %p', level=logging.INFO)

def match_persons_loop(starts_at=None, ends_at=None):
    unprocessed_records = True
    while unprocessed_records:
        records = models.PersonOriginalRecord.get_all_unprocessed_records(limit=100, min_id=starts_at, max_id=ends_at)
        if not records:
            unprocessed_records = False
        else:
            match_persons(records)
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

def match_persons(records):
    
    for record in records:
        viaf_id = None
        viaf_record = None
        match_quality = 1
        candidate = None
        maybe_viaf_id = None
        maybe_postgres_id = None
        record_group = None
        
        # if the collection is BNF, use sameAs matching first
        if record.collection_id == "ead_bnf":
            logging.info("attempting nsid special match on %d" % (record.id))
            record_group, viaf_id, viaf_record, match_quality = match_any_nsid_viaf(record)
            if record_group:
                logging.info("matched %d using nsid special %s" % (record.id, record_group.viaf_id))
        
        # exact match against in-db records
        if not record_group and not viaf_id:
            record_group, viaf_id, viaf_record, match_quality = match_person_exact(record)
            
        # ngram
        if not record_group and not viaf_id:
            record_group, viaf_id, viaf_record, match_quality = match_person_keyword_viaf(record)
            if match_quality <= ACCEPT_THRESHOLD:
                logging.info("keyword only found %s at %.2f, moving on to ngram", viaf_id, match_quality )
                record_group, viaf_id, viaf_record, match_quality = match_person_ngram_viaf(record)
            else:
                logging.info("keyword found %s at %.2f", viaf_id, match_quality )
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
                candidate = None
                match_quality = -1
                #candidate, match_quality = match_person_ngram_postgres(record)
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
    if record_type == "corporate" and record_group and len(record.name_norm) >= 20:
        logging.info("Found exact match using length check: %d %s" % (record_group.id, record.name_norm))
        return record_group, record_group.viaf_id, record_group.viaf_record, 1
    if record_group and record_group.viaf_record:
        viafInfo = viaf.getEntityInformation(record_group.viaf_record)
        authority_dates = viafInfo.get('dates')
        quality_date_from, quality_date_to = check_existence_dates(record, authority_dates)
        if quality_date_from != 0 and quality_date_to != 0:
            return record_group, record_group.viaf_id, record_group.viaf_record, 1
    viaf_records = viaf.query_cheshire_viaf(record.name_norm, index="xmainname[5=100]", name_type=record_type)
    if viaf_records:
        viaf_record = viaf_records[0]
        viafInfo = viaf.getEntityInformation(viaf_record)
        viaf_id = viafInfo['recordId']
        match = 1
        authority_dates = viafInfo.get('dates')
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
                return None, None, None, 0
        else:
            logging.info("Failed viaf record date check %s using viaf %s" % (record.name_norm, viaf_id))
            return None, None, None, 0
        return record_group, viaf_id, viaf_record, match
    else:
        logging.info("No viaf for %d %s" % (record.id, record.name_norm))
        return None, None, None, 0
        

def viaf_name_exact_validate_check(source_name):
    return (len(source_name) > 30 or utils.has_n_digits(source_name, n=4))

def match_person_exact(record, in_db_match=True):
    if in_db_match:
        record_group = models.PersonGroup.get_by_name(record.name_norm)
        if record_group:
            if record_group.merge_records and not record_group.merge_records[0].valid:
                logging.info("record_group %d:%s found in database for record %d:%s but is invalid, skipping" % (record_group.id, record_group.name, record.id, record.name_norm))
            else:
                quality_date_from = DATE_NO_DATA
                quality_date_to = DATE_NO_DATA
                for candidate in record_group.records:
                    date_from, date_to = check_existence_dates_with_datetime(record, candidate.to_date, candidate.from_date)
                    if date_from > quality_date_from:
                        quality_date_from = date_from
                    if date_to > quality_date_to:
                        quality_date_to = date_to
                if (((len(record.name_norm) > 10) and (quality_date_from > DATE_MATCH_FAIL or quality_date_to > DATE_MATCH_FAIL)) or
                    ((len(record_group.name) > 30 or utils.has_n_digits(record_group.name, n=2)) and (quality_date_from != DATE_MATCH_FAIL and quality_date_to != DATE_MATCH_FAIL))
                    ):
                    # if either the name length is greater than 10, and has either a matching date_from or a matching date_to
                    # or if the length is greater than 35, or has at least 2 digits (a year) in the name, and didn't fail quality date checks (but can be empty)
                    logging.info("record_group %d:%s found in database for record %d:%s" % (record_group.id, record_group.name, record.id, record.name_norm))
                    return record_group, record_group.viaf_id, record_group.viaf_record, 1
                else:
                    logging.info("failed validation check record_group %d:%s for record %d:%s" % (record_group.id, record_group.name, record.id, record.name_norm))
    is_spirit = False
    if "(spirit)" in record.name.lower():
        is_spirit = True
    viaf_records = viaf.query_cheshire_viaf(record.name_norm, name_type="person", index="xmainname[5=100]", limit=1, is_spirit=is_spirit)
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
                if quality_date_from != 0 and quality_date_to != 0 and viaf_name_exact_validate_check(record.name_norm):
                    logging.info("Found viaf %s for %d %s" % (viaf_id, record.id, record.name_norm))
                    record_group = models.PersonGroup.get_by_viaf_id(viaf_id)
                    if record_group:
                        logging.info("Found record_group id %d for %s using viaf %s" % (record_group.id, record.name_norm, viaf_id))
                    else:
                        logging.info("No record_group created yet for %s using viaf %s" % (record.name_norm, viaf_id))
                else:
                    logging.info("existence date mismatch for %s using viaf %s: %d %d" % (record.name_norm, viaf_id, quality_date_from, quality_date_to))
                    return None, None, None, 0
            else:
                logging.info("Failed viaf record date check %s using viaf %s" % (record.name_norm, viaf_id))
                return None, None, None, 0
        else:
            logging.info("No viaf for %d %s" % (record.id, record.name_norm))
        return record_group, viaf_id, viaf_record, match
    else:
        return None, None, None, 0

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
    x=utils.normalize_name_without_punc_with_space(x, lowercase=False)
    y=utils.normalize_name_without_punc_with_space(y, lowercase=False)
    x = utils.strip_controls(x)
    y = utils.strip_controls(y)
    length = utils.computeJaroWinklerDistance(x, y) # the basic score, without matching any name scrutiny tests
    match_name = nameparser.HumanName(x) # use the name parser to try to separate out components
    query_name = nameparser.HumanName(y)
    if len(match_name) > 1 and len(query_name) > 1:
        # if the name has more than 1 component, parse last name and first name.
        # if first names exist, make sure they match, then check middle names and/or initials if they exist
        if len(match_name.last) > 1:
            if match_name.last == query_name.last:
                #logging.info("name quality code executed for %s vs %s" % (x, y))
                if (len(match_name.first) > 1 and match_name.first == query_name.first):
                    # if we have more than just a first initial, they should match
                    if not match_name.middle or not query_name.middle:
                        # if one of them is missing a middle name, fail as maybe
                        pass
                    elif ((match_name.title.lower()=="mrs") or (query_name.title.lower()=="mrs") and match_name.title != query_name.title):
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
                    elif ((match_name.middle and query_name.middle) and query_name.middle[0] != match_name.middle[0]):
                        # just to make sure
                        return 0
                    elif (len(match_name.middle)==1 and match_name.middle[0] == query_name.middle[0]):
                        logging.info("name match : %s for %s as abbrev" % (x, y))
                        return 0.99
                    elif (len(query_name.middle)==1 and query_name.middle[0] == match_name.middle[0]):
                        logging.info("name match : %s for %s as abbrev" % (x, y))
                        return 0.99
                    else:
                        length = utils.computeJaroWinklerDistance((match_name.first + " " + match_name.last).encode('utf-8'), (query_name.first + " " + query_name.last).encode('utf-8'))
                        logging.info("name match: %s for %s as name %0.4f" % (x, y, length))
                        return ACCEPT_THRESHOLD-0.00001 if length >= ACCEPT_THRESHOLD else length
                elif (len(match_name.first) == 1 or len(query_name.first) == 1):
                    # if first initials only, only accept if it's a long match on middle
                    if query_name.middle and len(query_name.middle) > 1:
                        if query_name.first and match_name.first and query_name.first[0] == match_name.first[0] and len(match_name.middle) > 1 and match_name.middle == query_name.middle:
                            logging.info("name match: %s for %s as middle exact" % (x, y))
                            return 1
            else:
                logging.info("last name match fail: %s vs %s" % (match_name.last, query_name.last))
                return 0
    elif len(match_name) == 1 and len(query_name) == 1:
        # if only single names
        if match_name.first and match_name.first == query_name.first:
            return 1
    # default quality capped at ACCEPT_THRESHOLD; only a maybe if it got here
    return ACCEPT_THRESHOLD-0.00001 if length >= ACCEPT_THRESHOLD else length


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
    is_spirit = False
    if "(spirit)" in record.name.lower():
        is_spirit = True
    viaf_matches = viaf.query_cheshire_viaf(record.name_norm, index="mainnamengram", name_type="person", is_spirit=is_spirit)
    viaf_matches = [viaf.getEntityInformation(r) for r in viaf_matches]
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
                elif quality_to == DATE_MATCH_EXACT and quality_from == DATE_MATCH_EXACT and record.name.startswith(m['mainHeadings'][0][:5]):
                    return m, 1
                else:
                    logging.info("passed existence check %d %d" % (quality_from, quality_to))
                if record.r_type=="person":
                    for heading in m['mainHeadings']:
                        x = utils.strip_accents(heading)
                        y = utils.strip_accents(record.name)
                        name_quality = compute_name_match_quality(x, y)
                        if name_quality >= STR_FUZZY_MATCH_THRESHOLD:
                            logging.info("accepted as yes or maybe: %s for %s at %0.4f" % (x, y, name_quality))
                            return m, name_quality
                            #possible_matches.append((m, name_quality))
                        else:
                            logging.info("rejected heading: %s %.4f" % (x, name_quality))
                # elif record.r_type=="corporate":
#                     length = utils.computeJaroWinklerDistance(x, y)
#                     if length >= STR_FUZZY_MATCH_THRESHOLD:
#                         logging.info("accepted: %s for %s at %0.4f" % (x, y, length))
#                         #possible_matches.append((m, length))
#                         return m, length
#                     else:
#                         logging.info("rejected: %s %.4f" % (x, length))
#                 else:
#                     distance = utils.computeSimpleRelativeLength(x, y)
#                     if 1-distance >= STR_FUZZY_MATCH_THRESHOLD:
#                         logging.info("accepted: %s for %s at %0.4f" % (x, y, 1-distance))
#                         #possible_matches.append((m, 1-distance))
#                         return m, 1-distance
#                     else:
#                         logging.info("rejected: %s %.4f" % (x, 1-distance))

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
        record_group = None
        viaf_id = None
        if record.collection_id == "ead_bnf":
            logging.info("attempting nsid special match on %d" % (record.id))
            record_group, viaf_id, viaf_record, match_quality = match_any_nsid_viaf(record)
            if record_group:
                logging.info("matched %d using nsid special %s" % (record.id, record_group.viaf_id))
        if not record_group and not viaf_id:
            record_group = record.record_group
        if not record_group:
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
        record_group = None
        viaf_id = None
        if record.collection_id == "ead_bnf":
            logging.info("attempting nsid special match on %d" % (record.id))
            record_group, viaf_id, viaf_record, match_quality = match_any_nsid_viaf(record)
            if record_group:
                logging.info("matched %d using nsid special %s" % (record.id, record_group.viaf_id))
        # exact match against in-db records
        if not record_group and not viaf_id:
            record_group, viaf_id, viaf_record, match_quality = match_exact(record, record_type="corporate")
        maybe_viaf_id = None
        if not record_group and not viaf_id:
            record_group, viaf_id, viaf_record, match_quality = match_corp_keyword_viaf(record)
            if match_quality >= CORP_ACCEPT_THRESHOLD:
                logging.info("accepted: %s" % (viaf_id))
            elif match_quality >= STR_FUZZY_MATCH_THRESHOLD:
                maybe_viaf_id = viaf_id
                viaf_id = record_group = viaf_record = None
                logging.info("maybe: %s" % (maybe_viaf_id))
                
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
        if maybe_viaf_id:
            candidate = models.MaybeCandidate(candidate_type="viaf", candidate_id=maybe_viaf_id, record_group_id=record_group.id)
            candidate.save()
            logging.info("Set %s as maybe for record_group %d" % (maybe_viaf_id, record_group.id))
            maybe_viaf_id = None
        record.processed = True
        models.commit()
    return len(records)

def viaf_match_keyword_person(record):
    is_spirit = False
    if "(spirit)" in record.name.lower():
        is_spirit = True
    viaf_matches = viaf.query_cheshire_viaf(record.name_norm, index="mainname", name_type="person", is_spirit=is_spirit)
    viaf_matches = [viaf.getEntityInformation(r) for r in viaf_matches]
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
                    logging.info("Failed keyword existence check")
                    continue
                elif quality_to == DATE_MATCH_EXACT and quality_from == DATE_MATCH_EXACT and record.name.startswith(m['mainHeadings'][0][:5]):
                    #shortcut
                    return m, 1
                else:
                    logging.info("passed existence check %d %d" % (quality_from, quality_to))
                for heading in m['mainHeadings']:
                    x = utils.strip_accents(heading)
                    y = utils.strip_accents(record.name)
                    name_quality = compute_name_match_quality(x, y)
                    if name_quality >= STR_FUZZY_MATCH_THRESHOLD:
                        logging.info("accepted as yes or maybe: %s for %s at %0.4f" % (x, y, name_quality))
                        return m, name_quality
                        #possible_matches.append((m, name_quality))
                    else:
                        logging.info("rejected: %s %.4f" % (x, name_quality))
                

    return viaf.VIAF.get_empty_viaf_dict(), 0

def viaf_match_keyword(record, name_type="corporate"):
    name_norm = record.name_norm
    if name_type == "corporate":
        name_norm = utils.strip_corp_abbrevs(utils.strip_accents(utils.normalize_name_without_punc_with_space(name_norm)))
    logging.info("keyword matching on %d %s" % (record.id, name_norm))
    viaf_matches = viaf.query_cheshire_viaf(name_norm, index="mainname", name_type=name_type)
    viaf_matches = [viaf.getEntityInformation(r) for r in viaf_matches]
    for m in viaf_matches:
        for heading in m['mainHeadings']:                    
            candidate_name = utils.strip_corp_abbrevs(utils.strip_accents(utils.normalize_name_without_punc_with_space(heading)))
            length = utils.computeJaroWinklerDistance(name_norm, candidate_name)
            if length >= STR_FUZZY_MATCH_THRESHOLD:
                logging.info("accepted as yes or maybe: %s for %s at %0.4f" % (name_norm, candidate_name, length))
                return m, length
    return viaf.VIAF.get_empty_viaf_dict(), 0 

def match_corp_keyword_viaf(record):
    match, match_quality = viaf_match_keyword(record, name_type="corporate")
    if match_quality > 0:
        record_group = models.RecordGroup.get_by_viaf_id(match['recordId'])
        return record_group, match['recordId'], match['_raw'], match_quality
    else:
        return None, None, None, -1

def match_person_keyword_viaf(record):
    match, match_quality = viaf_match_keyword_person(record)
    if match_quality > 0:
        record_group = models.RecordGroup.get_by_viaf_id(match['recordId'])
        return record_group, match['recordId'], match['_raw'], match_quality
    else:
        return None, None, None, -1

def match_any_nsid_viaf(record):
    nsmap = {"eac":"urn:isbn:1-931666-33-4", "xlink":"http://www.w3.org/1999/xlink"}
    p = etree.XMLParser(huge_tree=True) # retry the parse with a huge_tree
    doc = etree.parse(record.path, parser=p)
    sameAs_relations = doc.xpath('//eac:cpfRelation[@xlink:arcrole="http://www.w3.org/2002/07/owl#sameAs"]', namespaces=nsmap)
    if not sameAs_relations:
        logging.info("No sameAs found in %d"  % (record.id))
        return None, None, None, -1
    nsid = sameAs_relations[0].attrib.get("{http://www.w3.org/1999/xlink}href")
    if not nsid:
        logging.info("No nsid found in %d"  % (record.id))
        return None, None, None, -1
    if nsid.endswith("/PUBLIC"):
        nsid = nsid[:-1*len("/PUBLIC")]
    viaf_record = viaf.query_cheshire_nsid(nsid)
    if not viaf_record:
        logging.info("No viaf found for %d using nsid %s"  % (record.id, nsid))
        return None, None, None, -1
    match = viaf.getEntityInformation(viaf_record)
    record_group = models.RecordGroup.get_by_viaf_id(match['recordId'])
    return record_group, match['recordId'], match['_raw'], 1
    

if __name__ == "__main__":
    db_uri = db_config.get_db_uri()
    models.init_model(db_uri)
    viaf.config_cheshire(db=app_config.VIAF_INDEX_NAME)
    import sys
    import argparse
    parser = argparse.ArgumentParser()
    #group = parser.add_mutually_exclusive_group()
    parser.add_argument("-p", "--person", action="store_true", help="match persons")
    parser.add_argument("-c", "--corporate", action="store_true", help="match corporate entities")
    parser.add_argument("-f", "--family", action="store_true", help="match families")
    parser.add_argument("-s", "--starts_at", help="start the match at this ID", type=int)
    parser.add_argument("-e", "--ends_at", help="stop matching at this ID", type=int)
    
    args=parser.parse_args()
    if args.person:
        match_persons_loop(starts_at=args.starts_at, ends_at=args.ends_at)
    if args.corporate:
        match_corporate_loop()
    if args.family:
        match_families_loop()

