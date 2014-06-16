#!/bin/python
import sys, string, datetime, cStringIO, uuid
import logging
import xml.dom.minidom
import lxml.etree as etree
import os.path

import snac2.config.app as app_config
import snac2.config.db as db_config
import snac2.models as models
import snac2.viaf as viaf
import snac2.cpf

from xml.sax.saxutils import escape

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d %I:%M:%S %p', level=logging.INFO)

def createCombinedRecord(cpfRecords, viafInfo, dbpediaInfo=None, r_type="", canonical_id="", maybes=None):
    """
	Create a combined EAC-CPF record by merging a list of CPF and name authority records
	K's routine is rather aggravating by not using a parser to assemble the document, and instead is writing raw strings with tags. fix this later, but validate output first
    """
    #record id
    mrecordIds = []
    
    #type
    mtypes = []
    
    #names
    mnames = []
    
    #sources
    msources = []
    
    #exist dates
    mexistDates = []
    
    #occupations
    moccupations = []
    
    #local descriptions
    mlocalDescriptions = []
    
    #relations
    mrelations = []
    
    #resource relations
    mresourceRelations = []
    
    #biography
    mbiography = []
    
    relations_canonical_idx = {}
    for cpfRecord in cpfRecords:
        legacyDoc = xml.dom.minidom.parse(cpfRecord)
        identityInfo = snac2.cpf.parseIdentityRaw(legacyDoc)
        if identityInfo != None:
            recordIds = identityInfo['id']
            if recordIds != None:
                mrecordIds.append(recordIds)         
            names =  identityInfo['name']
            if names != None:
                mnames.extend(names)
            type =  identityInfo['type']
            if type != None:
                mtypes.append(type)
            
        
        sources = snac2.cpf.parseSources(cpfRecord)
        if sources != None:
            msources.extend(sources)
        
        existDates = snac2.cpf.parseExistDates(cpfRecord)
        if existDates != None:
            mexistDates.extend(existDates)
        
        occupations = snac2.cpf.parseOccupations(legacyDoc)
        if occupations != None:
            moccupations.extend(occupations)
        
        localDescriptions = snac2.cpf.parseLocalDescriptions(legacyDoc)
        if localDescriptions != None:
            mlocalDescriptions.extend(localDescriptions)
        
        relations = snac2.cpf.parseAssociationsRaw(cpfRecord)
        if relations != None:
            filtered_relations = []
            for relation in relations:
                seen = False
                extracted_records = relation.getElementsByTagName("span")
                extracted_records = extracted_records[0]
                extracted_record_id = extracted_records.childNodes[0].nodeValue
                original_record = models.OriginalRecord.get_by_source_id(extracted_record_id)
                if original_record:
                    record_group = original_record.record_group
                    if not record_group:
                        logging.warning("Warning %s has no record_group" % (extracted_record_id))
                        relation.setAttribute("xlink:href", "")
                        continue
                    merge_records = record_group.merge_records
                    if not merge_records:
                        logging.warning("Warning %s has no merge record" % (extracted_record_id))
                        relation.setAttribute("xlink:href", "")
                        continue
                    for record in merge_records:
                        if record.canonical_id in relations_canonical_idx:
                            seen = True # there is a duplicate
                            logging.warning("%s already seen" % (record.canonical_id))
                            continue
                        else:
                            relations_canonical_idx[record.canonical_id] = 1 # make sure no duplicates
                            logging.info( "%s recorded" % (record.canonical_id) )
                        if record.valid:
                            if record.canonical_id:
                                relation.setAttribute("xlink:href", record.canonical_id)
                            else:
                                logging.warning( "Warning %s has no ARK ID" % (extracted_record_id) )
                                relation.setAttribute("xlink:href", "")
                            break
                        else:
                            logging.warning( "record not valid" )
                else:
                    logging.warning( "Warning %s has no original record" % (extracted_record_id) )
                    continue
                relation.removeChild(relation.getElementsByTagName("descriptiveNote")[0])
                if not seen:
                    filtered_relations.append(relation)
            mrelations.extend(filtered_relations)
        
        resourceRelations = snac2.cpf.parseResourceAssociationsRaw(cpfRecord)
        if resourceRelations != None:
            mresourceRelations.extend(resourceRelations)
            
        biography = snac2.cpf.parseBiogHist(cpfRecord)
        if biography != None:
            mbiography.append(biography)
            
    
    #Stich a new record
    cr = cStringIO.StringIO()
    
    #Root
    cr.write('<?xml version="1.0" encoding="UTF-8"?>')
    cr.write("""<eac-cpf xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:owl="http://www.w3.org/2002/07/owl#"
         xmlns="urn:isbn:1-931666-33-4"
         xmlns:xlink="http://www.w3.org/1999/xlink">""")
         
    #Control
    cr.write("<control>")
    
    #RecordID
    i = 0
    if canonical_id:
        cr.write('<recordId>%s</recordId>' % canonical_id)

    for recordId in mrecordIds:
        cr.write('<otherRecordId  localType="http://socialarchive.iath.virginia.edu/control/term#MergedRecord">%s</otherRecordId>' %recordId)
        i = i + 1
        
    #VIAF
    #1. From VIAF, add to eac-cpf/control after recordId
    viafRecordId = viafInfo['recordId']
    if viafRecordId != None:
        cr.write('<otherRecordId localType="http://viaf.org/viaf/terms#viafID">')
        cr.write('%s' %viafRecordId)
        cr.write('</otherRecordId>') 

    #DBPedia
    #From DBPedia, add to eac-cpf/control after recordid
    #dbpediaRecordId = dbpediaInfo['recordId']
    #if dbpediaRecordId != None:
    #    cr.write('<otherRecordId localType="dbpedia">')
    #    cr.write('dbpedia:%s' %dbpediaRecordId)
    #    cr.write('</otherRecordId>')

    #Maintanence
    cr.write("<maintenanceStatus>revised</maintenanceStatus>")
    cr.write("""<maintenanceAgency>
                     <agencyName>SNAC: Social Networks and Archival Context Project</agencyName>
                </maintenanceAgency>""")
    
    #Language
    cr.write("""<languageDeclaration>
                <language languageCode="eng">English</language>
                <script scriptCode="Latn">Latin Alphabet</script>
                </languageDeclaration>""")
    
    #2.From VIAF, add to eac-cpf/control after languageDeclaration
    if viafRecordId != None:
        cr.write('<conventionDeclaration>')
        cr.write('<citation>VIAF</citation>')
        cr.write('</conventionDeclaration>')

    
    #Maintanence History
    cr.write("""<maintenanceHistory>
                <maintenanceEvent>
                <eventType>revised</eventType>
                <eventDateTime>%s</eventDateTime> 
                <agentType>machine</agentType>
                <agent>CPF merge program</agent>
                <eventDescription>Merge v2.0</eventDescription>
                </maintenanceEvent>
                </maintenanceHistory>""" %datetime.date.today())
    #Sources
    cr.write("<sources>")
    for source in msources:
        cr.write(source.toxml(encoding='utf-8'))
    
    #VIAF
    if viafRecordId != None:
        cr.write('<source xlink:type="simple" xlink:href="http://viaf.org/viaf/%s"/>' %viafRecordId)
    
    cr.write("</sources>")
    
    #END control
    cr.write("</control>")
    
    #CPF Description
    cr.write("<cpfDescription>")
    
    #Identity
    cr.write("<identity>")
    
    #Type
    for type in set(mtypes):
        cr.write("<entityType>%s</entityType>" %type)
    
    #Name
    if mnames:
        mnames_index = set()
        mnames = filter(lambda name: snac2.cpf.extract_subelement_content_from_entry(name, "part") not in mnames_index and (mnames_index.add(snac2.cpf.extract_subelement_content_from_entry(name, "part")) or True), mnames)
        mnames.sort(key=lambda name: len(snac2.cpf.extract_subelement_content_from_entry(name, "part")), reverse=True)

    
    #VIAF Mainheadings
    viaf_name_nodes = viafInfo['mainHeadingsData']
    viaf_names = []
    viaf_name_index = {}
    LOC_name = []
    for nameEntry in viaf_name_nodes:
        text_nodes =  nameEntry.getElementsByTagName("text")
        source_nodes = nameEntry.getElementsByTagName("s")
        name = ""
        authorized_form = ""
        
        if text_nodes:
            name = text_nodes[0].firstChild.nodeValue
            if source_nodes:
                for source_node in source_nodes:
                    source = source_node.firstChild.nodeValue
                    if source and source.strip() == "LC":
                        authorized_form = "LoC"
                        LOC_name = (name, authorized_form)
                        break
        if not authorized_form:
            authorized_form = "VIAF"
        if name in viaf_name_index:
            pass
        else:
            viaf_name_index[name] = 1
            if authorized_form != "LoC":
                viaf_names.append((name, authorized_form))
    if LOC_name:
        viaf_names = [LOC_name] + viaf_names
    for name in viaf_names:
        cr.write('<nameEntry localType="http://viaf.org/viaf/terms#mainHeadings/data/text">')
        cr.write('<part>')
        cr.write(escape(name[0]).encode('utf-8'))
        cr.write('</part>')
        if (name[1] == "LoC"):
            cr.write('<authorizedForm>LoC</authorizedForm>')
        else:
            cr.write('<alternativeForm>VIAF</alternativeForm>')
        cr.write('</nameEntry>')
    for name in mnames:
        cr.write(name.toxml().encode('utf-8'))
        
        
    #VIAF X400s 
#     for nameEntry in set(viafInfo['x400s']) :
#         cr.write('<nameEntry localType="http://viaf.org/viaf/terms#x400s/400">')
#         cr.write('<part localType="http://viaf.org/viaf/terms#x400s/400/a">')
#         cr.write(escape(nameEntry).encode('utf-8'))
#         cr.write('</part>')
#         cr.write('<alternativeForm>VIAF</alternativeForm>')
#         cr.write('</nameEntry>')

    for x400 in viafInfo['x400s'] :
        cr.write('<nameEntry localType="http://viaf.org/viaf/terms#x400s/%s">' % (x400['tag']))
        for subfield in x400['subfields']:
            cr.write('<part localType="http://viaf.org/viaf/terms#x400s/%s/%s">' % (x400['tag'], subfield['code']))
            cr.write(escape(subfield['value']).encode('utf-8'))
            cr.write('</part>')
        cr.write('<alternativeForm>VIAF</alternativeForm>')
        cr.write('</nameEntry>')
        
    #END identity   
    cr.write("</identity>")
    
    #Description
    cr.write("<description>")
        
    #Exist Dates
    if len(mexistDates) > 0:
        existDate = mexistDates[0] #Which one to choose ?
        cr.write(existDate.toxml().encode('utf-8')) 
    else:
        existDate = viafInfo['dates']
        if existDate != None and (existDate[0] !='0' or existDate[1] != '0'):
            cr.write("<existDates>")
            if r_type != "person":
                 pass
            else:
                cr.write("<dateRange>")
                if existDate[0] and existDate[0] != '0':
                    term_start = "Birth"
                    cr.write("<fromDate standardDate=\"%s\" localType=\"http://socialarchive.iath.virginia.edu/control/term#%s\">" % (snac2.cpf.pad_exist_date(existDate[0]), term_start))
                    cr.write(existDate[0])
                    cr.write("</fromDate>")
                if existDate[1] and existDate[1] != '0':
                    term_end = "Death"
                    cr.write("<toDate standardDate=\"%s\" localType=\"http://socialarchive.iath.virginia.edu/control/term#%s\">" % (snac2.cpf.pad_exist_date(existDate[1]), term_end))
                    cr.write(existDate[1])
                    cr.write("</toDate>")
                cr.write("</dateRange>")
            cr.write("</existDates>")

    
    #Local Descriptions
    for localDescription in mlocalDescriptions:
        cr.write(localDescription.toxml().encode('utf-8'))
    
    #Nationality from VIAF
    entityNationality = viafInfo['nationality']
    for nationality in set(entityNationality):
        cr.write('<localDescription localType="http://viaf.org/viaf/terms#nationalityOfEntity">')
        cr.write('<placeEntry countryCode="%s"/>' %nationality.encode('utf-8'))
        cr.write('</localDescription>')
        
    #Gender from VIAF
    gender = viafInfo['gender']
    if gender != None:
        cr.write('<localDescription localType="http://viaf.org/viaf/terms#gender">')
        cr.write('<term>%s</term>' %gender)
        cr.write('</localDescription>')
        
    #Languages used from VIAF - should add script
    entityLanguages = viafInfo['language']
    for language in set(entityLanguages):
        cr.write('<languageUsed>')
        cr.write('<language languageCode="%s"/>' % escape(language.lower()).encode('utf-8'))
        cr.write('<script scriptCode="Zyyy"/>')
        cr.write('</languageUsed>')
        
    #Occupations
    if moccupations:
        occupations_index = set()
        moccupations = filter(lambda o: snac2.cpf.extract_subelement_content_from_entry(o, "term") not in occupations_index and (occupations_index.add(snac2.cpf.extract_subelement_content_from_entry(o, "term")) or True), moccupations)

    for occupation in moccupations:
        cr.write(occupation.toxml().encode('utf-8'))

    #BiogHist
    biogText = {}
    for biogHist in mbiography:
        # de-duplicate
        text = biogHist['text']
        concat_text = "\n".join(text)
        citation = biogHist['citation']
        if concat_text in biogText:
            biogText[concat_text]['citation'].append(citation)
        else:
            biogText[concat_text] = {'text':text, 'citation':[citation]}
    
    for concat_text in biogText.keys():
        cr.write("<biogHist>")
        for text in biogText[concat_text]['text']:
            cr.write("%s" % text.encode('utf-8'))
        for citation in biogText[concat_text]['citation']:
            cr.write("%s" % citation.encode('utf-8'))
        cr.write("</biogHist>")
        #cr.write(biogHist['raw'].encode('utf-8'))
        #print biogHist['raw'].encode('utf-8')
        #cr.write("<biogHist>"+escape(biogHist).encode('utf-8')+"</biogHist>")
    
    #END Description
    cr.write("</description>")
    
    #Relations
    cr.write("<relations>")
    
    #CPF Relations
    for relation in mrelations:
        #print [(k, relation.attributes[k].value) for k in relation.attributes.keys()]
        cr.write(relation.toxml().encode('utf-8'))
        
    if r_type == "person" or r_type == "corporateBody":
        r_type_t = r_type.capitalize()
        headings = viafInfo['mainHeadings']
        if headings:
            cr.write('<cpfRelation xlink:type="simple" xlink:href="http://viaf.org/viaf/%s"  xlink:arcrole="http://socialarchive.iath.virginia.edu/control/term#sameAs" xlink:role="http://socialarchive.iath.virginia.edu/control/term#%s">' % (viafRecordId, r_type_t)) 
            cr.write("<relationEntry>%s</relationEntry>" % (headings[0].encode('utf-8')))
            cr.write("</cpfRelation>")
        headingsEl = viafInfo['mainElementEl']
        if headingsEl:
            for h in headingsEl:
                if h["source"] == "LC":
                    cr.write('<cpfRelation xlink:type="simple"  xlink:arcrole="http://socialarchive.iath.virginia.edu/control/term#sameAs" xlink:href="http://www.worldcat.org/wcidentities/%s" xlink:role="http://socialarchive.iath.virginia.edu/control/term#%s" />' % (h["lccn_wcid"], r_type_t)) 
                    cr.write('<cpfRelation xlink:type="simple"  xlink:arcrole="http://socialarchive.iath.virginia.edu/control/term#sameAs" xlink:href="http://id.loc.gov/authorities/names/%s" xlink:role="http://socialarchive.iath.virginia.edu/control/term#%s" />' % (h["lccn_lcid"], r_type_t)) 
                elif h["source"] == "WKP":
                    cr.write((u'<cpfRelation xlink:type="simple" xlink:href="http://en.wikipedia.org/wiki/%s"  xlink:arcrole="http://socialarchive.iath.virginia.edu/control/term#sameAs"  xlink:role="http://socialarchive.iath.virginia.edu/control/term#%s" />' % (h["url_id"], r_type_t)).encode('utf-8')) 

    if maybes:
        for merge_candidate in maybes:
            #print merge_candidate.record_group.records[0].name.__repr__()
            cr.write('<cpfRelation xlink:type="simple"  xlink:arcrole="http://socialarchive.iath.virginia.edu/control/term#mayBeSameAs" xlink:href="%s" xlink:role="http://socialarchive.iath.virginia.edu/control/term#%s"><relationEntry>%s</relationEntry></cpfRelation>' % (merge_candidate.canonical_id.encode('utf-8'), r_type_t.encode('utf-8'), merge_candidate.record_group.records[0].name.encode('utf-8'))) 

        
    #Resource Relations
    for resourceRelation in mresourceRelations:
        cr.write(resourceRelation.toxml().encode('utf-8'))
        
    #Titles from VIAF
    for title in viafInfo['titles']:
        cr.write('<resourceRelation xlink:arcrole="http://socialarchive.iath.virginia.edu/control/term#creatorOf" xlink:role="http://socialarchive.iath.virginia.edu/control/term#BibliographicResource" xlink:type="simple">')
        cr.write('<relationEntry>')
        title = title.replace("&", "&amp;").replace("<", "&lt;")
        title = title.replace("\"", "&quot;").replace(">", "&gt;")
        title = title.replace("\r", "&#xD;").replace("\n", "&#xA;")
        title = title.replace("\t", "&#x9;")


        cr.write(escape(title).encode('utf-8'))
        cr.write('</relationEntry>')
        cr.write('</resourceRelation>')

            
    #END Relations
    cr.write("</relations>")   
    
    #END CPF Description
    cr.write("</cpfDescription>")
    
    #END EAC
    cr.write("</eac-cpf>")
    
    return cr.getvalue()
    
def merge_record(merged_record, canonical_id=""):
    record_group = merged_record.record_group
    logging.info("creating output for %d" % (record_group.id))
    #records_query = models.Record
    eac_records = record_group.records
    viaf_info = viaf.VIAF.get_empty_viaf_dict()
    if record_group.viaf_record:
        viaf_info = viaf.getEntityInformation(record_group.viaf_record)
    person_name = record_group.name
    original_record_paths = [record.path for record in record_group.records]
    combined_record = createCombinedRecord(original_record_paths, viaf_info, r_type=record_group.g_type, canonical_id=canonical_id, maybes=merged_record.get_all_maybes())
    #print combined_record
    try:
        doc = xml.dom.minidom.parseString(combined_record)
    except Exception, e:
        doc = None
        logging.warning( combined_record )
    return doc
        
def output_all_records(start_at=0):
    merged_record = models.MergedRecord.get_all_by_ids_starting_with(id=start_at, iterate=True)
    for record in merged_record:
        doc = merge_record(record, canonical_id=record.canonical_id)
        if not doc:
            logging.warning("failed %d" %(record.id))
            continue
        fname = record.canonical_id.split("ark:/")[1].replace("/", "-")
        full_fname = os.path.join(app_config.merged, fname+".xml")
        wf = open(full_fname,"w")
        wf.write(doc.toxml(encoding="utf-8"))
        wf.flush()
        wf.close()
        logging.info("%d: %s" %(record.id, full_fname))
        
def output_all_records_loop(start_at, batch_size=1000, end_after=None):
    n = 0
    while True:
        logging.info( "retrieving new batch..." )
        merged_record = models.MergedRecord.get_all_assigned_starting_with(id=start_at, iterate=True, limit=batch_size, offset=n)
        if not merged_record:
            break
        m = 0
        for record in merged_record:
            m += 1
            doc = merge_record(record, canonical_id=record.canonical_id)
            if not doc:
                logging.warning("failed %d" %(record.id))
                continue
            if not record.canonical_id:
                raise ValueError("This record has no assigned ARK id")
            fname = record.canonical_id.split("ark:/")[1].replace("/", "-")
            full_fname = os.path.join(app_config.merged, fname+".xml")
            wf = open(full_fname,"w")
            wf.write(doc.toxml(encoding="utf-8"))
            wf.flush()
            wf.close()
            logging.info("%d: %s" %(record.id, full_fname))
        if n == n+m:
            break
        n += m
        if end_after and n >= end_after:
            break
        
def create_merged_records(start_at=0, is_fake=True, batch_size=1000):
    record_groups = models.RecordGroup.get_all_with_no_merge_record(limit=batch_size)
    for record_group in record_groups:
        merged_record = models.MergedRecord.get_by_record_group_id(record_group.id)
        if merged_record:
            continue
        merged_record = models.MergedRecord(r_type=record_group.g_type, name=record_group.name, record_data="", valid=True)
        merged_record.save()
        merged_record.record_group_id = record_group.id
        logging.info("%d: %s" %(record_group.id, merged_record.name.encode('utf-8')))
        canonical_id = merged_record.assign_canonical_id(is_fake=is_fake)
        if canonical_id:
            logging.info("minted id for %d: %s" %(merged_record.record_group_id, canonical_id))
            models.commit()
        else:
            logging.warning("failed to mint id for %d.  skipping record creation." %(merged_record.record_group_id))
        models.commit()
    return len(record_groups)

def create_merged_records_loop(start_at, is_fake=True, batch_size=1000, total_limit=None):
    n = 0
    while True:
        num_created = create_merged_records(start_at=start_at, is_fake=is_fake, batch_size=batch_size)
        n += num_created
        if not num_created:
            break
        if total_limit and n > total_limit:
            break
    

def output_record_by_ark(ark_id):
    record = models.MergedRecord.get_by_canonical_id(canonical_id=ark_id)
    doc = merge_record(record, canonical_id=record.canonical_id)
    if not doc:
        logging.warning("failed %d" %(record.id))
    fname = record.canonical_id.split("ark:/")[1].replace("/", "-")
    full_fname = os.path.join(app_config.merged, fname+".xml")
    wf = open(full_fname,"w")
    wf.write(doc.toxml(encoding="utf-8"))
    wf.flush()
    wf.close()
    logging.info("%d: %s" %(record.id, full_fname))

            
def reassign_merged_records():
    merged_records = models.MergedRecord.get_all_unassigned()
    for record in merged_records:
        record.assign_canonical_id(is_fake=True)
    models.commit()
    return merged_records

if __name__ == "__main__":
    import sys
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output_dir", help="override target directory from config/app.py [not implemented yet]")
    parser.add_argument("-i", "--id", help="only assemble for this specific ARK ID")
    parser.add_argument("-s", "--starts_at", help="start the assembly at this position", default=0, type=int)
    parser.add_argument("-r", "--real", action="store_true", help="request real ARK IDs")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-m", "--merge", action="store_true", help="create merge records; should be run before assembly")
    group.add_argument("-a", "--assemble", action="store_true", help="assemble records into files; last step")
    args=parser.parse_args()
    db_uri = db_config.get_db_uri()
    models.init_model(db_uri)
    if args.merge:
        create_merged_records_loop(args.starts_at, is_fake=True)
    elif args.assemble:
        if args.id:
            output_record_by_ark(args.id)
        else:
            logging.info( args.starts_at )
            output_all_records_loop(args.starts_at, batch_size=100, end_after=None)
 
