#!/bin/python

import xml.dom.minidom
import logging

#Functions to parse and analyze EAC-CPF files

#Parse the entity field from a eac record
# MINIDOM IS KIND OF AWFUL.  WHY DID PEOPLE PICK THIS PARSER PACKAGE - yl

import lxml.etree as etree
import snac2.utils as utils


def parseIdentity(doc):
 
    identityInformation = {}
    
    #Parse Entity id
    idEntry = doc.getElementsByTagName("recordId")
    if idEntry:
        entityId = idEntry[0].childNodes[0].nodeValue
        identityInformation['id'] = entityId
    
    #Parse Identity Information
    cpfDescription = doc.getElementsByTagName("cpfDescription")
    if cpfDescription:
        identity = cpfDescription[0].getElementsByTagName("identity")
        #Parse Entity name
        identityInformation['name_entries'] = []
        nameEntries = identity[0].getElementsByTagName("nameEntry")
        for nameEntry in nameEntries:
            part = nameEntry.getElementsByTagName("part")
            entityName = part[0].childNodes[0].nodeValue
            authorized_form = nameEntry.getElementsByTagName("authorizedForm")
            alternative_form = nameEntry.getElementsByTagName("alternativeForm")
            sources = []
            n_type = None
            if authorized_form:
                sources.append(authorized_form[0].childNodes[0].nodeValue)
                n_type = utils.NAME_ENTRY_AUTHORIZED
            elif alternative_form:
                sources.append(alternative_form[0].childNodes[0].nodeValue)
                n_type = utils.NAME_ENTRY_ALTERNATIVE
            sources = set(sources)
            if not identityInformation.get('name'):
                identityInformation['name'] = entityName #common.normalizeName(entityName)
            use_dates = nameEntry.getElementsByTagName("useDates")
            lang = nameEntry.attributes.get("xml:lang")
            if lang:
                lang = lang.value
            script_code = nameEntry.attributes.get("scriptCode")
            if script_code:
                script_code = script_code.value
            identityInformation['name_entries'].append(utils.NameEntry(name=entityName, sources=sources, n_type=n_type, use_dates=use_dates, lang=lang, script_code=script_code))
        #Parse Entity type
        entityType = identity[0].getElementsByTagName("entityType")
        entityType = entityType[0].childNodes[0].nodeValue
        identityInformation['type'] = entityType
    
    #Parse Agency
#     print "DOC,", doc.toxml()
#     maintenanceEntry = doc.getElementsByTagName("maintenanceAgency")
#     if maintenanceEntry:
#         agencyEntry = maintenanceEntry[0].getElementsByTagName("agencyName")
#         agency = agencyEntry[0].childNodes[0].nodeValue
#         identityInformation['agency'] = agency
                
    return identityInformation
# 
# 
# 
# #Parse the entity field from a eac record
# def parseIdentityRaw(doc):
# 
# 
#     #Get identity information
#     try:
#         identityInformation = {}
#         
#         #Parse Entity id
#         idEntry = doc.getElementsByTagName("recordId")
#         entityId = idEntry[0].childNodes[0].nodeValue
#         identityInformation['id'] = entityId
#         
#         cpfDescription = doc.getElementsByTagName("cpfDescription")
#         identity = cpfDescription[0].getElementsByTagName("identity")
#     
#         #Parse all Entity name entries, get raw xml preserve attributes
#         nameEntry = identity[0].getElementsByTagName("nameEntry")
#         identityInformation['name'] = nameEntry
#         part = nameEntry[0].getElementsByTagName("part")
#         entityName = part[0].childNodes[0].nodeValue
#         identityInformation['name_part'] = entityName
#         
#         #Parse Entity type
#         entityType = identity[0].getElementsByTagName("entityType")
#         entityType = entityType[0].childNodes[0].nodeValue
#         identityInformation['type'] = entityType
#                     
#         return identityInformation
#     except:
#         logging.info("ERROR: Unable to extract entity information from %s" %eac)
#         return None

#Parse exist dates
def parseExistDates(doc):
    
    try:
        #doc = xml.dom.minidom.parseString(open(eac).read())
        existDates = doc.getElementsByTagName("existDates")
        return existDates
    except:
        logging.info("ERROR: Unable to extract exist dates from %s" % doc)

def parseLanguageUsed(doc):
    return doc.getElementsByTagName("languageUsed")

#Parse Occupations
def parseOccupations(doc):
    doc = doc.getElementsByTagName("cpfDescription")
    if not doc:
        return []
    doc = doc[0].getElementsByTagName("description")
    if not doc:
        return []
    doc = doc[0]
    try:
        
        occupationsNode = doc.getElementsByTagName("occupations")
        if len(occupationsNode) > 0:
            return occupationsNode[0].getElementsByTagName("occupation")
        else:
            occupationNodes = doc.getElementsByTagName("occupation")
            return occupationNodes

    except:
        logging.info("ERROR: Unable to extract occupations from %s" % doc)
        
#Parse LocalDescriptions
def parseLocalDescriptions(doc):

    localDescNode = doc.getElementsByTagName("localDescriptions")
    if len(localDescNode) > 0:
        return localDescNode[0].getElementsByTagName("localDescription")
    else:
        localDescNodes = doc.getElementsByTagName("localDescription")
        return localDescNodes
       
#Parse places
def parsePlaces(doc):
    doc = doc.getElementsByTagName("cpfDescription")
    if not doc:
        return []
    doc = doc[0].getElementsByTagName("description")
    if not doc:
        return []
    doc = doc[0]
    try:
        placeNodes = doc.getElementsByTagName("place")
        if len(placeNodes) > 0:
            return placeNodes
        else:
        	return []
    except Exception, e:
    	raise e
    	logging.info("ERROR: Unable to extract places from %s" % doc)
    	return []

#Parse sources
def parseSources(doc):
    
    try:
        #doc = xml.dom.minidom.parse(eac)
        sources = doc.getElementsByTagName("sources")
        if len(sources) > 0:
            return sources[0].getElementsByTagName("source")
        else:
            source = doc.getElementsByTagName("source")
            if len(source) > 0:
                return [source[0]]
        
    except:
        logging.info("ERROR: Unable to extract sources from %s" % doc)

#Parse associations for a eac record, return the raw xml
def parseAssociationsRaw(doc):
    
    try:
        #doc = xml.dom.minidom.parseString(open(eac).read())
        relation = doc.getElementsByTagName("relations")
        if len(relation) >0 :
            cpfRelations = relation[0].getElementsByTagName("cpfRelation")
            return cpfRelations
        else:
            return None
    except Exception, e:
        logging.info("ERROR: Unable to extract association information from %s" % doc)
        return None
     

#Parse resource associations for a eac record, return the raw xml
def parseResourceAssociationsRaw(doc):
    
  
        #doc = xml.dom.minidom.parseString(open(eac).read().replace('localType="Leader', 'localType="http://socialarchive.iath.virginia.edu/control/term#Leader'))
    relation = doc.getElementsByTagName("relations")
    if len(relation) > 0:
        resourceRelations = relation[0].getElementsByTagName("resourceRelation")
        return resourceRelations
    else:
        return None
#     except Exception, e:
#         logging.info("ERROR: Unable to extract resource association information from %s" % doc)
#         return None


#Parse the associations from a eac record
def parseAssociations(doc):

    try:
        associations = {}   
        #doc = xml.dom.minidom.parseString(open(eac).read().replace('localType="recordId"', 'localType="http://socialarchive.iath.virginia.edu/control/term#ExtractedRecordId"').replace("http://RDVocab.info/uri/schema/FRBRentitiesRDA/", "http://socialarchive.iath.virginia.edu/control/term#"))
        relation = doc.getElementsByTagName("relations")
        if len(relation) > 0:
            cpfRelations = relation[0].getElementsByTagName("cpfRelation")
            for cpfRelation in cpfRelations:
                relatedTo = cpfRelation.getElementsByTagName("relationEntry")
                relatedTo = relatedTo[0].childNodes[0].nodeValue
                associations[relatedTo] = [cpfRelation.attributes["xlink:role"].value,cpfRelation.attributes["xlink:arcrole"].value]
            return associations
        else:
            return None
    except:
        logging.info("ERROR: Unable to extract association information from %s" % doc)
        return None

def content(tag):
    text = tag.text if tag.text else ""
    return text + ''.join(etree.tostring(e) for e in tag)
        
def parseBiogHist2(etree_doc):
    doc = etree_doc
    bioghist = doc.xpath("//*[local-name() = 'biogHist']")
    if len(bioghist) > 0:
        b_node = bioghist[0]
        biog_data = {}
        #biog_data = {"raw":etree.tostring(b_node)}
        biog_children = b_node.getchildren()
        if len(biog_children) <= 2: # a <p> and a <citation>
            #print "a:", etree.tostring(b_node)
            p_node = b_node.xpath("./*[local-name() = 'p']")
            p_node = p_node[0] if p_node else None
            #print "p:", etree.tostring(p_node)
            citation = b_node.xpath("./*[local-name() = 'citation']")
            citation = citation[0] if citation else None
            #print "c:", etree.tostring(citation)
            if citation is not None and p_node is not None:
                biog_data['citation'] = etree.tostring(citation)
                biog_data['text'] = etree.tostring(p_node)
                return biog_data # short-circuit this now
        # direct pass through
        biog_data['data'] = content(b_node)
        return biog_data
    return None
        

def parseFunctions(doc):
    functions = doc.getElementsByTagName("function")
    if len(functions) > 0:
        return functions
    else:
        return []
        

def parseDescriptions(doc):
    results = []
    cpfDescription = doc.getElementsByTagName("cpfDescription")
    if len(cpfDescription) > 0:
        cpfDescription = cpfDescription[0]
        description = cpfDescription.getElementsByTagName("description")
        if len(description) > 0:
            description = description[0]
            generalContext = description.getElementsByTagName("generalContext")
            legalStatus = description.getElementsByTagName("legalStatus")
            mandate = description.getElementsByTagName("mandate")
            structureOrGenealogy = description.getElementsByTagName("structureOrGenealogy")
            result = {"legalStatus":legalStatus, "generalContext":generalContext, "mandate":mandate, "structureOrGenealogy":structureOrGenealogy}
            result = dict((k,v) for k,v in result.iteritems() if v is not None)
            if result:
                results.append(result)
    return results
            
def parseEntityId(doc):
    cpfDescription = doc.getElementsByTagName("cpfDescription")
    if len(cpfDescription)> 0:
        entityId = cpfDescription[0].getElementsByTagName("entityId")
        if entityId:
            return entityId[0]

def pad_exist_date(date_string):
    if not date_string:
        return date_string
    date_string_pieces = date_string.split("-")
    year_position = 0
    num_pieces = len(date_string_pieces)
    if not date_string_pieces[0]:
        #empty string indicates starting with - prefix
        year_position = 1
    date_string_pieces[year_position] = date_string_pieces[year_position].zfill(4)
    if  num_pieces >= year_position+2:
        date_string_pieces[year_position+1] = date_string_pieces[year_position+1].zfill(2)
    if num_pieces >= year_position+3:
        date_string_pieces[year_position+2] = date_string_pieces[year_position+2].zfill(2)
    date_string = "-".join(date_string_pieces)
    if date_string.endswith("-00"):
        return date_string[:-3]
    return date_string
    
def extract_name_part_from_nameEntry(nameEntry):
    return nameEntry.getElementsByTagName("part")[0].childNodes[0].nodeValue

def extract_subelement_content_from_entry(entry, subelement_name):
    return entry.getElementsByTagName(subelement_name)[0].childNodes[0].nodeValue
