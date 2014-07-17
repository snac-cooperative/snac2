#!/bin/python

import xml.dom.minidom
import logging

#Functions to parse and analyze EAC-CPF files

#Parse the entity field from a eac record
# MINIDOM IS KIND OF AWFUL.  WHY DID PEOPLE PICK THIS PARSER PACKAGE - yl

import lxml.etree as etree
import snac2.utils as utils


def parseIdentity(doc):


    #Get identity information
    try:
        
        #doc = xml.dom.minidom.parse(eac)
        
        identityInformation = {}
        
        #Parse Entity id
        idEntry = doc.getElementsByTagName("recordId")
        entityId = idEntry[0].childNodes[0].nodeValue
        identityInformation['id'] = entityId
        
        #Parse Identity Information
        cpfDescription = doc.getElementsByTagName("cpfDescription")
        identity = cpfDescription[0].getElementsByTagName("identity")
    
        #Parse Entity name
        nameEntry = identity[0].getElementsByTagName("nameEntry")
        part = nameEntry[0].getElementsByTagName("part")
        entityName = part[0].childNodes[0].nodeValue
        identityInformation['name'] = entityName #common.normalizeName(entityName)
        
        #Parse Entity type
        entityType = identity[0].getElementsByTagName("entityType")
        entityType = entityType[0].childNodes[0].nodeValue
        identityInformation['type'] = entityType
        
        #Parse Agency
        maintanenceEntry = doc.getElementsByTagName("maintenanceAgency")
        agencyEntry = maintanenceEntry[0].getElementsByTagName("agencyCode")
        agency = agencyEntry[0].childNodes[0].nodeValue
        identityInformation['agency'] = agency
                    
        return identityInformation
    except:
        logging.info("ERROR: Unable to extract entity information from %s" %eac)
        return None


#Parse the entity field from a eac record
def parseIdentityRaw(doc):


    #Get identity information
    try:
        identityInformation = {}
        
        #Parse Entity id
        idEntry = doc.getElementsByTagName("recordId")
        entityId = idEntry[0].childNodes[0].nodeValue
        identityInformation['id'] = entityId
        
        cpfDescription = doc.getElementsByTagName("cpfDescription")
        identity = cpfDescription[0].getElementsByTagName("identity")
    
        #Parse all Entity name entries, get raw xml preserve attributes
        nameEntry = identity[0].getElementsByTagName("nameEntry")
        identityInformation['name'] = nameEntry
        part = nameEntry[0].getElementsByTagName("part")
        entityName = part[0].childNodes[0].nodeValue
        identityInformation['name_part'] = entityName
        
        #Parse Entity type
        entityType = identity[0].getElementsByTagName("entityType")
        entityType = entityType[0].childNodes[0].nodeValue
        identityInformation['type'] = entityType
                    
        return identityInformation
    except:
        logging.info("ERROR: Unable to extract entity information from %s" %eac)
        return None

#Parse exist dates
def parseExistDates(doc):
    
    try:
        #doc = xml.dom.minidom.parseString(open(eac).read())
        existDates = doc.getElementsByTagName("existDates")
        return existDates
    except:
        logging.info("ERROR: Unable to extract exist dates from %s" %eac)

#Parse Occupations
def parseOccupations(doc):
    
    try:
        
        occupationsNode = doc.getElementsByTagName("occupations")
        if len(occupationsNode) > 0:
            return occupationsNode[0].childNodes
        else:
            occupationNode = doc.getElementsByTagName("occupation")
            if len(occupationNode) > 0:
                return [occupationNode[0]]

    except:
        logging.info("ERROR: Unable to extract occupations from %s" %eac)
        
#Parse LocalDescriptions
def parseLocalDescriptions(doc):
    
    try:
        localDescNode = doc.getElementsByTagName("localDescriptions")
        if len(localDescNode) > 0:
            return localDescNode[0].childNodes
        else:
            localDescNode = doc.getElementsByTagName("localDescription")
            if len(localDescNode) > 0:
                return [localDescNode[0]]
    except:
       logging.info("ERROR: Unable to extract local descriptions from %s" %eac)

#Parse sources
def parseSources(doc):
    
    try:
        #doc = xml.dom.minidom.parse(eac)
        sources = doc.getElementsByTagName("sources")
        if len(sources) > 0:
            return sources[0].childNodes
        else:
            source = doc.getElementsByTagName("source")
            if len(source) > 0:
                return [source[0]]
        
    except:
        logging.info("ERROR: Unable to extract sources from %s" %eac)

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
        logging.info("ERROR: Unable to extract association information from %s" %eac)
        return None
     

#Parse resource associations for a eac record, return the raw xml
def parseResourceAssociationsRaw(doc):
    
    try:
        #doc = xml.dom.minidom.parseString(open(eac).read().replace('localType="Leader', 'localType="http://socialarchive.iath.virginia.edu/control/term#Leader'))
        relation = doc.getElementsByTagName("relations")
        if len(relation) > 0:
            resourceRelations = relation[0].getElementsByTagName("resourceRelation")
            return resourceRelations
        else:
            return None
    except Exception, e:
        logging.info("ERROR: Unable to extract resource association information from %s" %eac)
        return None


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
        logging.info("ERROR: Unable to extract association information from %s" %eac)
        return None

def content(tag):
    return tag.text + ''.join(etree.tostring(e) for e in tag)
                                               
#Parse the bioghist field from a eac record
def parseBiogHist(etree_doc):
    doc = etree_doc
    doc = utils.strip_xml_ns(doc)
    bioghist = doc.xpath("//*[local-name() = 'biogHist']")
    if len(bioghist) > 0:
        b_node = bioghist[0]
        biog_data = {"raw":etree.tostring(b_node)}
        paragraphs = []
        p_nodes = b_node.findall('p')
        if p_nodes:
            for n in p_nodes:
                paragraphs.append(etree.tostring(n))
            biog_data["text"] = paragraphs
        citation = b_node.find("citation")
        if citation is not None:
            biog_data["citation"] = etree.tostring(citation)
        return biog_data
    else:
        return None
        

def parseFunctions(doc):
    functions = doc.getElementsByTagName("function")
    if len(functions) > 0:
        return functions
    else:
        return []


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
