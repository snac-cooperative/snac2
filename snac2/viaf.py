#!/bin/python

import xml.dom.minidom
import sys
import re
import commands
import logging
import CheshirePy as cheshire
import snac2.utils as utils

import lxml.etree as etree

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d %I:%M:%S %p', level=logging.INFO)

NSMAP = {'viaf': 'http://viaf.org/viaf/terms#',
 'foaf': 'http://xmlns.com/foaf/0.1/',
 'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
 'void': 'http://rdfs.org/ns/void#'}
 
def checkNodeName(e,eName):
    if e.nodeType == e.ELEMENT_NODE and e.localName == eName:
        return True
    return False
    

class VIAF(object):

    @classmethod
    def get_empty_viaf_dict(cls):
        info = {}
    
        info['recordId'] = None
        info['mainHeadings'] = []
        info['mainHeadingsData'] = []
        info['x400s'] = []
        info['gender'] = None
        info['dates'] = None
        info['titles'] = []
        info['language'] = []
        info['nationality'] = []
        info['mainElementEl'] = []
        info['authForms'] = {}
        info['altForms'] = {}
        info['_raw'] = ""
        return info
        

def getEntityInformation(viafRecord):
    info = VIAF.get_empty_viaf_dict()
    
    if viafRecord != None:
#          try :
        if isinstance(viafRecord, unicode):
            viafRecord = viafRecord.encode('utf-8')
        doc = xml.dom.minidom.parseString(viafRecord)
        doc2 = etree.XML(viafRecord) # because I can't deal with minidom anymore -yliu
        info['recordId'] = getRecordId(doc)
        info['mainHeadings'] = getMainHeadings(doc)
        info['mainHeadingsData'] = getMainHeadings2(doc)
        #info['x400s'] = getX400s(doc)
        info['x400s'] = getX400s2(doc2)
        info['gender'] = getGender(doc)
        info['dates'] = getBirthAndDeathDates(doc)
        info['titles'] = getTitles(doc)
        info['language'] = getLanguageOfEntity(doc)
        info['nationality'] = getNationalityOfEntity(doc)
        info['mainElementEl'] = getMainHeadingEl2(doc2) # parsed element headings
        info['sources'] = getSources(doc2)
        info['authForms'], info['altForms'] = getNameEntries(doc2)
        info['_raw'] = viafRecord
#          except Exception, e:
#              logging.info("ERROR: Could not parse VIAF file")
#              raise e
#              print "Parse failure"

    return info



def getMainHeadings(doc):
    try:
        names = []
        mainHeadings = doc.getElementsByTagName("mainHeadings")
        dataList = mainHeadings[0].getElementsByTagName("data")
        for data in dataList:
            name = ''
            for child in data.childNodes:
                if checkNodeName(child, "text"):
                    name = child.childNodes[0].nodeValue
                    names.append(name)
        return names
    except:
        logging.info("ERROR: Error parsing VIAF Mainheadings")
        return []
        
def getMainHeadings2(doc):
    '''this one returns data nodes rather than names'''
    try:
        names = []
        mainHeadings = doc.getElementsByTagName("mainHeadings")
        dataList = mainHeadings[0].getElementsByTagName("data")
        for data in dataList:
            names.append(data)
        return names
    except:
        logging.info("ERROR: Error parsing VIAF Mainheadings")
        return []
    
def getX400s(doc):
    try:
        names = []
        x400s = doc.getElementsByTagName("x400s")
        if len(x400s) > 0:
            x400List = x400s[0].getElementsByTagName("x400")
            for x400 in x400List:
                name = ''
                datafield = x400.getElementsByTagName("datafield")
                subfields = datafield[0].getElementsByTagName('subfield')
                for subfield in subfields:
                    name = name + subfield.childNodes[0].nodeValue
                names.append(name)
        return names
    except:
        logging.info("ERROR: Error parsing X400s")
        return []
        
def getX400s2(doc2):
    results = []
    x400s = doc2.findall("viaf:x400s/viaf:x400", namespaces=NSMAP)
    for field in x400s:
        datafield = field.find("viaf:datafield", namespaces=NSMAP)
        subfields = datafield.findall("viaf:subfield", namespaces=NSMAP)
        result = {'tag':datafield.attrib.get("tag"), 'subfields':[]}
        for subfield in subfields:
            result['subfields'].append({'code':subfield.attrib.get("code"), 'value':subfield.text})
        results.append(result)
    return results
        
def getNationalityOfEntity(doc):
    try:
        nationality = []
        nationalIdentities = doc.getElementsByTagName("nationalityOfEntity")
        for nationalIdentity in nationalIdentities:
            data = nationalIdentity.getElementsByTagName("data")
            text = data[0].getElementsByTagName("text")
            nationality.append(text[0].childNodes[0].nodeValue)
        return nationality
    except:
        logging.info("ERROR: Error parsing National Identity of Entity")
        return []
        
def getLanguageOfEntity(doc):
    try:
        languages = []
        entityLanguages = doc.getElementsByTagName("languageOfEntity")
        for entityLanguage in entityLanguages:
            data = entityLanguage.getElementsByTagName("data")
            if len(data) > 0:
                for language in data:
                    text = language.getElementsByTagName("text")
                    if len(text) > 0:
                        languages.append(text[0].childNodes[0].nodeValue)
        return languages
    except:
        logging.info("ERROR: Error parsing Entity languages")
        return []
    
def getTitles(doc):
    try:
        titles = []
        entityTitles = doc.getElementsByTagName("titles")
        if len(entityTitles) > 0:
            datas = entityTitles[0].getElementsByTagName("data")
            for data in datas:
                text = data.getElementsByTagName("text")
                titles.append(text[0].childNodes[0].nodeValue)
        return titles
    except:
        logging.info("ERROR: Error parsing Entity titles")
        return []
 
def getBirthAndDeathDates(doc):
    try:
        birthDate = doc.getElementsByTagName("birthDate")
        birthDate = birthDate[0].childNodes[0].nodeValue
        
        deathDate = doc.getElementsByTagName("deathDate")
        deathDate = deathDate[0].childNodes[0].nodeValue
        
        return (birthDate, deathDate)
    except:
        logging.info("ERROR: Error parsing dates")
        return None           

def getGender(doc):
    try:
        fixed = doc.getElementsByTagName("fixed")
        gender = fixed[0].getElementsByTagName("gender")
        gender = gender[0].childNodes[0].nodeValue
        if gender == 'a':
            return 'Female'
        elif gender == 'b':
            return 'Male'
        else:
            return None
    except:
        logging.info("ERROR: Error parsing Entity gender")
        return None
    
def getRecordId(doc):
    try:
        viafIds = doc.getElementsByTagName("viafID")
        viafId = viafIds[0].childNodes[0].nodeValue
        return viafId
    except:
        logging.info("ERROR: Error parsing viafId")
        return None

def getMainHeadingEl2(doc):
    mainHeadings = []
 #    try:
    mainHeadingEls = doc.findall("viaf:mainHeadings/viaf:mainHeadingEl", namespaces=NSMAP)
    for mainHeadingEl in mainHeadingEls:
        source = mainHeadingEl.find("viaf:sources/viaf:s", namespaces=NSMAP)
        data = {}
        if source.text.startswith("LC"):
            lcid = mainHeadingEl.find("viaf:id", namespaces=NSMAP)
            lcid_raw = lcid.text.split("|")[1]
            lcid_raw = re.sub("\s+", " ", lcid_raw)
            lcid_parts = lcid_raw.split(" ")
            if len(lcid_parts) > 1:
                lcid_prefix = lcid_parts[0]
                lcid_number = lcid_parts[1][:2]
                lcid_suffix = lcid_parts[1][2:]
                data['lccn_wcid'] = "lccn-%s%s%s" % (lcid_prefix, lcid_number, lcid_suffix)
            else:
                data['lccn_wcid'] = "lccn-%s" % (lcid_parts[0])
            data["lccn_lcid"]=lcid_raw.replace(" ", "")
            data["source"] = "LC"
        elif source.text.startswith("WKP"):
            data["source"] = "WKP"
            url_id = mainHeadingEl.find("viaf:id", namespaces=NSMAP).text.split("|")[1]
            data["url_id"] = unicode(url_id)
        if data:
            mainHeadings.append(data)
    return mainHeadings
    
def getSources(doc):
    results = []
    sources = doc.findall("viaf:sources/viaf:source", namespaces=NSMAP)
    for source in sources:
        results.append(source.text)
    return results
    
def getNameEntries(doc2):
    # obsoletes getMainHeadings2 and getX400s2 for the purpose of getting SNAC NameEntries out of VIAF
    viaf_type = doc2.find("viaf:nameType", namespaces=NSMAP)
    viaf_type = viaf_type.text
    authorized_forms = getAuthorizedForms(doc2)
    alternative_forms = getAlternativeForms(doc2, viaf_type)
    return authorized_forms, alternative_forms
    

def getAuthorizedForms(doc2):
    authorized_forms = {}
    mainHeadingData = doc2.findall("viaf:mainHeadings/viaf:data", namespaces=NSMAP)
    for data in mainHeadingData:
        # authorized forms
        name = data.find("viaf:text", namespaces=NSMAP)
        sources = data.findall("viaf:sources/viaf:s", namespaces=NSMAP)
        if name is not None:
            name = name.text
        if sources and len(sources):
            sources = set([s.text for s in sources])
        if name and sources:
            if authorized_forms.get(name):
                authorized_forms[name].sources.update(sources)
            else:
                authorized_forms[name] = utils.NameEntry(name=name, sources=sources, n_type=utils.NAME_ENTRY_AUTHORIZED)
        else:
            logging.warning("Failed to parse VIAF mainHeading for nameEntries")
    return authorized_forms
    
    
def getAlternativeFormsCorporate(data_field, dtype, tag, sources):
    subfields = getX400_subfields(data_field)
    name = u""
    if tag != '410' and tag != "411":
        return name
    if dtype == "UNIMARC":
        if "BNF" in sources:
            # handle BNF
            if subfields.get('a'):
                name += subfields['a']
            if subfields.get('b'):
                name += u" " + subfields['b']
            suffix = []
            for code in ['c', 'g', 'd', 'e', 'f']:
                if subfields.get(code):
                    suffix.append(subfields[code])
            if suffix:
                name += u' (' + u' ; '.join(suffix) + u')'
        
        else:
            # handle UNIMARC
            for code in ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']:
                if subfields.get(code):
                    name += u" " + subfields[code]
    else:
        # treat as MARC21
        if tag == "411":
            codes = ['a', 'e', 'c', 'd', 'g', 'h', 'n', 'q']
        else:
            codes = ['a', 'b', 'c', 'd', 'e', 'g']
        name_components  = []
        for code in codes:
            if subfields.get(code):
                name_components.append(subfields[code])
                #name += u" " + subfields[code]
        if name_components:
            name = u" ".join(name_components)
    return name
    
def getAlternativeFormsPerson(data_field, dtype, tag, sources):
    subfields = getX400_subfields(data_field)
    name = u""
    if tag != '400':
        return name
    if dtype == "UNIMARC":
        if "BNF" in sources:
            # handle BNF
            if subfields.get('a'):
                name += subfields['a']
            if subfields.get('b'):
                name += u", " + subfields['b']
            if subfields.get('d'):
                name += u" " + subfields['d']
            suffix = []
            for code in ['c', 'f']:
                if subfields.get(code):
                    suffix.append(subfields[code])
            if suffix:
                name += u' (' + " ; ".join(suffix) + u')'
        else:
            # handle UNIMARC
            for code in ['a', 'b', 'c', 'd', 'f', 'g']:
                if subfields.get(code):
                    name += u" " + subfields[code]
            name = name.strip()
    else:
        # treat as MARC21
        for code in ['a', 'c', 'j', 'b', 'd', 'q']:
            if subfields.get(code):
                name += u" " + subfields[code]
        name = name.strip()
    return name
    
def getAlternativeForms(doc2, viaf_type):
    alternative_forms = {}
    x400_container = doc2.find("viaf:x400s", namespaces=NSMAP)
    if x400_container is None:
        return alternative_forms
    x400s = x400_container.findall("viaf:x400", namespaces=NSMAP)
    if not x400s:
        return alternative_forms
    for x400 in x400s:
        data_field = x400.find("viaf:datafield",  namespaces=NSMAP)
        sources = x400.findall("viaf:sources/viaf:s",  namespaces=NSMAP)
        if data_field is not None and sources is not None:
            dtype = data_field.attrib.get("dtype")
            tag = data_field.attrib.get("tag")
            subfields = getX400_subfields(data_field)
            #sources = set([s.text for s in sources])
            sources = set(["VIAF"])
            if viaf_type.lower() == "corporate":
                name = getAlternativeFormsCorporate(data_field=data_field, dtype=dtype, tag=tag, sources=sources)
            else:
                name = getAlternativeFormsPerson(data_field, dtype, tag, sources)
            if name and sources:
                if alternative_forms.get(name):
                    alternative_forms[name].sources.update(sources)
                else:
                    alternative_forms[name] = utils.NameEntry(name=name, sources=sources, n_type=utils.NAME_ENTRY_ALTERNATIVE)
    return alternative_forms

def getX400_subfields(data_field):
    subfields = data_field.findall("viaf:subfield", namespaces=NSMAP)
    result = {}
    for subfield in subfields:
        result[subfield.attrib.get("code")] = subfield.text
    return result
    
def config_cheshire(config_path=None, db="viaf"):
    if not config_path:
        import snac2.config.app as app
        config_path = app.VIAF_CONFIG
        if not config_path:
            raise ValueError("Must have a Cheshire config_path")
    cheshire.init(config_path)
    cheshire.setdb(db)
    return 
    
def query_cheshire_viaf_id(viaf_id, name_type="person", db="viaf", limit=1, index="viafid"):
    record = None
    try:
        if name_type == "person":
            name_type = "personal"
        elif name_type == "corporateBody":
            name_type = "corporate"
        searchstr = "%s '%s'" % (index, viaf_id)
        if name_type:
            searchstr += " and nametype '%s'" % (name_type)
        logging.info("searching for '%s'" % searchstr.encode('utf-8'))
        r = cheshire.Search(searchstr.encode('utf-8'))
        n = cheshire.getnumfound(r)
        logging.info ("%d records found; limit set to %d" % (n, limit))
        if n:
            record = cheshire.getrecord(r,0)
            rel = cheshire.getrelevance(r,0)
    except Exception, e:
        if r:
            cheshire.closeresult(r)
        raise e
    return record
    
    
def query_cheshire_nsid(nsid, name_type="", db="viaf", limit=1, index="xnsid"):
    record = None
    try:
        if name_type == "person":
            name_type = "personal"
        elif name_type == "corporateBody":
            name_type = "corporate"
        searchstr = "%s '%s'" % (index, nsid)
        if name_type:
            searchstr += " and nametype '%s'" % (name_type)
        logging.info("searching for '%s'" % searchstr.encode('utf-8'))
        r = cheshire.Search(searchstr.encode('utf-8'))
        n = cheshire.getnumfound(r)
        logging.info ("%d records found; limit set to %d" % (n, limit))
        if n:
            record = cheshire.getrecord(r,0)
            rel = cheshire.getrelevance(r,0)
    except Exception, e:
        if r:
            cheshire.closeresult(r)
        raise e
    return record
        

def query_cheshire_viaf(name, name_type=None, index="mainnamengram", config_path=None, db='viaf', limit=10, is_spirit=False):
    r = None
    try:
    
#         if not config_path:
#             import snac2.config.app as app
#             config_path = app.VIAF_CONFIG
#         if not config_path:
#             raise ValueError("Must have a Cheshire config_path")
#         cheshire.init(config_path)
# 
#         cheshire.setdb(db)
        if name_type == "person":
            name_type = "personal"
        elif name_type == "corporateBody":
            name_type = "corporate"
        
        searchstr = "%s '%s'" % (index, name)
        if name_type:
            searchstr += " and nametype '%s'" % (name_type)

        if is_spirit:
            searchstr = "(" +searchstr + ")" + " and spirit 1"
        else:
            searchstr = "(" +searchstr + ")" + " not spirit 1"
        logging.info("searching for '%s'" % searchstr.encode('utf-8'))
        r = cheshire.Search(searchstr.encode('utf-8'))
        records = []
        i = 0
        n = cheshire.getnumfound(r)
        logging.info ("%d records found; limit set to %d" % (n, limit))
        if limit and n > limit:
            n = limit
        while i < n:
            rec = cheshire.getrecord(r,i)
            rel = cheshire.getrelevance(r,i)
            records.append(rec)
            i += 1
        #n = cheshire.getnumfound(r)
#         
#     
#         i = 0
# 
#         while i < n :
#             rec = cheshire.getrecord(r,i)
#             rel = cheshire.getrelevance(r,i)
#             print("*********************************************")
#             print("Rank: %d Relevance: %f" % (i, rel))
#             print(rec)
#             print("*********************************************")
#             i += 1

        cheshire.closeresult(r)
    except Exception, e:
        if r:
            cheshire.closeresult(r)
        raise e
    return records
    
if __name__ == "__main__":
    viaf = query_cheshire_viaf(sys.argv[1])
    if viaf:
        viaf = viaf[0]
        E = getEntityInformation(viaf)
        print E['mainHeadings']
        print E['x400s']
