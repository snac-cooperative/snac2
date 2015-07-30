import string, random, uuid, datetime, time, decimal, re, collections, os, os.path, unicodedata
import urlparse, urllib, urllib2
import lxml.etree as etree, io
import xml.dom.minidom
import jellyfish

class URI(object):
    
    def __init__(self, uri):
        parsed_uri = urlparse.urlparse(uri)
        self.scheme = parsed_uri.scheme
        self.netloc = parsed_uri.netloc
        self.path = parsed_uri.path
        self.params = parsed_uri.params
        self._query = parsed_uri.query
        self.fragment = parsed_uri.fragment
    
    def get_query(self):
        return self._query
    
    def set_query(self, query):
        self._query = query
        
    query = property(get_query, set_query)
    
    def get_query_dict(self):
        return dict(urlparse.parse_qsl(self.query))
    
    def set_query_dict(self, q_dict, keep_blank_values=False):
        d = q_dict
        if not keep_blank_values:
            d = dict( [(k,v) for k,v in q_dict.items() if v])
        self._query = urllib.urlencode(d)

        
    query_dict = property(get_query_dict, set_query_dict)
    
    def __str__(self):
        return urlparse.urlunparse((self.scheme, self.netloc, self.path, self.params, self.query, self.fragment))

NAME_ENTRY_AUTHORIZED = "authorized_form"
NAME_ENTRY_ALTERNATIVE = "alternative_form"

class NameEntry(object):
    
    def __repr__(self):
        return "<NameEntry %s, %s, %s>" % (self.name.encode('utf-8'), " ".join(self.sources), self.n_type)
    
    def __init__(self, name, sources, n_type):
        super(object, self)
        self.name = name
        self.name_norm = name_entry_normalize(self.name)
        self.sources = []
        if sources:
            self.sources = sources
        self.n_type = n_type
        
        
class MergedNameEntry(object):

    def __repr__(self):
        return "<MergedNameEntry %s, %s>" % (self.name.encode('utf-8'), self.sources)
        
    def __init__(self, name, name_norm, sources):
        self.name = name
        self.name_norm = name_norm
        self.sources = {}
        if sources:
           self.sources = sources
    
    def merge(self, name_entry, name_origin=""):
        if len(name_entry.name) > self.name:
            self.name = name_entry.name
        self.merge_sources(name_entry, name_origin=name_origin)
        
    def merge_sources(self, name_entry, name_origin=""):
        for source in name_entry.sources:
            if source in self.sources:
                if name_entry.n_type == NAME_ENTRY_AUTHORIZED and self.sources[source]['n_type'] == NAME_ENTRY_ALTERNATIVE:
                    self.sources[source]["n_type"] = NAME_ENTRY_AUTHORIZED
                    self.sources[source]["name_origin"] = name_origin
            else:
                self.sources[source] = {"source":source, "n_type":name_entry.n_type, "name_origin":name_origin}
    @property
    def preferenceScore(self):
        if self.auth_source_present("LC"):
            return 99
        elif self.auth_source_present("LAC"):
            return 98
        elif self.auth_source_present("NLA"):
            return 97
        else:
            return self.n_auth_sources
    
    def auth_source_present(self, code):
        source = None
        if code in self.sources:
            source = self.sources[code]
        elif code.lower() in self.sources:
            source = self.sources[code.lower()]
        if source and source["n_type"] == NAME_ENTRY_AUTHORIZED:
            return True
        return False
    
    @property
    def n_auth_sources(self):
        n = 0
        for code in self.sources:
            if self.sources[code]["n_type"] == NAME_ENTRY_AUTHORIZED:
                n += 1
        return n
        
def prev_dir(name, nest_n=1):
    for n in range(0, nest_n):
        name = os.path.dirname(name)
    return name
    
def extract_year(s):
    return re.findall("\d+", s)

def str_clean(s):
    return s.strip()

def str_clean_and_lowercase(s):
    """Lower case + strip end white characters"""
    s = s.lower().strip()
    return s
    
def str_remove_punc(s, excepting="-", replace_with=""):
    """Remove all punctuation and digits from a string"""
    return "".join([c if (c not in string.punctuation or c in excepting) else replace_with for c in s])
    
def str_remove_punc_and_digits(s, excepting="-", replace_with=""):
    """Remove all punctuation and digits from a string"""
    return "".join([c if c not in string.punctuation and c not in string.digits or c in excepting else replace_with for c in s])

def strip_accents(s):
    if (isinstance(s, str)):
        s = s.decode('utf-8')
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn' ))

def has_n_digits(s, n=2):
    return True if re.search(r'\d{%d,}' % (n), s) else False

# source from: http://stackoverflow.com/a/93029
control_chars = ''.join(map(unichr, range(0,32) + range(127,160)))
control_char_re = re.compile('[%s]' % re.escape(control_chars))
def strip_controls(s):
    return control_char_re.sub('', s)
   
def strip_corp_abbrevs(s):
    stoplist = set(["co", "corp", "llc", "lp", "llp", "pllc", "inc", "pc", "dba", "gp", "cic", "cio", "ltd", "plc", "eg", "ag", "sa", "sas", "ptp"])
    name_norm = s.split(" ")
    name_norm = " ".join([token for token in name_norm if token not in stoplist])
    return name_norm
    
def filter_tokens(s, stoplist=["d", "b", "circa", "ca", "active", "approximately", "fl", "c", "approximate"]):
    stoplist = set(stoplist)
    tokens = s.split(" ")
    tokens = [token for token in tokens if token not in stoplist]
    return " ".join(tokens)
    
def compress_spaces(s):
    return ' '.join(s.split())
   
def normalize(s):
    return strip_accents(str_remove_punc(str_clean_and_lowercase(s)))
    
def normalize_with_space(s):
    return compress_spaces(strip_accents(str_remove_punc(str_clean_and_lowercase(s), replace_with=" ")))
    
def normalize_name_without_punc_with_space(s, lowercase=True):
    clean = str_clean
    if lowercase:
        clean = str_clean_and_lowercase
    return compress_spaces(strip_accents(str_remove_punc_and_digits(clean(s), excepting=",", replace_with=" ")))

def computeJaroWinklerDistance(x, y):
    if isinstance(x, unicode):
        x = x.encode('utf-8')
    if isinstance(y, unicode):
        y = y.encode('utf-8')
    return jellyfish.jaro_distance(x, y)

def computeSimpleRelativeLength(x, y):
    return abs((len(x)-len(y))) * 1.0 / (len(x)+len(y))

def strip_xml_ns(etree_doc):
    strip_ns='''<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" indent="no"/>

<xsl:template match="/|comment()|processing-instruction()">
    <xsl:copy>
      <xsl:apply-templates/>
    </xsl:copy>
</xsl:template>

<xsl:template match="*">
    <xsl:element name="{local-name()}">
      <xsl:apply-templates select="@*|node()"/>
    </xsl:element>
</xsl:template>

<xsl:template match="@*">
    <xsl:attribute name="{local-name()}">
      <xsl:value-of select="."/>
    </xsl:attribute>
</xsl:template>
</xsl:stylesheet>
'''
    xslt_doc=etree.parse(io.BytesIO(strip_ns))
    transform=etree.XSLT(xslt_doc)
    etree_doc=transform(etree_doc)
    return etree_doc
    

def name_entry_normalize(s):
    pat1 = re.compile(ur'[/!,"();:\.?{}\-\u00BF\u00a1<>]', flags=re.UNICODE)
    s = pat1.sub(" ", s)
    pat2 = re.compile(ur"[\[\]'']", flags=re.UNICODE)
    s = pat2.sub("", s)
    return compress_spaces(s).lower()
    
def minidom_create_element(doc, tag_name, attrs=None, text=None):
    node = doc.createElement(tag_name)
    if attrs:
        for attr in attrs.keys():
            node.setAttribute(attr, attrs[attr])
    if text:
        textNode = doc.createTextNode(text)
        node.appendChild(textNode)
    return node

# def name_entry_normalize(etree_doc):
#     norm_xslt='''<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
# <xsl:output method="xml" indent="no"/>
# 
# <xsl:template match="/">
#         <foo><xsl:value-of select="lower-case(normalize-space(replace(replace(/foo/text(), '[/!,&quot;();:\.?{}\-&#xbf;&#xa1;&lt;>]', ' '),'[\[\]'']','')))" /></foo>
#      </xsl:template>
# </xsl:stylesheet>
# '''
#     xslt_doc=etree.parse(io.BytesIO(norm_xslt))
#     transform=etree.XSLT(xslt_doc)
#     etree_doc=transform(etree_doc)
#     return etree_doc
