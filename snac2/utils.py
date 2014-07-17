import string, random, uuid, datetime, time, decimal, re, collections, os, os.path, unicodedata
import urlparse, urllib, urllib2
import lxml.etree as etree, io
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


def prev_dir(name, nest_n=1):
    for n in range(0, nest_n):
        name = os.path.dirname(name)
    return name
    
def extract_year(s):
    return re.findall("\d+", s)

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

all_chars = (unichr(i) for i in xrange(0x110000))
control_chars = ''.join(c for c in all_chars if unicodedata.category(c) == 'Cc')
# or equivalently and much more efficiently
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
    
def normalize_name_without_punc_with_space(s):
    return compress_spaces(strip_accents(str_remove_punc_and_digits(str_clean_and_lowercase(s), excepting=",", replace_with=" ")))

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
