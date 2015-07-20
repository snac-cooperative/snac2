import urllib

# WARNING: This API has been discontinued.  Look for an alternative way to access ARK API features.
NOID_STAGING_API = "http://noid.cdlib.org/nd/noidu_fk4?mint+%d"
NOID_PRODUCTION_API = "http://noid.cdlib.org/nd/noidu_w6?mint+%d"
ARK_BASE_URI = "http://n2t.net/ark:/"

def mint_one(is_fake=True):
    id = mint_n(1, is_fake=is_fake)
    if id:
        return id[0]
    else:
        return ""

def mint_n(n, is_fake=True):
    api = NOID_PRODUCTION_API % (n)
    if is_fake:
        api = NOID_STAGING_API % (n)
    ids = []
    response = urllib.urlopen(api)
    if response:
        if (response.code > 300):
            raise RuntimeError("The NOID ARK API has been discontinued.")
        response = response.read()
        lines = response.split("\n")
        lines = filter(None, lines)
        for line in lines:
            print line
            line_components = line.strip().split(": ")
            ids.append(line_components[1])
        return ids
    else:
        return []
    
def get_ark_id(is_fake=True):
    noid_response = mint_one(is_fake)
    if noid_response:
        return create_full_ark_id(noid_response)
    return ""

def get_ark_ids(n, is_fake=True):
    ids = mint_n(n, is_fake=is_fake)
    if ids:
        return [create_full_ark_id(identifier) for identifier in ids]
    return ""

def create_full_ark_id(identifier):
    result = identifier
    if not identifier.startswith(ARK_BASE_URI):
        result = ARK_BASE_URI + identifier
    return result
