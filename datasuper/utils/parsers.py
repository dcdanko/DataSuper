

def parseFileTypes(rawTypes):
    out = []
    for rtype in rawTypes:
        if isinstance(rtype, str):
            out.append({'name': rtype, 'ext': rtype})
        else:
            assert 'name' in rtype
            assert 'ext' in rtype
            out.append({'name': rtype['name'], 'ext': rtype['ext']})
    return out
