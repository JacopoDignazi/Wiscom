import tldextract

def get_url_domain(url, add_suffix=True, add_subdomain=True):
    try:
        tld=tldextract.extract(url)
        dom=tld.domain
        if add_suffix:    dom=dom+'.'+   tld.suffix
        if add_subdomain: dom=tld.subdomain+'.'+dom

        if dom.startswith('www.'): dom=dom[4:]
        dom=dom.strip('.')
        return dom
    except:
        return 'FAILED'