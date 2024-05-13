import datetime
import pytz
 
def get_current_time():
    ct = (datetime.datetime.now(pytz.utc))
    return str(ct).replace(' ','T').split('.')[0]+'Z'

def find_all(a_str, sub, force_find=True):
    if sub=='':
        if not force_find: return []
        print('find_all function got empty string to look for')
        assert 0
            
    where=[]
    
    start = 0
    while True:
        start = a_str.find(sub, start)
        if start == -1:
            if not len(where)>=1:
                if not force_find: return []
                print("\n\n\n not found", sub,'\n\n\n\n', a_str)
                assert 0
            return where
        where.append(start)
        start += len(sub) # use start += 1 to find overlapping matches
        
    return where

#<------------------------------------------------------------------------------- source gather
################################# setting for storing the blanked page after each iteration
STORE_BLANKED_PAGE=True

################################# settings for source cooccurrences storage
SOURCE_STORAGE=True
# if true, a dictionary of source identifier cooccurrences is computed during the process
SOURCE_STORAGE_COUNT_ONE_PER_LOCAL_CALL=True
# if true, for each revision, multiple cooccurrences of source identifier will be counted ONE time
# if false, if a source identifier cooccurred with another more time in the same page, it will be counted 

#<----------------- corresponding objects
GLOBAL_SOURCE_STORAGE={}
_LOCAL_SOURCE_STORAGE={}

################################# settings for language-wise exploration
RUN_EXPLORE_LANG_TEMPLATES=True
RUN_EXPLORE_LANG_NOSOURCE_REF_TEXT=True

#<----------------- corresponding objects
EXPLORE_LANG_TEMPLATES={}
EXPLORE_LANG_NOSOURCE_REF_TEXT={}

################################# settings for testing various functionalities
RUN_TEST_ALL=False
# this setting below tells the pipeline to store the ref statement that are not assigned a source
RUN_TEST_NOSOURCE_REF_TEXT=False

#<----------------- corresponding objects
TEST_FOUND_SOURCES_REF_TXT =set()
TEST_NOSOURCE_REF_TEXT=set()

TEST_NOT_FOUND_REFNAMES=set()
TEST_MULTIPLE_NAMED=set()

TEST_BLANKED_PAGE=''
# all <ref> statements that contain a source are blanked with '$'
# all templates outside ref statements who contain a source are blanked with '£' 
# all [bracketed texts] that contains a source outside a ref statement are blanked with '&'

TEST_TEMPLATE_NAMES_TO_DICT={}

############################################ pipeline timers and debug functionalities
DEBUG_LATEST_CALLS=[]
DEBUG_LATEST_TIME='ph'

from time import time

TEMP_TIMERS={}
TEMP_COUNTERS={}

def reset_timers():
    global TEMP_TIMERS
    TEMP_TIMERS={timer_name:0 for timer_name in TEMP_TIMERS}

def dir_timers(f='min'):
    print("  Latest run timer info:")
    print()
    for timer_name, time in TEMP_TIMERS.items():
        if f=='sec': pass
        if f=='min': time/=60
        # if f=='auto'
        if time>0.:
            print("    {:_<55}:{:_>20.6f}_{}.".format(timer_name, time, f))

def dir_counters(f='min'):
    print("  Latest run counters info:")
    print()
    for timer_name, count in TEMP_COUNTERS.items():
        if count>0.:
            print("    {:_<55}:{:_>20}_times.".format(timer_name, count))
            
def dir_performance(normalize='whole_process'):
    print("  Latest run timer info:")
    print()
    for (timer_name, time), (_,count) in zip(TEMP_TIMERS.items(), TEMP_COUNTERS.items()):
        name=timer_name[:40]
        if count>10**6   : count_f, c_f  =count/10**6, "M"
        elif count>10**3 : count_f, c_f  =count/10**3, "K"
        else             : count_f, c_f  =float(count),"_"  
        if time>60*60    : time_f , t_f  =time/60**2, 'hrs.'
        elif time>60     : time_f , t_f  =time/60   , 'min.'
        else             : time_f , t_f  =time      , 'sec.'
        
        print(f"    {name:_<42}:{count_f:_>10.3f}{c_f}_times__{time_f:_>10.3f}_{t_f}___", end='')
        if normalize!=False:
            print(f"{time/TEMP_TIMERS[normalize]*100:_>6.2f}%___", end='')
        print(f"{time/(1 if count==0 else count):_>10.5f}_sec/time")
    
def timer(timer_name='auto'):
    global TEMP_TIMERS
    global TEMP_COUNTERS

    global DEBUG_LATEST_CALLS
    global DEBUG_LATEST_TIME

    if timer_name=='auto':
        name='timer_'+str(len(TEMP_TIMERS))
    if timer_name not in TEMP_TIMERS:
        TEMP_TIMERS[timer_name]=0.
    if timer_name not in TEMP_COUNTERS:
        TEMP_COUNTERS[timer_name]=0
    def time_it(func):
        global DEBUG_LATEST_CALLS
        global DEBUG_LATEST_TIME
        
        def inner(*args, **kwargs):
            global DEBUG_LATEST_CALLS
            global DEBUG_LATEST_TIME
            
            start=time()
            to_return=func(*args, **kwargs)
            end=time()
            TEMP_TIMERS[timer_name]+=end-start
            TEMP_COUNTERS[timer_name]+=1
            
            DEBUG_LATEST_CALLS=[timer_name]+DEBUG_LATEST_CALLS[:49]
            DEBUG_LATEST_TIME =time()
            
            return to_return
        return inner 
    return time_it            

timer_whole_process         =timer('whole_process')
_                           =timer('___pipeline_english')
_                           =timer('___pipeline_other_languages')
timer_request_language_data =timer('request_language_links')
timer_save_data             =timer('saving_data')
timer_pipeline_sources      =timer('pipeline_sources')
_                           =timer('_preliminar_scan_and_selection')
_                           =timer('_request_revision_data')
_                           =timer('_comparing_revision_sources')
_                           =timer('_storing_revision_sources_all')
timer_source_extraction     =timer('_sources_extraction')
_                           =timer('___ref_sources')
timer_ref_lookup            =timer('______find_ref_text')
_                           =timer('______ref_text_disambiguation')
timer_ref_to_source         =timer('______ref_text_to_source')
_                           =timer('_________ref_structured')
_                           =timer('____________ref_template_filtering')
_                           =timer('_________ref_unstructured')
_                           =timer('___outside_sources')
_                           =timer('______ref_removal')
_                           =timer('______outside_structured')                       
_                           =timer('_________outside_template_filtering')
_                           =timer('______outside_unstructured')
timer_template_filtering    =timer('...template_filtering') 
timer_template_to_source    =timer('...template_analysis') 
timer_template_lang_match   =timer('......template_language_matching') 
timer_bracketed_lookup      =timer('...find_bracketed_text')
timer_unstructurl           =timer('...unstruct_url_retrieval')
timer_source_storage        =timer('...sources_storage')
timer_meta_functions        =timer('...meta_functions')
timer_test_functions        =timer('...test_functions')
timer_explore_functions     =timer('...explore_functions')


@timer_meta_functions
def _update_timer(name, start_time):
    global TEMP_TIMERS
    global TEMP_COUNTERS
    
    global DEBUG_LATEST_CALLS
    global DEBUG_LATEST_TIME
    
    TEMP_TIMERS[name]+=time()-start_time
    TEMP_COUNTERS[name]+=1
    
    DEBUG_LATEST_CALLS=[name]+DEBUG_LATEST_CALLS[:49]
    DEBUG_LATEST_TIME =time()

@timer_test_functions
def _update_test_found_templates(template_name, template_dict):
    if not RUN_TEST_ALL: return
    global TEST_TEMPLATE_NAMES_TO_DICT
    
    if template_name not in TEST_TEMPLATE_NAMES_TO_DICT: TEST_TEMPLATE_NAMES_TO_DICT[template_name]=set()
    TEST_TEMPLATE_NAMES_TO_DICT[template_name].update(set([p for p in template_dict.keys()])) 
    
@timer_explore_functions
def _update_explore_lang_templates(template_name, template_dict, lang):
    if not RUN_EXPLORE_LANG_TEMPLATES: return
    global     EXPLORE_LANG_TEMPLATES
    
    if lang not in EXPLORE_LANG_TEMPLATES: EXPLORE_LANG_TEMPLATES[lang]={}
        
    if template_name not in EXPLORE_LANG_TEMPLATES[lang]: 
        EXPLORE_LANG_TEMPLATES[lang][template_name]={'count':0,'fields':{}}
    EXPLORE_LANG_TEMPLATES[lang][template_name]['count']+=1
    for par_name in template_dict.keys():
        if par_name not in EXPLORE_LANG_TEMPLATES[lang][template_name]['fields']:
            EXPLORE_LANG_TEMPLATES[lang][template_name]['fields'][par_name]=0
        EXPLORE_LANG_TEMPLATES[lang][template_name]['fields'][par_name]+=1
    
@timer_explore_functions
def _update_explore_lang_nosource_ref_text(ref_text, lang):
    if not RUN_EXPLORE_LANG_NOSOURCE_REF_TEXT: return
    global     EXPLORE_LANG_NOSOURCE_REF_TEXT
    
    if lang not in EXPLORE_LANG_NOSOURCE_REF_TEXT: EXPLORE_LANG_NOSOURCE_REF_TEXT[lang]=set()
    EXPLORE_LANG_NOSOURCE_REF_TEXT[lang].update([ref_text])
    
@timer_test_functions
def _update_test_set(set_name, value):
    global TEST_FOUND_SOURCES_REF_TXT 
    global TEST_NOT_FOUND_REFNAMES
    global TEST_NOSOURCE_REF_TEXT
    global TEST_MULTIPLE_NAMED
    global TEST_BLANKED_PAGE
    
    # test set nosource_ref_text is "privileged" because can be useful across multiple pages
    # while the others, with multiple pages, can memorize overwhelming amount of data
    if RUN_TEST_ALL or RUN_TEST_NOSOURCE_REF_TEXT:
        if set_name=='nosource_ref_text':
            TEST_NOSOURCE_REF_TEXT.update([value]) 
    
    if RUN_TEST_ALL or STORE_BLANKED_PAGE:
        if set_name=='blanked_page':
            TEST_BLANKED_PAGE=value
            
    if not RUN_TEST_ALL: return

    if set_name=='found_sources_ref_text':
        TEST_FOUND_SOURCES_REF_TXT.update([value])
    elif set_name=='not_found_refnames':
        TEST_NOT_FOUND_REFNAMES.update([value])

    elif set_name=='multiple_named_ref':
        TEST_MULTIPLE_NAMED.update([value])
    else:
        print("unknown test set to update:", set_name)
        
def dir_debug_calls():
    print(f"time from latest executed call (among process we are tracking): {time()-DEBUG_LATEST_TIME} sec")
    print("list of latest calls (among process we are tracking) of function that HAD completed execution:")
    print(DEBUG_LATEST_CALLS)
        
import mwparserfromhell as mw

import re

from copy import deepcopy

from urlextract import URLExtract
URL_EXTRACTOR = URLExtract()

def fix_url(url):
    url=url.strip("{}[]|")
    if '|' in url:
        url="".join(url.split('|')[:-1])
    return url

@timer_unstructurl
def get_unstructured_urls_list(content):
    return [fix_url(url) for url in URL_EXTRACTOR.find_urls( content ) if 'www' in url or 'http' in url]


SEP_CHARS='|$|'

REF_RE     = re.compile(r'<ref(\s[^/>]*)?/>|<ref[^/>]*>[\s\S]*?</ref>', re.M|re.I)
COMMENT_RE = re.compile(r'<!--(.*?)-->')
URL_RE     = re.compile(r'\[(.*?)\]')

def get_text_comment_clear(text):
    initial_length=len(text)
    # first clears the text out of endlines (position persistent)
    for esc_char in ['\n']:
        text_cleared=text.replace(esc_char,' ')
    # then looks for comments in text withoud endlines (comment cleared)
    found_comments_cleared=set([m.group(0) for m in COMMENT_RE.finditer(text_cleared)])
    # then find the position of those comments (cleared) in the text (cleared)
    found_comments_uncleared=[]
    for comment_cleared in found_comments_cleared:
        c_ls=find_all(text_cleared, comment_cleared)
        # and uses those positions to find the comment (uncleared) in the original text
        for loc in c_ls:
            comment_uncleared=text[loc:loc+text[loc:].find('-->')+3]
            found_comments_uncleared.append(comment_uncleared)
    # finally replaces the comment with blank spaces in the original text (that includes endlines)
    for comment in found_comments_uncleared:
        text=text.replace(comment, ' '*len(comment))
    # this is because regexp does not match a comment if there is an endline inside
    # finally checks if positions are permanent
    assert len(text)==initial_length
    return text

def get_text_clear(text):
    return get_text_comment_clear(text)

def get_ref_texts(text):
    return set([m.group(0) for m in REF_RE.finditer(text)])

@timer_bracketed_lookup
def get_bracketed_texts(text):
    return set([strg.strip('[]') for strg in [m.group(0) for m in URL_RE.finditer(text)] if '[[' not in strg and strg!='[]'])

@timer_ref_lookup
def get_ref_text_to_locations(text):
    """
    requires input text cleaned of
    returns a list of ref templates texts, based on regular expression
    
    REF: https://github.com/mediawiki-utilities/python-mwrefs/blob/master/mwrefs/extract.py
    """        
    found_ref_texts=get_ref_texts(text)
    return {ref_text: find_all(text, ref_text) for ref_text in found_ref_texts}

def get_bracketed_text_to_locations(text):
    found_bracketed_texts=get_bracketed_texts(text)
    return {b_txt: find_all(text, b_txt) for b_txt in found_bracketed_texts}

@timer_template_filtering
def get_templates(text):
    return mw.parse(text).filter_templates()
        
def get_template_name(template):
    return template.name.lower().replace('\n', ' ').strip()

def get_template_dict(template):
    return {param.name.lower().replace('\n', ' ').strip(): param.value.strip() for param in template.params}

def _reset_local_source_storage():
    if not  SOURCE_STORAGE: return 
    global _LOCAL_SOURCE_STORAGE
    _LOCAL_SOURCE_STORAGE={}
    
def _reset_global_source_storage():
    if not  SOURCE_STORAGE: return 
    global GLOBAL_SOURCE_STORAGE
    GLOBAL_SOURCE_STORAGE={}
    
# @timer_source_storage
# def _update_local_source_storage(source_dict, lang, template_name=''):
#     if not  SOURCE_STORAGE: return 
    
#     global _LOCAL_SOURCE_STORAGE   

#     found_s_id=set([_get_typ_source_id(par_name, par_val, template_name, lang)
#                     for par_name, par_val in source_dict.items() 
#                     if _is_to_store(par_name, par_val, template_name, lang)])
    
#     for s_id in found_s_id:
#         if s_id not in _LOCAL_SOURCE_STORAGE: _LOCAL_SOURCE_STORAGE[s_id]={}
#         for s_id_j in found_s_id:
#             if s_id_j not in _LOCAL_SOURCE_STORAGE[s_id]: _LOCAL_SOURCE_STORAGE[s_id][s_id_j]=0
#             if SOURCE_STORAGE_COUNT_ONE_PER_LOCAL_CALL:
#                 _LOCAL_SOURCE_STORAGE[s_id][s_id_j]=1
#             else:
#                 _LOCAL_SOURCE_STORAGE[s_id][s_id_j]+=1

@timer_source_storage
def _update_local_source_storage(source_identifier_list):
    if not  SOURCE_STORAGE: return 
    
    global _LOCAL_SOURCE_STORAGE   
    
    for s_id in source_identifier_list:
        if s_id not in _LOCAL_SOURCE_STORAGE: _LOCAL_SOURCE_STORAGE[s_id]={}
        for s_id_j in source_identifier_list:
            if s_id_j not in _LOCAL_SOURCE_STORAGE[s_id]: _LOCAL_SOURCE_STORAGE[s_id][s_id_j]=0
            if SOURCE_STORAGE_COUNT_ONE_PER_LOCAL_CALL:
                _LOCAL_SOURCE_STORAGE[s_id][s_id_j]=1
            else:
                _LOCAL_SOURCE_STORAGE[s_id][s_id_j]+=1

    
@timer_source_storage
def _update_global_source_storage():
    if not  SOURCE_STORAGE: return 
    
    global _LOCAL_SOURCE_STORAGE
    global GLOBAL_SOURCE_STORAGE   
    
    for s_i in _LOCAL_SOURCE_STORAGE:
        if s_i not in GLOBAL_SOURCE_STORAGE: GLOBAL_SOURCE_STORAGE[s_i]={}
        for s_j in _LOCAL_SOURCE_STORAGE[s_i]:
            if s_j not in GLOBAL_SOURCE_STORAGE[s_i]: GLOBAL_SOURCE_STORAGE[s_i][s_j]=0
            GLOBAL_SOURCE_STORAGE[s_i][s_j]+=_LOCAL_SOURCE_STORAGE[s_i][s_j]
                
def get_global_source_storage_list():
    if not SOURCE_STORAGE:
        print("WARNING: SOURCE STORAGE MODE IS SET TO:", SOURCE_STORAGE)
        
    g_s_s_list=[]
    for s_i in GLOBAL_SOURCE_STORAGE:
        for s_j, count in GLOBAL_SOURCE_STORAGE[s_i].items():
            g_s_s_list.append((s_i, s_j, count))
    
    return g_s_s_list

# def _template_to_source_english(template_name, template_dict, mode):
#     if mode=='cite' or mode=='any' :
#         if not mode=='any' and not ('cite' in template_name or 'citation' in template_name) :
#             return
#         # if 'needed' in template_name: #<--- "full citation needed" could be treated differently
#         #     return
            
#         # the get(...,'') is to ignore empty string values
#         if template_dict.get('doi','') != '':
#             return _get_typ_source_id('doi', template_dict['doi'], template_name)
#         if template_dict.get('archive-url','') != '':
#             return _get_typ_source_id('archive-url', template_dict['archive-url'], template_name)
#         if template_dict.get('url','') != '':
#             return _get_typ_source_id('url', template_dict['url'], template_name)
#         if template_dict.get('chapter-url','') != '':
#             return _get_typ_source_id('chapter-url', template_dict['chapter-url'], template_name)
#         if 'cite q' in template_name and template_dict.get('1','') !='': # cite q templates
#             return _get_typ_source_id('1', template_dict['1'], template_name)
#         if template_dict.get('isbn','') != '':
#             return _get_typ_source_id('isbn', template_dict['isbn'], template_name)
#         if template_dict.get('issn','') != '':
#             return _get_typ_source_id('issn', template_dict['issn'], template_name)
#         if template_dict.get('title','') != '':
#             return _get_typ_source_id('title', template_dict['title'], template_name)
#         elif not mode=='any':
#             return
        
#     if mode=='webarchive' or mode=='any':
#         if mode=='any' or template_name=='webarchive':
#             if template_dict.get('archive-url','')!='' : 
#                 return _get_typ_source_id('archive-url', template_dict['archive-url'], template_name)
#             elif template_dict.get('url','')!='' :       
#                 return _get_typ_source_id('url', template_dict['url'], template_name)
        
#     if mode=='harv' or mode=='any':
#         if mode=='any' or template_name=='harv' or template_name=='harvnb' or template_name=='harvid':
#             pass
        
#     if mode=='sfn' or mode=='any':
#         if mode=='any' or template_name=='sfn':
#             pass
        
# _TEMPLATE_PARAMETER_NAMES_TO_STORE={'en':['unstructurl','url','doi', 'archive-url', 'chapter-url', 'isbn', 'issn', 'title']}
# def _is_to_store(par_name, par_val, template_name, lang):
#     """
#     decides if a parameter (field) value in a template is to store, depending on the template name, field name and language 
#     """
#     if par_val=='': return False
    
#     if par_name in _TEMPLATE_PARAMETER_NAMES_TO_STORE['en']: 
#         return True
#     if par_name in _TEMPLATE_PARAMETER_NAMES_TO_STORE.get(lang, []):
#         return True
    
#     # templates field names accepted DEPENDING on template name
#     if template_name!=None:
#         if 'cite q' in template_name and par_name=='1':
#             return True
        
#     return False

# _TEMPLATE_PARAMETER_NAME_TO_TYPE={'generic':{}}
# def _get_typ_source_id(par_name, par_val, template_name, lang): #not doing anything with lang
    
#     ################################################# mapping field name to a category name (called typ/type)
#     # <-------------------------------------------- field names occurring in english named templates
#     # <------------------ cases that depend on BOTH template name and field name
#     if template_name=='webarchive':
#         if par_name=='url':     par_name='archive'
#     if 'cite q' in template_name:
#         if par_name=='1':       par_name='citeq'
#     # <------------------ cases that depend only on field name
#     if par_name=='archive-url': par_name='archive'
#     if par_name=='chapter-url': par_name='chapter'
        
#     # <-------------------------------------------- field names occurring in other languages
#     # <------------------ cases that depend on language and BOTH template name and field name
#     # more complicated than is worth
#     # <------------------ cases that depend on language and only on field 
#     if   par_name in _TEMPLATE_FIELD_NAME_TO_TYPE[lang]:
#          par_name=   _TEMPLATE_FIELD_NAME_TO_TYPE[lang][par_name]
#     elif par_name in _TEMPLATE_FIELD_NAME_TO_TYPE['generic']: 
#          par_name=   _TEMPLATE_FIELD_NAME_TO_TYPE['generic'][par_name]

#     return SEP_CHARS.join([par_name,par_val])

_TEMPLATE_NAME_WORDS_FOR_CITATION=[
    # (en) english and generic
    'cite','citation','url'
    # (it) italian and similar
    'cita','citazione',
    # (es) spanish and similar
    'cita', 'citación', 'citada',
    # (de) german and similar
    'quelle', 'literatur',
    # (fr) french and similar
    'lien','ouvrage','article',
    # (pt) portuguese and similar
    'citar',
    # (hr) croatian and similar
    'citiranje',
    # (cs) ceco and similar
    'citace', 
]
_TEMPLATE_NAME_WORDS_FOR_ARCHIVE=[
    # english and generic
    'webarchive', 'wayback',
    # german and similar
    'webarchiv',
    # franch and similar
    'lien archive',
]
# _LANG_THAT_WE_WANT_TO_RECOGNIZE_TEMPLATE_CATEGORY=['en','it','es','de', 'fr', 'pt', 'hr', 'cs'] deprecated
def match_template_name_to_mode(lang, template_name, mode):
    """
    needed because in a ref statement there can be more templates that contain source identifiers:
    - case 1: 2 different citation templates ---> 2 different sources
    - case 2: a citation template and other stuff ---> 1 only source
    - case 3: other stuff ---> maybe one citation
    so we need to check FIRST teplate citations: if any, then return those
    if none, return what can be found in the others (es. a webarchive alone)
    
    this whole process is not needed when we are analysing templates outside ref statements,
    because in that case there is no need to disambiguate between different source identifiers containers
    in that case, mode='any' is good enough
    """
#     if not lang in _LANG_THAT_WE_WANT_TO_RECOGNIZE_TEMPLATE_CATEGORY: return True
    # shortcut for languages for which we don't have a template translation analysis
    # basically makes so that the templates inside ref statements are always analysed in mode=any
    # in that way the name of that template does not matter for languages outside the ones above
    # so that identifier will be looked for only among the template parameters names, 
    # regardles of the template name. 
    # Undesider effetc is that ref statements can yeld more than one source identifier
    # without considering if those are two actual different citation on just like citation + webarchive templates
    # EDIT: since the ref content is then elaborated with mode='any'
    # then it is worth to try matching it anyway at the beginning
    # so that maybe it catches english and similar languages citation names
    
    if mode=='any': return True
    
    if mode=='cite':
        for citation_word in _TEMPLATE_NAME_WORDS_FOR_CITATION:
            if  citation_word in template_name and not 'archive' in template_name:
                return True
    
    if mode=='webarchive':
        for archive_word in  _TEMPLATE_NAME_WORDS_FOR_ARCHIVE:
            if archive_word in template_name:
                return True
    
#     if mode=='harv':
#         if lang=='en':
#             if template_name=='harv' or template_name=='harvnb' or template_name=='harvid':
#                 return True
            
#     if mode=='sfn':
#         if lang=='en':
#             if template_name=='sfn':
#                 return True
    return False

############################################################
# ----------------- notes on matching precision
# for the last kind of entry (word across all languages)
# and for the second kind of entry with lang='en'
# it can be important to check
# whether other languages have that same specific word
# used in fields of templates BUT with other meaning
# example maybe 'doi' in ukranian means author, who tf knows
############################################################
# ----------------- MISSING LANGUAGES THAT STILL ARE ON TOP 10
# arabian (ar) - 50/50 uses english templates 
# chinese (zu) - still mostly uses english templates
# russian (ru) - 50/50 uses english templates
# ----------------- not on top ten but close enough
# japanese (ja) -apparently uses only english templates
############################################################
# allowed entries are 
# (lang, t_name, f_name)
# (lang, -,      f_name)
# (- ,   -,      f_name)
_TEMPLATE_TRIPLETS_TO_SOURCE_TYPE={
    #-------------------- english and generic
    ('en','cite doi', '1'):'doi',
    ('en','citedoi', '1'):'doi',
    ('en','doi', '1'):'doi',
    
    ('en','webarchive','url'):'archive',
    ('en','wayback','url'):'archive',
    
    ('en','url','1'):'url',
    
    ('en','cite q','1'):'citeq',
    ('en','citeq','1'):'citeq',

    ('en','-','doi'):'doi',
    
    ('en','-','archive'):'archive',
    ('en','-','archive-url'):'archive',
    ('en','-','archiveurl'):'archive',
    ('en','-','url-archive'):'archive',
    ('en','-','urlarchive'):'archive',
    
    ('en','-','url'):'url',
    ('en','-','article-url'):'url',
    ('en','-','articleurl'):'url',
    
    ('en','-','section-url'):'chapter',
    ('en','-','sectionurl'):'chapter',
    ('en','-','chapter-url'):'chapter',
    ('en','-','chapterurl'):'chapter',
    
    ('en','-','dead-url'):'url',
    ('en','-','deadurl'):'url',
    ('en','-','bibcode'):'bibcode',
    ('en','-','isbn'):'isbn',
    ('en','-','issn'):'issn',
    ('en','-','title'):'title',
    ('en','-','trans-title'):'title',
    ('en','-','transtitle'):'title',

    
    ##################################################
    # other languages translation follow this pattern:
    # translation for archive 
    # translation for chapter 
    # translation for dead url 
    # translation for title 
    ##################################################
    
    #-------------------- italian and similar
    ('-','-', 'urlarchivio'):'archive',
    # chapter: same as english
    ('-','-', 'urlmorto'):'url',
    ('-','-', 'titolo'):'title',
    ('-','-', 'titolotradotto'):'title',
    
    #-------------------- spanish (es) and similar
    ('-','-', 'urlarchivo'):'archive',
    ('-','-', 'url-capítulo'): 'chapter',
    # dead urls: same as english
    ('-','-', 'título'):'title',
    ('-','-', 'títulotrad'):'title',
    ('-','-', 'título-trad'):'title',
    
    #-------------------- german (de) and similar
    ('-','literatur','online'):'url',
    # ignoring specific case by archive id in https://de.wikipedia.org/wiki/Vorlage:Webarchiv
    ('-','webarchiv', 'url'): 'archive',
    ('-','-', 'archiv-url'):'archive',
    # chapter: same as english
    # dead urls: same as english
    ('-','-', 'titel'):'title',
    
    
    #-------------------- franch (fr) and similar
    ('-','lien archive', 'url'): 'archive',
    # chapter: same as english 
    ('-','-', 'lien brisé'):'url',
    ('-','-', 'titre'):'title',
    ('-','-', 'traduction titre'):'title',
    
    #-------------------- portuguese (pt) and similar
    ('-','-', 'arquivourl'):'archive',
    # chapter: same as english 
    ('-','-', 'urlmorta'):'url',
    ('-','-', 'titulo'):'title',
    ('-','-', 'titulotrad'):'title',
    ('-','-', 'titulo-trad'):'title',
    
    #-------------------- croatian (hr) and similar
    # uses citiranje template names but (apparently) english parameter names
    
    #-------------------- Ceco (cs) and similar
    ('-','-', 'url archivu'):'archive',
    # chapter: not found
    # dead url: same as english
    ('-','-', 'titul'): 'title',
    
    # other, generic
    ('-' ,'-','unstructurl'):'unstructurl',

}

TT2ST=_TEMPLATE_TRIPLETS_TO_SOURCE_TYPE
@timer_template_lang_match
def _get_source_identifier_type(lang, template_name, par_name, par_value):
    if par_value=='': return 'invalid'
    
    found_t= TT2ST.get( (lang, template_name, par_name), '') 
    if found_t!='': return found_t
    
    found_t= TT2ST.get( (lang,      '-',      par_name), '') 
    if found_t!='': return found_t
    
    found_t= TT2ST.get( ('-' ,      '-',      par_name), '') 
    if found_t!='': return found_t
    
    return 'unknown'

_PREFERENCE_ORDER_IDENTIFIER_TYPE={t:n for n, t in enumerate(
    ['doi','archive', 'url', 'chapter', 'bibcode', 'citeq','isbn', 'issn', 'title'])}
def get_best_source_identifier(found_source_identifiers):
    if not len(found_source_identifiers): return None
    
    best_t_value=len(_PREFERENCE_ORDER_IDENTIFIER_TYPE)+2
    for ts in found_source_identifiers:
        t, s = ts.split(SEP_CHARS)
        t_value=_PREFERENCE_ORDER_IDENTIFIER_TYPE[t]
        if t_value<best_t_value:
            best_t_value=t_value
            best_ts=ts
    return best_ts
    
    
@timer_template_to_source    
def template_to_source(lang, template, mode='any', store_this=True):
    template_name=get_template_name(template)
    template_dict=get_template_dict(template)
    
    # saves data about this template, for testing, exploration and debugging purposes
    if store_this:
        _update_test_found_templates(  template_name, template_dict)
        _update_explore_lang_templates(template_name, template_dict, lang)
    
    # looks for all possible source identifiers,
    # matching language-template_name-parameter_name triplets 
    # to the corresponding identifier type
    found_source_identifiers=[]
    for par_name, par_value in template_dict.items():
        source_identifier_type=_get_source_identifier_type(lang, template_name, par_name, par_value)
        if source_identifier_type in ['unknown','invalid']:
            # if it does not find it for the current language, looks for the english version of those
            source_identifier_type=_get_source_identifier_type('en', template_name, par_name, par_value)
        if source_identifier_type not in ['unknown','invalid']:
            found_source_identifiers.append(source_identifier_type+SEP_CHARS+par_value)
            
    # calls storage of cooccurrences of identifiers,
    # adding an entry of a link for each identifier found (including self links)
    if store_this: 
        _update_local_source_storage(found_source_identifiers)  

    # check if the language-template_name pair is matching the required mode
    # NB this need to be after every operation of updating for testing/storage and so on
    # because if store_this=True, the storage needs to happend even if the match is not correct with required mode
    # this is important because the first call of a template inside a ref statement is always in mode 'cite'
    # and that first call scans all the templates to look for citation ones, and also store them
    if not match_template_name_to_mode(lang, template_name, mode):
        # if it is not, tries the english-template pair
        if not match_template_name_to_mode('en', template_name, mode):
            return
        
    # picks the best identifier (if any had been found) by a preference sorting
    best_source_identifier=get_best_source_identifier(found_source_identifiers)
    # and returns it (None otherwise)
    return best_source_identifier
    

@timer_ref_to_source
def ref_text_to_sources(ref_text, lang):
    """
    takes a text inside a <ref> statement, returns a list of source found inside
    structured refs: first looks at templates, returns every valid source found - one for each valid template
    unstructured refs: if nothing is found it retrieves sources from raw and returns the list of all the one found
    returns empty list if nothing is found
    
    NB about the code: every case is distinguished for clarity and testing
    """
    
    found_sources=[]
    
    ###################################################### STRUCTURED REFS:
    s_start=time()
    
    found_templates=get_templates(ref_text)
    _update_timer('____________ref_template_filtering', s_start)
    
    #<--------------------------- looking for cite/citation template (possibly more than one)  
    for found_template in found_templates:
        source=template_to_source(lang, found_template, mode='cite', store_this=True)
        if source!=None:
            found_sources.append(source)
    if len(found_sources)>=1: 
        _update_timer('_________ref_structured', s_start)
        return found_sources 

    #<--------------------------- special cases: no source found inside cite/citation template but other template present
    #<------------ webarchive template
    for found_template in found_templates: #store_this=False because templates have been already scanned
        source=template_to_source(lang, found_template, mode='webarchive', store_this=False)
        if source!=None: 
            found_sources.append(source)
    if len(found_sources)>=1: 
        _update_timer('_________ref_structured', s_start)
        return found_sources 
    
#     #<------------ harv/harvnb template
#     for found_template in found_templates:
#         source=template_to_source(lang, found_template, mode='harv', store_this=False)
#         if source!=None: 
#             found_sources.append(source)
#     if len(found_sources)>=1: 
#         _update_timer('_________ref_structured', s_start)
#         return found_sources 

    #<------------ whatever it can find, as long as it has some valid identifier type in its parameter names
    for found_template in found_templates:
        source=template_to_source(lang, found_template, mode='any', store_this=False)
        if source!=None: 
            found_sources.append(source)
    if len(found_sources)>=1: 
        _update_timer('_________ref_structured', s_start)
        return found_sources 
    
    ###################################################### UNSTRUCTURED REFS: 
    # no template found OR found only template with no source  
    start=time()
        
    # this will store input text while clearing it of the sections that were already analysed
    remaining_ref_text=deepcopy(ref_text) 
    for found_template in found_templates:
        remaining_ref_text=remaining_ref_text.replace(f'{found_template}', '')
    
    #<------------------------------------- looks for url inside [brackets]
    unstructured_urls=[]
    for b_text in get_bracketed_texts(remaining_ref_text): #not using locations
        remaining_ref_text=remaining_ref_text.replace(b_text,'')
        unstructured_urls+=get_unstructured_urls_list(b_text)
    #<------------ if none is found inside brackets, looks at the rest of the text
    if not len(unstructured_urls):  
        unstructured_urls+=get_unstructured_urls_list(remaining_ref_text)
        
    #<------------ at least one url found 
    if len(unstructured_urls):
         #one url for each ref statement (first valid)
        _update_local_source_storage(['unstructurl'+SEP_CHARS+unstructured_urls[0]]) #watch out this is just [0]
        _update_timer('_________ref_unstructured',start)
        return ['unstructurl'+SEP_CHARS+unstructured_urls[0]] 
    #<------------ no url found inside brackets
    _update_test_set('nosource_ref_text', ref_text)
    _update_explore_lang_nosource_ref_text(ref_text, lang)
    _update_timer('_________ref_unstructured',start)
    return [] 
    
    
    
@timer_source_extraction
def get_sources2locations(text, lang):
    """
    takes wikicode text and returns a list of all valid sources found inside <ref> statements
    """
    
    sources2loc={}
    
    ######################################################## sources inside ref statemets
    start=time()
    
    # find comments here, then replace them with blank spaces (mantain characters offsets to get correct location)
    text=get_text_clear(text)
    
    # extract references and location
    ref_text_to_locations=get_ref_text_to_locations(text)
    
    ref_named={}
    ref_link={}
    ref_unnamed=[]
    
    # recognizing reference structure
    s_start=time()
    for ref_text, locations in ref_text_to_locations.items(): 
        if '<ref name' in ref_text:
            raw_name=ref_text[ref_text.find('=')+1:ref_text.find('>')]
            name=raw_name.replace('"','').replace("'",'').replace('/','').strip(' ') 
            #wikicode reference names are case sensitive?
            if '</ref>' in ref_text: 
                if name in ref_named:
                    _update_test_set('multiple_named_ref', ref_text) 
                    #if name is already present, it just store the value using the raw name instead of cleaned name
                    ref_named[raw_name]=(ref_text, locations)
                ref_named[name]=(ref_text, locations)
            else:
                ref_link [name]=(ref_text, locations)
        else:                        
            ref_unnamed.append( (ref_text, locations) )
    _update_timer('______ref_text_disambiguation',s_start)
   
    #<-------------- named sources   <ref name="blabla"> ... </ref>
    name2source={}
    for name, (ref_text, locations) in ref_named.items():
        name2source[name]=ref_text_to_sources(ref_text, lang)
        for source in name2source[name]:
            _update_test_set('found_sources_ref_text', ref_text)
            sources2loc['ref_named'+SEP_CHARS+source]= locations

    #<-------------- unnamed sources  <ref> ... </ref>
    for ref_text, locations in ref_unnamed:
        found_sources=ref_text_to_sources(ref_text, lang)
        for source in found_sources:
            _update_test_set('found_sources_ref_text', ref_text)
            sources2loc['ref_unnamed'+SEP_CHARS+source]= locations  
    
    #<-------------- linked sources  <ref name="blabla"/>
    for name, (ref_text, locations) in ref_link.items():
        if not name in name2source:   
            #ONLY HAPPENDS FOR AN ERROR ON EDITOR SIDE
            # or if we are using rev diffs, it might be that diff texts do not contain the named reference
            # so in that case, code must be added here to look for it in the whole content page
            _update_test_set('not_found_refnames', name)
            continue
        for source in name2source[name]:
            _update_test_set('found_sources_ref_text', ref_text)
            sources2loc['ref_link'+ SEP_CHARS+source]=locations
    
    _update_timer('___ref_sources',start)
    ######################################################## sources outside ref statemets and in templates 
    start=time()
    
    s_start=time()
    blanked_text=text
    for ref_text in ref_text_to_locations.keys():
        # removing every <ref> statement in text (so that following code do not double check)
        blanked_text=blanked_text.replace(ref_text, '$'*len(ref_text))
    _update_timer('______ref_removal',s_start)
    
    global TMP_DEBUG_BLANKED_TEXT
    TMP_DEBUG_BLANKED_TEXT=blanked_text

        
    s_start=time()  
    found_templates=get_templates(blanked_text)
    _update_timer('_________outside_template_filtering',s_start)
    
    found_outside_template_texts=[]
    for template in found_templates:
        template_text=f'{template}'
        if template_text in found_outside_template_texts: continue
        # it can happend that the exact template text occurrs more than once in the text
        source=template_to_source(lang, template, mode='any', store_this=True)
        if source!=None:
            found_outside_template_texts.append(template_text)
            t_locs=find_all(blanked_text, template_text)
            sources2loc['outside'+SEP_CHARS+source]=t_locs
             
    _update_timer('______outside_structured',start)
    ######################################################## sources outside ref statemets inside brackets (no template) 
    u_start=time()
    
    for template_text in found_outside_template_texts:
        blanked_text=blanked_text.replace(template_text, '£'*len(template_text))    
    bracketed_texts2locs=get_bracketed_text_to_locations(blanked_text)
    
    for b_text, b_locs in bracketed_texts2locs.items(): 
        outside_unstruct_urls=get_unstructured_urls_list(b_text)
        for o_url in outside_unstruct_urls:
            blanked_text=blanked_text.replace(b_text, '&'*len(b_text)) 
            _update_local_source_storage(  ['unstructurl'+SEP_CHARS+o_url])
            sources2loc['outside'+SEP_CHARS+'unstructurl'+SEP_CHARS+o_url]=b_locs
                                               
    _update_timer('______outside_unstructured',u_start)
    _update_timer('___outside_sources',start)
    _update_test_set('blanked_page', blanked_text)
    
    return sources2loc

#---------------------------------------------- utils api + urls
import requests

from time import sleep

N_RESPONSE_EXCEPTIONS=0
N_RESPONSE_EXCEPTIONS_LL=0

def request_page_revision_metadata(TITLE, LANG='en', verbose=True, verbose_prefix=''):
    global N_RESPONSE_EXCEPTIONS
    
    session = requests.Session()

    BASE_URL = f"http://{LANG}.wikipedia.org/w/api.php"
        
    PARAMS = {
        "action": "query",
        "prop": "revisions",
        "format": "json",
        "titles": TITLE,
        "rvprop": "ids|timestamp|user|userid|content|flags|comment",
        "rvslots": "main",
        "rvdir": "newer", 
        "rvlimit":"max",
        # "formatversion": "2",
        "continue": ""

    }
    
    METADATA=[]
    
    while True:
        REQUEST_TIME=get_current_time()
        
        while True:
            try:
                _response = session.get(url=BASE_URL, params=PARAMS, timeout=30)
                break
            except Exception as e:
                print("\nresponse exception in revisions request...")
                print(e)
                N_RESPONSE_EXCEPTIONS+=1
                sleep(10)
        
        try:
            response_data = _response.json()
            response_pages = response_data["query"]["pages"]
        except Exception as e:
            print("Error in decoding response:")
            print(e)
            print(response_data)
            assert 0 
        
        for page_id, page_data in response_pages.items():
            if not 'revisions' in page_data:
                print(f'revisions not found in response data for page {TITLE}({LANG}); response data:\n')
                print(response_data)
                return []
                      
            for rv in page_data['revisions']:
                t_rv_dict={
                    'page':TITLE,
                    'page_id':page_id, 
                    'lang': LANG, 
                    'timestamp_query': REQUEST_TIME,
                    'revid':str(rv.get('revid')), 
                    'revid_parent':str(rv.get('parentid')),
                    'timestamp':rv.get('timestamp'), 
                    'comment':rv.get('comment'),
                    'flags':rv.get('flags'),
                    'user': str(rv.get('user')),
                    'user_id':str(rv.get('userid')),
                }
                METADATA.append(deepcopy(t_rv_dict))
            
            
            if verbose:
                print(f"{verbose_prefix}; page {TITLE} ({LANG}); preliminarly scanned {len(METADATA)} revisions", end='\r')
                    
        if 'continue' in response_data:
            PARAMS['continue'] = response_data['continue']['continue']
            PARAMS['rvcontinue'] = response_data['continue']['rvcontinue']
        else:
            return METADATA

GLOBAL_COUNT_SKIPPED =0
GLOBAL_COUNT_RETAINED=0

def revision_metadata_to_selected(metadata):
    global GLOBAL_COUNT_SKIPPED
    global GLOBAL_COUNT_RETAINED
    
    last_user=''
    REV_TO_RETAIN=[]
    REV_TO_SKIP  =[]
    
    for rev_meta in metadata:
        rev_user=rev_meta['user_id']+'|$|'+rev_meta['user']
        rev_id=rev_meta['revid']
        
        if rev_user==last_user:
            GLOBAL_COUNT_SKIPPED+=1
            
            #<----- adding last entry of revision id to retain to the ones to skip
            REV_TO_SKIP.append(REV_TO_RETAIN[-1])
            #<----- replacing last entry in revision id to retain with current id
            REV_TO_RETAIN[-1]=rev_id
            
            last_user=rev_user
            continue
        
        GLOBAL_COUNT_RETAINED+=1
        
        REV_TO_RETAIN.append(rev_id)
        last_user=rev_user
    return REV_TO_RETAIN, REV_TO_SKIP

DEBUG_LATEST_PAGE={}

@timer_pipeline_sources
def get_sources_revisions(TITLE, LANG, verbose=True, verbose_prefix='', return_all_revisions=False):
    global DEBUG_LATEST_PAGE
    global N_RESPONSE_EXCEPTIONS
    
    # preliminar operations: retrieving metadata and selecting revisions to study
    pre_timer=time()
    METADATA=request_page_revision_metadata(TITLE, LANG, verbose=verbose, verbose_prefix=verbose_prefix)
    REV_TO_RETAIN, _REV_TO_SKIP = revision_metadata_to_selected(METADATA) 
    EXT_SEL_REV=len(REV_TO_RETAIN)
    EXT_TOT_REV=len(REV_TO_RETAIN)+len(_REV_TO_SKIP)
    # there could be a difference from ext_tot_rev to actually retrieved below, 
    # if a revision happends DURING data collection
    _update_timer('_preliminar_scan_and_selection', pre_timer)
    
    REQUEST_TIME=get_current_time()

    # setting up the requests
    session = requests.Session()
    
    BASE_URL = f"http://{LANG}.wikipedia.org/w/api.php"
        
    PARAMS = {
        "action": "query",
        "prop": "revisions",
        "format": "json",
        "titles": TITLE,
        "rvprop": "ids|timestamp|user|userid|content|flags|comment",
        "rvslots": "main",
        "rvdir": "newer", 
        "rvlimit":"max",
        # "formatversion": "2",
        "continue": ""

    }
    latest_s2ls={}
    
    REV_DATA_ALL=[]
    REV_DATA=[]
    VALID_REV_DATA=[]
    
    N_RV_TOT=0
    N_RV_SEL=0
    
    _reset_global_source_storage()
    while True:
#         REQUEST_TIME=get_current_time()
# moved this after preliminar scan
        
        r_time=time()
        while True:
            try:
                _response = session.get(url=BASE_URL, params=PARAMS, timeout=30)
                break
            except Exception as e:
                print("\nresponse exception in revisions request...")
                print(e)
                N_RESPONSE_EXCEPTIONS+=1
                sleep(10)
        _update_timer('_request_revision_data',r_time)
        
        try:
            response_data = _response.json()
            response_pages = response_data["query"]["pages"]
        except Exception as e:
            print("Error in decoding response:")
            print(e)
            print(response_data)
            assert 0 
        
        for page_id, page_data in response_pages.items():
            if not 'revisions' in page_data:
                print(f'revisions not found in response data for page {TITLE}({LANG}); response data:\n')
                print(response_data)
                return {'sources_revision_edits':[], 
                        'sources_revision_edits_valid_rev':[],
                        'sources_current': [],
                        'sources_storage': [],
                        'pages_revdata': [], 
                        'sources_revision_all':[]
                       }
                      
            for rv in page_data['revisions']:
                N_RV_TOT+=1

                # if id is not present in revisions selected from preliminar analysis: continues with the next one
                if not str(rv.get('revid')) in REV_TO_RETAIN: continue
                    
                N_RV_SEL+=1
                t_rv_dict={
                    'page':TITLE,
                    'page_id':page_id, 
                    'lang': LANG, 
                    'timestamp_query': REQUEST_TIME,
                    'revid':str(rv.get('revid')), 
                    'revid_parent':str(rv.get('parentid')),
                    'timestamp':rv.get('timestamp'), 
                    'comment':rv.get('comment'),
                    'flags':rv.get('flags'),
                    'user': rv.get('user'),
                    'user_id':rv.get('userid'),
                    'source_use':'placeholder',
                    'source_type':'placeholder',
                    'source':'placeholder',
                    'prev_locs':'placeholder',
                    'curr_locs':'placeholder',
                    'a': 0,
                    'r': 0,
                    'n_rev_valid':N_RV_SEL
                }
                
                t_rv_valid={
                    'page':TITLE,
                    'page_id':page_id, 
                    'lang': LANG, 
                    'timestamp_query': REQUEST_TIME,
                    'revid':str(rv.get('revid')), 
                    'revid_parent':str(rv.get('parentid')),
                    'timestamp':rv.get('timestamp'),
                    'comment':rv.get('comment'),
                    'flags':rv.get('flags'),
                    'user': rv.get('user'),
                    'user_id':rv.get('userid'),
                    'n_rev_valid':N_RV_SEL
                }
                VALID_REV_DATA.append(deepcopy(t_rv_valid))
                
                slots=rv.get('slots')
                if slots== None: continue
                main=slots.get('main')
                if main == None: continue
                content= main.get('*')
                if content == None: continue
                    
                DEBUG_LATEST_PAGE={'page':TITLE, 'page_id':page_id, 'lang':LANG, 'revid': str(rv.get('revid')), 'content':content}
                    
                _reset_local_source_storage()
                # local source storage gets reset for every revision
                this_s2ls =get_sources2locations( content, LANG )
                # updates the global source storage with sources collected in this revision
                _update_global_source_storage() 
                
                # organizes information about all sources in this revision (if required)
                if return_all_revisions:
                    a_time=time()
                    for s, locs in this_s2ls.items():
                        REV_DATA_ALL.append({
                            'page':TITLE,
                            'page_id':page_id, 
                            'lang': LANG, 
                            'timestamp_query': REQUEST_TIME,
                            'revid':str(rv.get('revid')), 
                            'revid_parent':str(rv.get('parentid')),
                            'timestamp':rv.get('timestamp'), 
                            'comment':rv.get('comment'),
                            'flags':rv.get('flags'),
                            'user': str(rv.get('user')),
                            'user_id':str(rv.get('userid')),
                            'source_use':s.split(SEP_CHARS)[0],
                            'source_type':s.split(SEP_CHARS)[1],
                            'source':s.split(SEP_CHARS)[2],
                            'locations':locs
                        })
                    _update_timer('_storing_revision_sources_all', a_time)
                
                # organizes information about all sources EDITS in this revision
                s_time=time()
                _added_sources  ={s for s,ls in this_s2ls.items()   if len(ls) > len(latest_s2ls.get(s,[]))}
                _removed_sources={s for s,ls in latest_s2ls.items() if len(ls) > len(this_s2ls.get(s, []))}
                for s in _added_sources:
                    t_rv_dict['source_use'], t_rv_dict['source_type'], t_rv_dict['source']=s.split(SEP_CHARS)
                    t_rv_dict['prev_locs']=latest_s2ls.get(s,'[]')
                    t_rv_dict['curr_locs']=this_s2ls.get(s,'[]')
                    t_rv_dict['a'] =len(this_s2ls[s])-len(latest_s2ls.get(s, []))
                    REV_DATA.append(deepcopy(t_rv_dict))
                t_rv_dict['a']=0 #<--- reset value before remove counter
                
                for s in _removed_sources:
                    t_rv_dict['source_use'], t_rv_dict['source_type'], t_rv_dict['source']=s.split(SEP_CHARS)
                    t_rv_dict['prev_locs']=latest_s2ls.get(s,'[]')
                    t_rv_dict['curr_locs']=this_s2ls.get(s,'[]')
                    t_rv_dict['r'] =len(latest_s2ls[s])-len(this_s2ls.get(s, []))
                    REV_DATA.append(deepcopy(t_rv_dict))
                t_rv_dict['r']=0# <---- (not really necessary here)
                
                latest_s2ls=deepcopy(this_s2ls)
                _update_timer('_comparing_revision_sources', s_time)
                


                if verbose:
                    print(f"{verbose_prefix}; page {TITLE} ({LANG}); scanned {N_RV_SEL} revisions of {EXT_SEL_REV}/{EXT_TOT_REV}; collected", len(REV_DATA),"sources revisions...", end='\r')

        if 'continue' in response_data:
            PARAMS['continue'] = response_data['continue']['continue']
            PARAMS['rvcontinue'] = response_data['continue']['rvcontinue']
            
        else:
            # list of dictionaries are resource intensive 
            # for no reason other than having column names easily accessible
            CURR_PAGE_DATA=[{'page': TITLE,
                             'page_id': page_id, 
                             'lang':LANG, 
                             'source_use':s.split(SEP_CHARS)[0], 
                             'source_type':s.split(SEP_CHARS)[1],
                             'source':s.split(SEP_CHARS)[2],
                             'locations':ls,
                             'timestamp_query':REQUEST_TIME
                            } for s, ls in latest_s2ls.items()]
            
            source_storage=get_global_source_storage_list()
            PAGE_SOURCE_STORAGE=[{'page': TITLE,
                                  'page_id':page_id,
                                  'lang':LANG,
                                  'source_id_i':s_i,
                                  'source_id_j':s_j,
                                  'coocc_count':count
                                 } for s_i, s_j, count in source_storage]
            
            PAGES_REVDATA=[{'page': TITLE,
                            'page_id':page_id,
                            'lang':LANG,
                            'n_revisions_tot':N_RV_TOT,
                            'n_revisions_sel':N_RV_SEL,
                            'n_sources_revisions':len(REV_DATA)}
                          ]
            break
            
    to_return={'sources_revision_edits':REV_DATA, 
               'sources_revision_edits_valid_rev':VALID_REV_DATA,
               'sources_current': CURR_PAGE_DATA,
               'sources_storage': PAGE_SOURCE_STORAGE,
               'pages_revdata': PAGES_REVDATA
              }
    
    if return_all_revisions:
        to_return['sources_revision_all']=REV_DATA_ALL
        
    return to_return

import pandas as pd

@timer_save_data
def save_rv_data(src_rv_data, path_output, name_suffix, mode, header):    
    to_save=['sources_revision_edits', 'sources_revision_edits_valid_rev','sources_current','sources_storage','pages_revdata']
    if src_rv_data.get('sources_revision_all',[])!=[]: to_save.append('sources_revision_all')
        
    for data_name in to_save:
        pd.DataFrame(src_rv_data[data_name])\
        .to_csv(f'{path_output}{data_name}{name_suffix}.csv', header=header, mode=mode, index=False) 

@timer_request_language_data
def get_language_links_data(EN_PAGE_TITLE):
    global DEBUG_LATEST_PAGE
    global N_RESPONSE_EXCEPTIONS_LL
    
    ll_session = requests.Session()

    BASE_URL = "http://en.wikipedia.org/w/api.php"

    LL_DATA=[]

    LL_PARAMS = {
        "action": "query",
        "prop": "langlinks",
        "format": "json",
        "titles": EN_PAGE_TITLE,
        # "rvprop": "ids|timestamp|user|content",
        # "rvslots": "main",
        # "rvdir": "newer", 
        # "limit":"1",
        # "formatversion": "2",
        "continue": ""
    }
    
    N_RESPONSE_EXCEPTIONS_LL=0
    while True:
        while True:
            try:
                _response_ll = ll_session.get(url=BASE_URL, params=LL_PARAMS, timeout=30)
                break
            except Exception as e:
                print("\nresponse exception in language link request...")
                print(e)
                sleep(10)
                N_RESPONSE_EXCEPTIONS_LL+=1
                
        ll_response_data = _response_ll.json()
        # print(response_data)
        for page in ll_response_data['query']['pages'].values():
            if not 'langlinks' in page:
                print(f"langlinks not found for request of page {EN_PAGE_TITLE}")
                print("full response data:\n")
                print(ll_response_data)
                print()
                return LL_DATA
            
            LL_DATA+=page['langlinks']
        if 'continue' in ll_response_data:
            LL_PARAMS['continue'] = ll_response_data['continue']['continue']
            LL_PARAMS['llcontinue'] = ll_response_data['continue']['llcontinue']
        else:
#             print()
            break
            
    return LL_DATA