from paths import get_path_collected, get_path_urlsinfo, get_path_processed
import os

import pandas as pd
import numpy as np

import pickle

from tqdm.notebook import tqdm

from copy import deepcopy

from datetime import datetime, timedelta

def timestr_to_datetime(date_str, date_format='%Y-%m-%dT%H:%M:%SZ'):
    return datetime.strptime(date_str, date_format) 

def ts_to_url(ts, doi_as_url=True):
    """
    maps every url type to specific url type.
    If doi_as_url the doi is mapped to url|$|https://doi.org/{doi_code}
    else doi|$|{doi_code} is returned
    """
    t, s =ts.split('|$|')
    
    if not t in ['doi', 'unstructurl', 'url', 'archive', 'chapter']:
        return 'NOTURL|$|'+s
    
    if t=='doi':
        if doi_as_url:
            return 'url|$|https://doi.org/'+s
        else:
            return ts
        
    return 'url|$|'+s #<--- converts unstructurl, archive, chapter to type url
        
    
def get_df_column_url_ts(df, doi_as_url=True):
    """
    uses ts_to_url propriety to generate a new dataframe column with those composition rules
    returns a column which is compatible to formatting of TS_TO_EQUIVALENT_TS
    suggest doi_as_url=True so that every entry is an actual url
    """
    ts=df['source_type']+'|$|'+df['source']
    ts=ts.map(lambda x: ts_to_url(x, doi_as_url=doi_as_url))
    return ts

PROJECT_NAME_TO_SHORT={
    'Climate change':'CL',
    'COVID-19': 'CV',
    'Media_sample':'MED',
    'History_sample':'HIS',
    'Biology_sample':'BIO',
    'Aggregated': 'AGG',
    'Custom':'CST'
}

PROJECT_SHORT_TO_NAME={s: n for n, s in PROJECT_NAME_TO_SHORT.items()}
   
def project_short_to_name(project_name):
    return PROJECT_SHORT_TO_NAME[project_name]

def language_short_to_name(language):
    if language=='en':
        return 'english'
    else:
        return 'other_languages'
    
def get_df_column_equivalent_url(df, _TS_TO_EQUIVALENT_TS, name='url'):
    """
    needs the output of get_df_column_url as input
    """

    return df[name].map(lambda x: _TS_TO_EQUIVALENT_TS.get(x, x).split('|$|')[-1])


######## NB
# things done inside load_revision_data that could be done later
#   - folders for preprocessed data are created in load_revision_data BEFORE preprocessing
#        this means that some processed/PROJECT/LANG/ folders could remain empty
#        this issue can be solved just by encasing the loading of processed data in a try: except
#   - paths and name for output of preprocessed data are given by load_revisions_data
#        while for AGG dataset they are created in process_dataset
#        this is not a big issue but it is confusing, while at the moment it works
# just do not want to change it now because it works and everything was already generated with these rules
# but it makes sense to change it for code readability

def load_revision_data(PROJECT_NAME, LANG, FOLDER_OUTPUT_PROCESSED='processed'):#, USE_SCORES=False):
    """
    load revision data
    selects history of sources identified by urls (remove title, etc...)
    maps urls/doi to equivalent urls
    merge with urlsinfo (final domain, perennial status, status of the link)
    
    returns a bunch of things:
    URLSRV, URLS_INFO, INV_SCORES, CONTRO_SCORES_FOUND, OUTPUT_NAMES
    """
    
    print("Managing paths and names")    
    ################################################ get input names from given shorthands
    assert PROJECT_NAME in PROJECT_SHORT_TO_NAME
#     assert LANG in ALL_LANG not really mandatory
    
    collect_name_project =project_short_to_name(PROJECT_NAME)
    collect_name_language=language_short_to_name(LANG)
        
    ################################################ folder management
    INPUT_FOLDER=get_path_collected(collect_name_project, collect_name_language)
    INPUT_FOLDER_URLSINFO=get_path_urlsinfo()
    
    OUTPUT_FOLDER=get_path_processed(PROJECT_NAME, LANG)
    
    if FOLDER_OUTPUT_PROCESSED!='processed':
        OUTPUT_FOLDER=OUTPUT_FOLDER.replace('processed', FOLDER_OUTPUT_PROCESSED)
    
    # Check if the folder exists
    if not os.path.exists(OUTPUT_FOLDER):
        # If it doesn't exist, create it
        os.makedirs(OUTPUT_FOLDER)
        print(f"  Folder '{OUTPUT_FOLDER}' created")
    else:
        print(f"  Folder '{OUTPUT_FOLDER}' found")
        # since preprocessing can fail later, this means it can create a folder that remains empty

    print("Loading urls revision data")
    ################################################ Load urls revisions data
    URLSRV=pd.read_csv(INPUT_FOLDER+'/sources_revision_edits.csv', on_bad_lines='warn')
    
    URLSRV=URLSRV[URLSRV['lang']==LANG]

    URLSRV['page']=URLSRV['lang']+'&'+URLSRV['page'] #to mantain different lang page separated

    ################################################ manage equivalent groups
    with open(INPUT_FOLDER+f'ts_to_equivalent.pickle', 'rb') as handle:
        TS_TO_EQUIVALENT_TS=pickle.load(handle)
        
    URLSRV['url_ts']=get_df_column_url_ts(URLSRV)
    URLSRV['url']   =get_df_column_equivalent_url(URLSRV, TS_TO_EQUIVALENT_TS, name='url_ts')

    print("Loading contro scores data")
    ################################################ Load contro scores data
    try:
        INV_SCORES=pd.read_csv(INPUT_FOLDER+'contro_scores.csv')
        CONTRO_SCORES_FOUND=True
    except:
        print(f"  CONTRO SCORES NOT FOUND for project {PROJECT_NAME} and language {LANG}. Creating a dummy dataframe")
        CONTRO_SCORES_FOUND=False
        INV_SCORES=pd.DataFrame(columns=['page', 'page_id', 'lang', 'timestamp_query', 'revid', 'revid_parent',
           'timestamp', 'comment', 'flags', 'user', 'user_id', 'source_use',
           'source_type', 'source', 'involved_count', 'involved_count_normalized',
           'n_rev_valid', 'url_ts', 'url'])
    
    INV_SCORES=INV_SCORES[INV_SCORES['lang']==LANG]
    INV_SCORES['page']=INV_SCORES['lang']+'&'+INV_SCORES['page']

    INV_SCORES['url_ts']=get_df_column_url_ts(INV_SCORES)
    INV_SCORES['url']   =get_df_column_equivalent_url(INV_SCORES, TS_TO_EQUIVALENT_TS, name='url_ts')

    print("Loading urls info")
    ################################################ Load urls info
    URLS_INFO=pd.read_csv(INPUT_FOLDER_URLSINFO+'urls_info_and_domain.csv')
    URLS_INFO=URLS_INFO.dropna(subset=['final_domain'])
    assert len(URLS_INFO[URLS_INFO.final_domain.isna()])==0
    URLS_INFO=URLS_INFO.drop_duplicates(subset='url')

    print("Data management")
    ################################################# select actual urls from dataset
#     print(len(URLSRV))
    URLSRV=URLSRV[URLSRV.url.isin(URLS_INFO.url)]
    URLSRV=URLSRV.reset_index(drop=True)
#     print(len(URLSRV))

#     print(len(URLS_INFO))
    URLS_INFO=URLS_INFO[URLS_INFO.url.isin(URLSRV.url)]
#     print(len(URLS_INFO))

#     print(len(INV_SCORES))
    INV_SCORES=INV_SCORES[INV_SCORES['url'].isin(URLS_INFO.url)]
    INV_SCORES=INV_SCORES.reset_index(drop=True)
#     print(len(INV_SCORES))

    # merge the two
    URLSRV=URLSRV.merge(URLS_INFO[['url', 'status_code', 'reason', 'final_domain', 'perennial_status']].set_index('url'),
                        how='left', left_on='url', right_on='url')#.drop_duplicates(subset='index') 

    #################################################### select specific language (if required)
        
    OUTPUT_NAMES={
        'url': f'{OUTPUT_FOLDER}url2info_{PROJECT_NAME}{LANG}.csv',
        'dom': f'{OUTPUT_FOLDER}dom2info_{PROJECT_NAME}{LANG}.csv'
    }

    return URLSRV, URLS_INFO, INV_SCORES, CONTRO_SCORES_FOUND, OUTPUT_NAMES

def select_lifewindow(page, lw, MIN_DATE, p2initrev):
    if MIN_DATE==None:
        return lw
    if lw['start']>MIN_DATE:
        return lw
    if lw['end']<MIN_DATE:
        return None
    lw['start']=MIN_DATE
    lw['start_nrev']=p2initrev[page]
    
    return lw

def process_dataset(PROJECT_NAME, LANG, MIN_DATE=None, MAX_DATE=None, FOLDER_OUTPUT_PROCESSED='processed', ALLOW_SAVE=True, ALLOW_REPLACE=False):
    if MIN_DATE!=None:
        # MIN DATE must be formatted as %Y-%m-%d
        MIN_DATE=timestr_to_datetime(MIN_DATE, date_format='%Y-%m-%d')
    if MAX_DATE!=None:
        # MIN DATE must be formatted as %Y-%m-%d
        MAX_DATE=timestr_to_datetime(MAX_DATE, date_format='%Y-%m-%d')
    
    ############################################################ LOAD DATA
    URLSRV=pd.DataFrame()
    URLS_INFO=pd.DataFrame()
    INV_SCORES=pd.DataFrame()
    CONTRO_SCORES_FOUND=True
#     DATASET_NAMES=[]

    for project_name, lang in zip(PROJECT_NAME, LANG):    
        print('-'*20, project_name, lang)
        T_URLSRV, T_URLS_INFO, T_INV_SCORES, T_CONTRO_SCORES_FOUND, T_OUTPUT_NAMES= \
        load_revision_data(project_name, lang, FOLDER_OUTPUT_PROCESSED)

        URLSRV=pd.concat([URLSRV, T_URLSRV])
        URLS_INFO=pd.concat([URLS_INFO, T_URLS_INFO])
        INV_SCORES=pd.concat([INV_SCORES, T_INV_SCORES])

        CONTRO_SCORES_FOUND=CONTRO_SCORES_FOUND and T_CONTRO_SCORES_FOUND 
        #is false when at least one of the datasets does not have contro scores

#         DATASET_NAMES.append(f'{PROJECT_NAME}{LANG}')
        print()
        print()
        
    #------------------------------------------- make output pathnames
    if len(PROJECT_NAME)>1:
        output_folder=get_path_processed('AGG',LANG[0])
        if FOLDER_OUTPUT_PROCESSED!='processed':
            output_folder=output_folder.replace('processed', FOLDER_OUTPUT_PROCESSED)
            
        PATHNAME_URL=output_folder+'url2info_'+'AGG'+LANG[0]+'.csv'
        PATHNAME_DOM=output_folder+'dom2info_'+'AGG'+LANG[0]+'.csv'
        
        # Check if the folder exists 
        # NB this is done inside load_data... function for single topic-language datasets
        # so here the T_OUTPUT_NAMES returned by that function is not used 
        # a bit confusing but it works and I'm not going to change it now

        
        if not os.path.exists(output_folder):
            # If it doesn't exist, create it
            os.makedirs(output_folder)
            print(f"  Folder '{output_folder}' created")
        else:
            print(f"  Folder '{output_folder}' found")
    else:
        PATHNAME_URL=T_OUTPUT_NAMES['url']
        PATHNAME_DOM=T_OUTPUT_NAMES['dom']
        
    if not ALLOW_REPLACE:
        if os.path.exists(PATHNAME_URL) and os.path.exists(PATHNAME_DOM):
            print(f"PROJECT {PROJECT_NAME}, LANGUAGE {LANG} already processed. Skipping")
            return 1
                      
    print("data will be saved in:")
    print("    ", PATHNAME_URL) 
    print("    ", PATHNAME_DOM)
   
    ########################################################### converts time strings to datetime
    URLSRV['timestamp']=URLSRV['timestamp'].map(timestr_to_datetime)
    URLSRV['timestamp_query']=URLSRV['timestamp_query'].map(timestr_to_datetime)

    ########################################################### cut by max date
    if MAX_DATE!=None:
        URLSRV=URLSRV[URLSRV['timestamp']<MAX_DATE]
    
    print()
    ########################################################### check that the data is not empty
    if len(URLSRV)==0:
        return 5
    
    ########################################################### extract pages lifetime
    GB_URLSRV=URLSRV.groupby('page')

    P2TIME=[]
    for page in tqdm(URLSRV.page.unique(), total=len(URLSRV.page.unique()), desc='analysis of lifetimes of pages'):
        page_df=GB_URLSRV.get_group(page)

        highest_date= page_df['timestamp_query'].max()
        lowest_date = page_df['timestamp'].min()
        if MIN_DATE!=None:
            lowest_date=max(MIN_DATE, lowest_date)


        highest_nrev= page_df['n_rev_valid'].max()
        lowest_nrev=1
        if MIN_DATE!=None:
            lowest_nrev=page_df[page_df['timestamp']>=MIN_DATE]['n_rev_valid'].min()
            if np.isnan(lowest_nrev): #only triggers if last edit happened before minimum allowed date
                lowest_nrev=highest_nrev

        P2TIME.append({
            'page':page,
            'initial_date': lowest_date,
            'initial_rev': lowest_nrev,
            'query_date': highest_date,
            'lifetime':(highest_date-lowest_date).days,
            'lifetime_edits':(highest_nrev-lowest_nrev)+1
        })
        
    p2time=pd.DataFrame(P2TIME).set_index('page')['lifetime'].to_dict()
    p2nrev=pd.DataFrame(P2TIME).set_index('page')['lifetime_edits'].to_dict()
    p2initrev=pd.DataFrame(P2TIME).set_index('page')['initial_rev'].to_dict()
    
    #------------------------------------------- dataset global counts
    TOT_PAGES=len(p2time)

    TOT_DAYS=0
    for p, t in p2time.items():
        TOT_DAYS+=t
    TOT_NREV=0
    for p, t in p2nrev.items():
        TOT_NREV+=t
        
    print("TOT_PAGES ", TOT_PAGES)
    print("TOT_DAYS ", TOT_DAYS)
    print("TOT_NREV ", TOT_NREV)
    
    ############################################# selection of urlsrv ABOVE MIN DATE
    # will be useful for computing user activity on min_date-cutoff dataset
    if MIN_DATE!=None:
        SEL_URLSRV=URLSRV[URLSRV['timestamp']>MIN_DATE]
    else:
        SEL_URLSRV=URLSRV
    
    TOT_EDITORS=len(SEL_URLSRV['user'].unique())
    TOT_REGISTEREDEDITORS=len(SEL_URLSRV[SEL_URLSRV['user_id']!=0]['user'].unique())
    print("TOT_EDITORS ", TOT_EDITORS)
    print("TOT_REGISTEREDEDITORS ", TOT_REGISTEREDEDITORS)
    
    ###########################  saving metadata of the collection 
    # statistics of collected data from which features are extracted, 
    # so it is statistic of data BEFORE preprocessing
    if len(PROJECT_NAME)>1:
        output_pathname_pickle=get_path_processed('AGG',LANG[0])+'collected_stats_'+'AGG'+LANG[0]+'.pkl'
    else:
        output_pathname_pickle=get_path_processed(PROJECT_NAME[0], LANG[0])+'collected_stats_'+PROJECT_NAME[0]+LANG[0]+'.pkl'
        
    if FOLDER_OUTPUT_PROCESSED!='processed':
        output_pathname_pickle=output_pathname_pickle.replace('processed', FOLDER_OUTPUT_PROCESSED)
        
    data = {
        'TOT_PAGES': TOT_PAGES,
        'TOT_DAYS': TOT_DAYS,
        'TOT_NREV': TOT_NREV,
        'TOT_EDITORS': TOT_EDITORS,
        'TOT_REGISTEREDEDITORS': TOT_REGISTEREDEDITORS
    }
    with open(output_pathname_pickle, 'wb') as f:
        pickle.dump(data, f)
    print(f"Dictionary saved as pickle file at: {output_pathname_pickle}")
    
    
    print()
    ########################################################### url-page-wise analysis
    GB_URLSRV=URLSRV.groupby('page')

    pu2time_dc={}
    for page in tqdm(URLSRV.page.unique(), total=len(URLSRV.page.unique()), desc='url timeline analysis'):
        page_df=GB_URLSRV.get_group(page)
        page_max_n_rev=page_df.n_rev_valid.max()
        gb_page_df=page_df.groupby('url')
        for url in page_df.url.unique():
            url_page_df=gb_page_df.get_group(url)
            page_and_url_id=f'{page}${url}'

            url_page_df=url_page_df.sort_values(by='timestamp') #<-- from older to newer
            pu2time_dc[page_and_url_id]=[]
            for _, row in url_page_df.iterrows():
                pu2time_dc[page_and_url_id].append({
                    'time': row.timestamp,
                    'delta_count':row.a-row.r,
                    'n_rev':row.n_rev_valid,
                    'user':row.user,
                    'user_id': row.user_id
                })
            # last one is a dummy entry to state the last observation of the page (delta count is zero)
            pu2time_dc[page_and_url_id].append({
                'time':row.timestamp_query,
                'delta_count':0,
                'n_rev':page_max_n_rev
            })
            
    ########################################################### dom-page-wise analysis
    GB_URLSRV=URLSRV.groupby('page')

    pD2time_dc={}
    for page in tqdm(URLSRV.page.unique(), total=len(URLSRV.page.unique()), desc='domain timeline analysis'):
        page_df=GB_URLSRV.get_group(page)
        page_max_n_rev=page_df.n_rev_valid.max()
        gb_page_df=page_df.groupby('final_domain')
        for dom in page_df.final_domain.unique():
            dom_page_df=gb_page_df.get_group(dom)
            page_and_dom_id=f'{page}${dom}'

            dom_page_df=dom_page_df.sort_values(by='timestamp') #<-- from older to newer
            pD2time_dc[page_and_dom_id]=[]
            for _, row in dom_page_df.iterrows():
                pD2time_dc[page_and_dom_id].append({'time': row.timestamp,
                                                    'delta_count':row.a-row.r,
                                                    'n_rev':row.n_rev_valid,
                                                    'user':row.user,
                                                    'user_id':row.user_id
                                                   })
            # last one is a dummy entry to state the last observation of the page (delta count is zero)
            pD2time_dc[page_and_dom_id].append({'time':row.timestamp_query,
                                                'delta_count':0,
                                                'n_rev':page_max_n_rev
                                               })
            
    # ---------------------------------------- control 
    for pu, time_dc in pu2time_dc.items():
        count=0
        for tdc in time_dc:
            dc=tdc['delta_count']
            count+=dc
        if count<0:
            print(pu, time_dc)
            assert 0
    for pD, time_dc in pD2time_dc.items():
        count=0
        for tdc in time_dc:
            dc=tdc['delta_count']
            count+=dc
        if count<0:
            print(pD, time_dc)
            assert 0
            
    ########################################################### url-page-wise extracting deltas            
    pu2dt_dc={}
    for pu, time_dc in pu2time_dc.items():
        pu2dt_dc[pu]=[]# list of {'delta_time':'ph', 'delta_count':'ph'}
        for n_t, tdc in enumerate(time_dc):
            time=tdc['time']
            nrev=tdc['n_rev']
            dc  =tdc['delta_count']
            user=tdc.get('user')
            user_id=tdc.get('user_id')
            if n_t==0:
                previous_time =time
                previous_nrev =nrev
                pu2dt_dc[pu].append({'delta_time':'start', 
                                     'delta_nrev':'start', 
                                     'delta_count':dc,
                                     'user': user, 'user_id': user_id
                                    })
                continue
            pu2dt_dc[pu].append({'delta_time':time-previous_time, 
                                 'delta_nrev':nrev-previous_nrev, 
                                 'delta_count':dc,
                                 'user': user, 'user_id': user_id
                                })
            previous_time=time
            previous_nrev =nrev
        
    #-------------------------------------------------------------- dom-page-wise extracting deltas            
    pD2dt_dc={}
    for pD, time_dc in pD2time_dc.items():
        pD2dt_dc[pD]=[]# list of {'delta_time':'ph', 'delta_count':'ph'}
        for n_t, tdc in enumerate(time_dc):
            time=tdc['time']
            nrev=tdc['n_rev']
            dc  =tdc['delta_count']
            user=tdc.get('user')
            user_id=tdc.get('user_id')
            if n_t==0:
                previous_time =time
                previous_nrev =nrev
                pD2dt_dc[pD].append({'delta_time':'start', 
                                     'delta_nrev':'start', 
                                     'delta_count':dc,
                                     'user': user, 'user_id': user_id
                                    })
                continue
            pD2dt_dc[pD].append({'delta_time':time-previous_time, 
                                 'delta_nrev':nrev-previous_nrev, 
                                 'delta_count':dc,
                                 'user': user, 'user_id': user_id
                                })
            previous_time=time
            previous_nrev =nrev
            
    ########################################################### url-page-wise extracting time-count            
    pu2tc={}
    for (pu, dtdc_line), (_, tdc_line) in zip(pu2dt_dc.items(), pu2time_dc.items()):
        pu2tc[pu]=[]
        assert len(dtdc_line)==len(tdc_line)
        for n_entry, (dtdc, tdc) in enumerate(zip(dtdc_line, tdc_line)):
            time=tdc['time']
            nrev=tdc['n_rev']
            dt=dtdc['delta_time']
            dr=dtdc['delta_nrev']
            dc=dtdc['delta_count']
            user=tdc.get('user')
            user_id=tdc.get('user_id')
    #         if pu==test_pu: print(n_entry, tdc, dtdc)
    #         if pu==test_pu: print(pu2tc[pu])
            if dt=='start':
                count=dc
                pu2tc[pu].append({'time':time, 
                                  'nrev':nrev,
                                  'count':count,
                                  'user':user, 'user_id':user_id
                                 })
                # previous_d_c=d_c
                # continue
            elif dt.days==0: #<---- if this revision is closer than a day to previous revision
                pu2tc[pu][-1]['time']  =time #<---- previous one gets AGGREGATED with this one
                pu2tc[pu][-1]['nrev']  =nrev
                pu2tc[pu][-1]['count']+=dc
                # the aggregated revision is attributed to the LAST author who changed the revision count
                # comment two lines below to attribute to the FIRST
    #             pu2tc[pu][-1]['user']  =user
    #             pu2tc[pu][-1]['user_id']=user_id

                # previous_d_c=pu2tl[pu][-1]['count']
            else:
                pu2tc[pu].append({'time':time, 
                                  'nrev':nrev, 
                                  'count':previous_count+dc,
                                  'user':user, 'user_id':user_id
                                 })
            previous_count=pu2tc[pu][-1]['count']
    #         if pu==test_pu: print(pu2tc[pu], end='\n\n\n')

    #-------------------------------------------------------------- dom-page-wise extracting time-count            
    pD2tc={}
    for (pD, dtdc_line), (_, tdc_line) in zip(pD2dt_dc.items(), pD2time_dc.items()):
        pD2tc[pD]=[]
        assert len(dtdc_line)==len(tdc_line)
        for dtdc, tdc in zip(dtdc_line, tdc_line):
            time=tdc['time']
            nrev=tdc['n_rev']
            dt=dtdc['delta_time']
            dr=dtdc['delta_nrev']
            dc=dtdc['delta_count']
            user=tdc.get('user')
            user_id=tdc.get('user_id')
    #         if pu==test_pu: print(n_entry, tdc, dtdc)
    #         if pu==test_pu: print(pu2tc[pu])
            if dt=='start':
                count=dc
                pD2tc[pD].append({'time':time, 
                                  'nrev':nrev,
                                  'count':count,
                                  'user':user, 'user_id':user_id
                                 })
                # previous_d_c=d_c
                # continue
            elif dt.days==0: #<---- if this revision is closer than a day to previous revision
                pD2tc[pD][-1]['time']  =time #<---- previous one gets AGGREGATED with this one
                pD2tc[pD][-1]['nrev']  =nrev
                pD2tc[pD][-1]['count']+=dc
                # the aggregated revision is attributed to the LAST author who changed the revision count
                # comment two lines below to attribute to the FIRST
    #             pD2tc[pD][-1]['user']  =user
    #             pD2tc[pD][-1]['user_id']=user_id

                # previous_d_c=pu2tl[pu][-1]['count']
            else:
                pD2tc[pD].append({'time':time, 
                                  'nrev':nrev, 
                                  'count':previous_count+dc,
                                  'user':user, 'user_id':user_id
                                 })
            previous_count=pD2tc[pD][-1]['count']
            
    ########################################################### url-page-wise extracting timewindows            
    pu2lifewindows={}
    for n, (pu, tc_line) in enumerate(pu2tc.items()):
        pu2lifewindows[pu]=[]
        TOGGLE_LIFETIME_START=False
        for n_t, tc in enumerate(tc_line):
            try:
                assert tc['count']>=0 #just to be sure
            except:
                print(pu, tc_line)
                break

            if not TOGGLE_LIFETIME_START: #<--- starts a lifetime even if the count is zero (zero length lifetimes considered)
                 #     unless it is the last element of the list (which is a dummy entry)
                if n_t+1!=len(tc_line) or n_t+1==len(tc_line) and tc['count']>0: 
                #      second condition is to keep track of urls added at query time
                    pu2lifewindows[pu].append({
                        'start': tc['time'], 
                        'start_nrev': tc['nrev'],
                        'start_user': tc['user'],
                        'start_user_id': tc['user_id']
                    })
                    TOGGLE_LIFETIME_START=True
            if TOGGLE_LIFETIME_START:
                if tc['count']==0:
                    pu2lifewindows[pu][-1]['end']=tc['time']
                    pu2lifewindows[pu][-1]['end_nrev']=tc['nrev']
                    pu2lifewindows[pu][-1]['end_user']=tc['user']
                    pu2lifewindows[pu][-1]['end_user_id']=tc['user_id']
                    TOGGLE_LIFETIME_START=False
                
        for n_l, lw in enumerate(pu2lifewindows[pu]):
            if 'end' in lw:
                pu2lifewindows[pu][n_l]['current']=0
            else:
                pu2lifewindows[pu][n_l]['end']=tc['time'] #<--- end takes the value of the last tc['time'] (which is query time)
                pu2lifewindows[pu][n_l]['end_nrev']=tc['nrev']
                pu2lifewindows[pu][n_l]['current']=1
                
    #-------------------------------------------------------------- dom-page-wise extracting time windows            
    pD2lifewindows={}
    for n, (pD, tc_line) in enumerate(pD2tc.items()):
        pD2lifewindows[pD]=[]
        TOGGLE_LIFETIME_START=False
        for n_t, tc in enumerate(tc_line):
            try:
                assert tc['count']>=0 #just to be sure
            except:
                print(pD, tc_line)
                break

            if not TOGGLE_LIFETIME_START: #<--- starts a lifetime even if the count is zero (zero length lifetimes considered)
                 #     unless it is the last element of the list (which is a dummy entry)
                if n_t+1!=len(tc_line) or n_t+1==len(tc_line) and tc['count']>0: 
                #      second condition is to keep track of urls added at query time
                    pD2lifewindows[pD].append({
                        'start': tc['time'], 
                        'start_nrev': tc['nrev'],
                        'start_user': tc['user'],
                        'start_user_id': tc['user_id']
                    })
                    TOGGLE_LIFETIME_START=True
            if TOGGLE_LIFETIME_START:
                if tc['count']==0:
                    pD2lifewindows[pD][-1]['end']=tc['time']
                    pD2lifewindows[pD][-1]['end_nrev']=tc['nrev']
                    pD2lifewindows[pD][-1]['end_user']=tc['user']
                    pD2lifewindows[pD][-1]['end_user_id']=tc['user_id']
                    TOGGLE_LIFETIME_START=False

        for n_l, lw in enumerate(pD2lifewindows[pD]):
            if 'end' in lw:
                pD2lifewindows[pD][n_l]['current']=0
            else:
                pD2lifewindows[pD][n_l]['end']=tc['time'] #<--- end takes the value of the last tc['time'] (which is query time)
                pD2lifewindows[pD][n_l]['end_nrev']=tc['nrev']
                pD2lifewindows[pD][n_l]['current']=1
                
    #-------------------------------- clearing lifewindows before MIN_DATE
    if MIN_DATE!=None:
        for pu, lifewindows in pu2lifewindows.items():
            page=pu.split('$')[0]
            selected_lw=[]
            for lw in lifewindows:
                sel_lw=select_lifewindow(page, lw, MIN_DATE, p2initrev)
                if sel_lw!=None:
                    selected_lw.append(lw)

            pu2lifewindows[pu]=deepcopy(selected_lw)
        
        for pD, lifewindows in pD2lifewindows.items():
            page=pD.split('$')[0]
            selected_lw=[]
            for lw in lifewindows:
                sel_lw=select_lifewindow(page, lw, MIN_DATE, p2initrev)
                if sel_lw!=None:
                    selected_lw.append(lw)

            pD2lifewindows[pD]=deepcopy(selected_lw)
    
    
    #-------------------------------- counting number of lifes           
    pu2nlife={pu: len(lw) for pu, lw in pu2lifewindows.items()}
    pD2nlife={pD: len(lw) for pD, lw in pD2lifewindows.items()}
    
    mult_count=0
    zero_count=0
    for pD, nlife in pD2nlife.items():
        if nlife>1:
            mult_count+=1
        if nlife==0:
            zero_count+=1

    # assert zero_count==0
    print(f"found {zero_count} domains with no lifetimes (); {zero_count/len(pD2nlife)*100:.3f}\% of total ")
    print(f"found {mult_count} domains with multiple lifetimes (deleted and added again after more than 1 day); {mult_count/len(pD2nlife)*100:.3f}\% of total ")
    
    #################################################################### make dataframes out of this data
    _pu2lw_list=[]
    for pu, lws in pu2lifewindows.items():
        # u=pu.split('$') # used $ as separator of page$url
        # p=u.pop(0)      # but $ is sometimes used also inside of url
        # u=''.join(u)    # so first instance of $ is the one that separate p, u
        u=pu[pu.find('$')+1:]
        p=pu[:pu.find('$')]
        for lw in lws:
            _pu2lw_list.append({
                'pu':pu,
                'page':p,
                'url':u,
                'start':lw['start'],
                'start_nrev':lw['start_nrev'],
                'start_user':lw['start_user'],
                'start_user_id':lw['start_user_id'],
                'end':lw['end'],
                'end_nrev':lw['end_nrev'],
                'end_user':lw.get('end_user'),
                'end_user_id':lw.get('end_user_id'),
                'current':lw['current'],
            })

    U_LT_DF=pd.DataFrame(_pu2lw_list)


    _pD2lw_list=[]
    for pD, lws in pD2lifewindows.items():
        # u=pu.split('$') # used $ as separator of page$url
        # p=u.pop(0)      # but $ is sometimes used also inside of url
        # u=''.join(u)    # so first instance of $ is the one that separate p, u
        D=pD[pD.find('$')+1:]
        p=pD[:pD.find('$')]
        for lw in lws:
            _pD2lw_list.append({
                'pD':pD,
                'page':p,
                'domain':D,
                'start':lw['start'],
                'start_nrev':lw['start_nrev'],
                'start_user':lw['start_user'],
                'start_user_id':lw['start_user_id'],
                'end':lw['end'],
                'end_nrev':lw['end_nrev'],
                'end_user':lw.get('end_user'),
                'end_user_id':lw.get('end_user_id'),
                'current':lw['current'],
            })

    D_LT_DF=pd.DataFrame(_pD2lw_list)
    
    
    # ---------------------------------------- dataframes of currently used
    CURR_U_LT_DF=U_LT_DF[U_LT_DF['current']==1]
    CURR_D_LT_DF=D_LT_DF[D_LT_DF['current']==1]
    pu_curr=CURR_U_LT_DF['pu'].unique()
    pD_curr=CURR_D_LT_DF['pD'].unique()
    
    ########################################## removing lifetimes that STARTED before MIN_DATE for editor_start
    if MIN_DATE!=None:
        SEL_U_LT_DF=U_LT_DF[U_LT_DF['start']>MIN_DATE]
        SEL_D_LT_DF=D_LT_DF[D_LT_DF['start']>MIN_DATE]
    else:
        SEL_U_LT_DF=U_LT_DF
        SEL_D_LT_DF=D_LT_DF
    
    ########################################################### number of editors who start/end lifetimes 
    pu2editors_start={}
    pu2editors_end={}
    pu2registerededitors_start={}
    pu2registerededitors_end={}

    pu2norm_editors_start={}
    pu2norm_editors_end={}
    pu2norm_registerededitors_start={}
    pu2norm_registerededitors_end={}

    PU_GB=U_LT_DF.groupby('pu')[['pu','start_user','end_user', 'start_user_id','end_user_id']]
    SEL_PU_GB=SEL_U_LT_DF.groupby('pu')[['pu','start_user','end_user', 'start_user_id','end_user_id']]

    for pu in tqdm(U_LT_DF['pu'].unique()):
        pu_df=PU_GB.get_group(pu)
        try:
            sel_pu_df=SEL_PU_GB.get_group(pu)
        except:
            sel_pu_df=pd.DataFrame(columns=['pu','start_user','end_user', 'start_user_id','end_user_id'])

        pu_df_start=sel_pu_df[~sel_pu_df['start_user'].isnull()]# remove invalid entries
        pu_df_end  =pu_df[~pu_df['end_user'].isnull()]  # remove invalid entries and lifetimes that have not ended yet

        editors_start=pu_df_start['start_user'].tolist()
        editors_end  =pu_df_end['end_user'].tolist()
        registerededitors_start=pu_df_start[pu_df_start['start_user_id']!=0]['start_user'].tolist() 
        #select editors who are registered registerededitors
        registerededitors_end  =pu_df_end[pu_df_end['end_user_id']!=0]  ['end_user'].tolist() 
        #select editors who are registered registerededitors


        pu2editors_start[pu]=len(set(editors_start))
        pu2editors_end[pu]  =len(set(editors_end))
        pu2registerededitors_start[pu]  =len(set(registerededitors_start))  
        pu2registerededitors_end[pu]    =len(set(registerededitors_end))

        n_starts=max(1, len(pu_df_start))
        n_ends  =max(1, len(pu_df_end))
        pu2norm_editors_start[pu]=len(set(editors_start))/n_starts #how many different editors/n starts
        pu2norm_editors_end[pu]=len(set(editors_end))/n_ends       #how many different editors/n ends
        pu2norm_registerededitors_start[pu]=len(registerededitors_start)/n_starts  #how many times started by a registered user/n start
        pu2norm_registerededitors_end[pu]=len(registerededitors_end)/n_ends        #how many times ended by a registered user/n start
        
    #------------------------------------------------- page-dom-wise    
    pD2editors_start={}
    pD2editors_end={}
    pD2registerededitors_start={}
    pD2registerededitors_end={}

    pD2norm_editors_start={}
    pD2norm_editors_end={}
    pD2norm_registerededitors_start={}
    pD2norm_registerededitors_end={}


    D2editors_start={}
    D2editors_end={}
    D2registerededitors_start={}
    D2registerededitors_end={}

    D2norm_editors_start={}
    D2norm_editors_end={}
    D2norm_registerededitors_start={}
    D2norm_registerededitors_end={}

    PD_GB=D_LT_DF.groupby('pD')[['pD','start_user','end_user', 'start_user_id','end_user_id']]
    SEL_PD_GB=SEL_D_LT_DF.groupby('pD')[['pD','start_user','end_user', 'start_user_id','end_user_id']]

    for pD in tqdm(D_LT_DF['pD'].unique()):
        pD_df=PD_GB.get_group(pD)
        try:
            sel_pD_df=SEL_PD_GB.get_group(pD)
        except: #this can happends for domains added before MIN_TIME and never edited again 
            sel_pD_df=pd.DataFrame(columns=['pD','start_user','end_user', 'start_user_id','end_user_id']) 
        
        pD_df_start=sel_pD_df[~sel_pD_df['start_user'].isnull()]# remove invalid entries
        pD_df_end  =pD_df[~pD_df['end_user'].isnull()]# remove invalid entries and lifetimes that have not ended yet

        editors_start=pD_df_start['start_user'].tolist()
        editors_end  =pD_df_end  ['end_user'].tolist()
        registerededitors_start=pD_df_start[pD_df_start['start_user_id']!=0]['start_user'].tolist() 
        #select editors who are registered registerededitors
        registerededitors_end  =pD_df_end  [pD_df_end  ['end_user_id']!=0]  ['end_user'].tolist() 
        #select editors who are registered registerededitors

        pD2editors_start[pD]=len(set(editors_start))
        pD2editors_end[pD]  =len(set(editors_end))
        pD2registerededitors_start[pD]  =len(set(registerededitors_start))  
        pD2registerededitors_end[pD]    =len(set(registerededitors_end))

        n_starts=max(1, len(pD_df_start))
        n_ends  =max(1, len(pD_df_end))
        pD2norm_editors_start[pD]=len(set(editors_start))/n_starts #how many different editors/n starts
        pD2norm_editors_end[pD]=len(set(editors_end))/n_ends       #how many different editors/n ends
        pD2norm_registerededitors_start[pD]=len(registerededitors_start)/n_starts  #how many times started by a registered user/n start
        pD2norm_registerededitors_end[pD]=len(registerededitors_end)/n_ends        #how many times ended by a registered user/n start

        D=pD.split('$')[-1]

        if D not in D2editors_start: D2editors_start[D]=[]
        D2editors_start[D]+=editors_start
        if D not in D2editors_end: D2editors_end[D]=[]
        D2editors_end[D]+=editors_end   
        if D not in D2registerededitors_start: D2registerededitors_start[D]=[]
        D2registerededitors_start[D]+=registerededitors_start   
        if D not in D2registerededitors_end: D2registerededitors_end[D]=[]
        D2registerededitors_end[D]+=registerededitors_end

    for D in D2editors_start:
        D2norm_editors_start[D]=len(set(D2editors_start[D]))/max(1,len(D2editors_start[D]))
    for D in D2editors_end:
        D2norm_editors_end[D]=len(set(D2editors_end[D]))/max(1,len(D2editors_end[D]))
    for D in D2registerededitors_start:
        D2norm_registerededitors_start[D]=len(D2registerededitors_start[D])/max(1,len(D2editors_start[D]))
    for D in D2registerededitors_end:
        D2norm_registerededitors_end[D]=len(D2registerededitors_end[D])/max(1,len(D2editors_end[D]))
    # NB Normalization BEFORE because still needed the full list for the normalized values
    # so do not move this 
    for D in D2editors_start:
        D2editors_start[D]=len(set(D2editors_start[D]))
    for D in D2editors_end:
        D2editors_end[D]=len(set(D2editors_end[D]))
    for D in D2registerededitors_start:
        D2registerededitors_start[D]=len(set(D2registerededitors_start[D]))
    for D in D2registerededitors_end:
        D2registerededitors_end[D]=len(set(D2registerededitors_end[D]))
        
    ############################################################ editors who add/remove a source
    SEL_URLSRV['d']=SEL_URLSRV['a']-SEL_URLSRV['r']
    SEL_URLSRV['pu']=SEL_URLSRV['page']+'$'+SEL_URLSRV['url']
    SEL_URLSRV['pD']=SEL_URLSRV['page']+'$'+SEL_URLSRV['final_domain']
    
    pu2editors_add={}
    pu2editors_rem={}
    pu2registerededitors_add={}
    pu2registerededitors_rem={}

    pu2norm_editors_add={}
    pu2norm_editors_rem={}
    pu2norm_registerededitors_add={}
    pu2norm_registerededitors_rem={}

    PU_GB=SEL_URLSRV.groupby('pu')[['pu','d', 'user','user_id']]

    for pu in tqdm(U_LT_DF['pu'].unique()):
        try:
            pu_df=PU_GB.get_group(pu)
        except: 
            #can happend that pu is not in SEL_URLSRV
            #because of discrepancy between U_LT_DF and SEL_URLSRV (where everything before 2019 is removed)
            #but it is intended behaviour             
            continue

        pu_df_add=pu_df[pu_df['d']>0][~pu_df['user'].isnull()]# remove invalid entries
        pu_df_rem=pu_df[pu_df['d']<0][~pu_df['user'].isnull()]# remove invalid entries and lifetimes that have not ended yet

        editors_add=pu_df_add['user'].tolist()
        editors_rem=pu_df_rem['user'].tolist()
        registerededitors_add=pu_df_add[pu_df_add['user_id']!=0]['user'].tolist()
        registerededitors_rem=pu_df_rem[pu_df_rem['user_id']!=0]['user'].tolist()

        pu2editors_add[pu]=len(set(editors_add))
        pu2editors_rem[pu]=len(set(editors_rem))
        pu2registerededitors_add[pu]  =len(set(registerededitors_add))
        pu2registerededitors_rem[pu]  =len(set(registerededitors_rem))

        n_adds=max(1, len(pu_df_add))
        n_rems=max(1, len(pu_df_rem))
        pu2norm_editors_add[pu]=len(set(editors_add))/n_adds #how many different editors/n starts
        pu2norm_editors_rem[pu]=len(set(editors_rem))/n_rems       #how many different editors/n ends
        pu2norm_registerededitors_add[pu]=len(registerededitors_add)/n_adds  #how many times started by a registered user/n start
        pu2norm_registerededitors_rem[pu]=len(registerededitors_rem)/n_rems        #how many times ended by a registered user/n start
        
   #---------------------------------------------------- page-dom-wise
    pD2editors_add={}
    pD2editors_rem={}
    pD2registerededitors_add={}
    pD2registerededitors_rem={}

    pD2norm_editors_add={}
    pD2norm_editors_rem={}
    pD2norm_registerededitors_add={}
    pD2norm_registerededitors_rem={}

    D2editors_add={}
    D2editors_rem={}
    D2registerededitors_add={}
    D2registerededitors_rem={}

    D2norm_editors_add={}
    D2norm_editors_rem={}
    D2norm_registerededitors_add={}
    D2norm_registerededitors_rem={}

    PD_GB=SEL_URLSRV.groupby('pD')[['pD','d', 'user','user_id']]

    for pD in tqdm(D_LT_DF['pD'].unique()):
        try:
            pD_df=PD_GB.get_group(pD)
        except:
            #can happend that pD is not in SEL_URLSRV
            #because of discrepancy between D_LT_DF and SEL_URLSRV (where everything before 2019 is removed)
            #but it is intended behaviour 
            continue
            
        pD_df_add=pD_df[pD_df['d']>0][~pD_df['user'].isnull()]# remove invalid entries
        pD_df_rem=pD_df[pD_df['d']<0][~pD_df['user'].isnull()]# remove invalid entries and lifetimes that have not ended yet

        editors_add=pD_df_add['user'].tolist()
        editors_rem=pD_df_rem['user'].tolist()
        registerededitors_add=pD_df_add[pD_df_add['user_id']!=0]['user'].tolist()
        registerededitors_rem=pD_df_rem[pD_df_rem['user_id']!=0]['user'].tolist()

        pD2editors_add[pD]=len(editors_add)
        pD2editors_rem[pD]=len(editors_rem)
        pD2registerededitors_add[pD]  =len(registerededitors_add)
        pD2registerededitors_rem[pD]  =len(registerededitors_rem)

        n_adds=max(1, len(pD_df_add))
        n_rems=max(1, len(pD_df_rem))
        pD2norm_editors_add[pD]=len(set(editors_add))/n_adds #how many different editors/n starts
        pD2norm_editors_rem[pD]=len(set(editors_rem))/n_rems       #how many different editors/n ends
        pD2norm_registerededitors_add[pD]=len(registerededitors_add)/n_adds  #how many times started by a registered user/n start
        pD2norm_registerededitors_rem[pD]=len(registerededitors_rem)/n_rems        #how many times ended by a registered user/n start


        D=pD.split('$')[-1]

        if D not in D2editors_add: D2editors_add[D]=[]
        D2editors_add[D]+=editors_add
        if D not in D2editors_rem: D2editors_rem[D]=[]
        D2editors_rem[D]+=editors_rem   
        if D not in D2registerededitors_add: D2registerededitors_add[D]=[]
        D2registerededitors_add[D]+=registerededitors_add   
        if D not in D2registerededitors_rem: D2registerededitors_rem[D]=[]
        D2registerededitors_rem[D]+=registerededitors_rem


    for D in D2editors_add:
        D2norm_editors_add[D]=len(set(D2editors_add[D]))/max(1,len(D2editors_add[D]))
    for D in D2editors_rem:
        D2norm_editors_rem[D]=len(set(D2editors_rem[D]))/max(1,len(D2editors_rem[D]))
    for D in D2registerededitors_add:
        D2norm_registerededitors_add[D]=len(D2registerededitors_add[D])/max(1,len(D2editors_add[D]))
    for D in D2registerededitors_rem:
        D2norm_registerededitors_rem[D]=len(D2registerededitors_rem[D])/max(1,len(D2editors_rem[D]))
    # NB Normalization BEFORE because still needed the full list for the normalized values
    # so do not move this 
    for D in D2editors_add:
        D2editors_add[D]=len(set(D2editors_add[D]))
    for D in D2editors_rem:
        D2editors_rem[D]=len(set(D2editors_rem[D]))
    for D in D2registerededitors_add:
        D2registerededitors_add[D]=len(set(D2registerededitors_add[D]))
    for D in D2registerededitors_rem:
        D2registerededitors_rem[D]=len(set(D2registerededitors_rem[D]))

    ########################################################################### Age
    pu2age={}
    pu2age_nrev={}

    for pu, lw in pu2lifewindows.items():
        if not len(lw):
            continue

        pu2age[pu]=(lw[-1]['end']-lw[0]['start']).days
        pu2age_nrev[pu]=lw[-1]['end_nrev']-lw[0]['start_nrev']
        if pu2age_nrev[pu]==0:
            pu2age_nrev[pu]=1
            
    pD2age={}
    pD2age_nrev={}

    for pD, lw in pD2lifewindows.items():
        if not len(lw):
            continue

        pD2age[pD]=(lw[-1]['end']-lw[0]['start']).days
        pD2age_nrev[pD]=lw[-1]['end_nrev']-lw[0]['start_nrev']
        if pD2age_nrev[pD]==0:
            pD2age_nrev[pD]=1
            
            
    ######################################## Merging with urlsinfo
    URLS_INFO=URLS_INFO[URLS_INFO.url.isin(U_LT_DF.url)] #some might have been cut out by lifetime algorithm
    U_LT_DF=U_LT_DF.merge(URLS_INFO, on='url')
    CURR_U_LT_DF=CURR_U_LT_DF.merge(URLS_INFO, on='url')
    
    ######################################## Life length
    U_LT_DF['length']=(U_LT_DF['end']-U_LT_DF['start']).map(lambda x: x.days)
    U_LT_DF['length_nrev']=U_LT_DF['end_nrev']-U_LT_DF['start_nrev']
    U_LT_DF['length_nrev']=U_LT_DF['length_nrev'].map(lambda x: 1 if x==0 else x)
    #---------------------- life lenght on current
    CURR_U_LT_DF['length']=(CURR_U_LT_DF['end']-CURR_U_LT_DF['start']).map(lambda x: x.days)
    CURR_U_LT_DF['length_nrev']=CURR_U_LT_DF['end_nrev']-CURR_U_LT_DF['start_nrev']
    CURR_U_LT_DF['length_nrev']=CURR_U_LT_DF['length_nrev'].map(lambda x: 1 if x==0 else x)
    
    #------------------------------ page-dom-wise
    D_LT_DF['length']=(D_LT_DF['end']-D_LT_DF['start']).map(lambda x: x.days)
    D_LT_DF['length_nrev']=D_LT_DF['end_nrev']-D_LT_DF['start_nrev']
    D_LT_DF['length_nrev']=D_LT_DF['length_nrev'].map(lambda x: 1 if x==0 else x)
    #---------------------- life lenght on current
    CURR_D_LT_DF['length']=(CURR_D_LT_DF['end']-CURR_D_LT_DF['start']).map(lambda x: x.days)
    CURR_D_LT_DF['length_nrev']=CURR_D_LT_DF['end_nrev']-CURR_D_LT_DF['start_nrev']
    CURR_D_LT_DF['length_nrev']=CURR_D_LT_DF['length_nrev'].map(lambda x: 1 if x==0 else x)
    
    #---------------------------------------- control cells
    try:
        assert U_LT_DF['length'].min()>=0
        assert U_LT_DF['length_nrev'].min()>0

        assert CURR_U_LT_DF['length'].min()>=0
        assert CURR_U_LT_DF['length_nrev'].min()>0

        assert D_LT_DF['length'].min()>=0
        assert D_LT_DF['length_nrev'].min()>0

        assert CURR_D_LT_DF['length'].min()>=0
        assert CURR_D_LT_DF['length_nrev'].min()>0
    except:
        print(U_LT_DF['length'].min())
        print(U_LT_DF['length_nrev'].min())
        
        print(CURR_U_LT_DF['length'].min())
        print(CURR_U_LT_DF['length_nrev'].min())
        
        print(D_LT_DF['length'].min())
        print(D_LT_DF['length_nrev'].min())
        
        print(CURR_D_LT_DF['length'].min())
        print(CURR_D_LT_DF['length_nrev'].min())
        
        print(f"NO SOURCE BEING USED AT ALL CURRENTLY IN PROJECT {PROJECT_NAME}, language {LANG}")  
        print("No data will be saved")
        return 2
    
    ######################################################################### Permanence
    PU2PERMANENCE_DF=U_LT_DF.groupby(['page','url']).sum().reset_index()
    PU2PERMANENCE_DF['pu']=PU2PERMANENCE_DF['page']+'&'+PU2PERMANENCE_DF['url'] # I think I can change this & to $ (look 2 cells below)
    PU2PERMANENCE_DF=PU2PERMANENCE_DF.set_index('pu')

    PU2PERMANENCE_DF=PU2PERMANENCE_DF.rename(columns={'length':'permanence'})
    PU2PERMANENCE_DF['norm_permanence']=PU2PERMANENCE_DF.apply(lambda x: x['permanence']/max(1,p2time[x['page']]), axis=1)
    PU2PERMANENCE_DF=PU2PERMANENCE_DF.rename(columns={'length_nrev':'permanence_nrev'})
    PU2PERMANENCE_DF['norm_permanence_nrev']=PU2PERMANENCE_DF.apply(lambda x: x['permanence_nrev']/p2nrev[x['page']], axis=1)

    PU2PERMANENCE_DF['pu']=PU2PERMANENCE_DF['page']+'$'+PU2PERMANENCE_DF['url'] 
    #because it is set as index, but I want to use pu column to access the dictionaries
    PU2PERMANENCE_DF['selfnorm_permanence']=PU2PERMANENCE_DF.apply(\
        lambda x: x['permanence']/(pu2age[x['pu']] if pu2age[x['pu']]>0 else 1), axis=1)
    PU2PERMANENCE_DF['selfnorm_permanence_nrev']=PU2PERMANENCE_DF.apply(\
        lambda x: x['permanence_nrev']/(pu2age_nrev[x['pu']] if pu2age_nrev[x['pu']]>0 else 1), axis=1)

    PU2PERMANENCE_DF=PU2PERMANENCE_DF.drop(columns=['pu', 'start_nrev', 'end_nrev'])

    # ---------------------------------------------------------------- current permanence
    CURR_PU2PERMANENCE_DF=CURR_U_LT_DF.groupby(['page','url']).sum().reset_index()
    CURR_PU2PERMANENCE_DF['pu']=CURR_PU2PERMANENCE_DF['page']+'&'+CURR_PU2PERMANENCE_DF['url'] # I think I can change this & to $ (look 2 cells below)
    CURR_PU2PERMANENCE_DF=CURR_PU2PERMANENCE_DF.set_index('pu')

    CURR_PU2PERMANENCE_DF=CURR_PU2PERMANENCE_DF.rename(columns={'length':'curr_permanence'})
    CURR_PU2PERMANENCE_DF['curr_norm_permanence']=CURR_PU2PERMANENCE_DF.apply(lambda x: x['curr_permanence']/max(1,p2time[x['page']]), axis=1)
    CURR_PU2PERMANENCE_DF=CURR_PU2PERMANENCE_DF.rename(columns={'length_nrev':'curr_permanence_nrev'})
    CURR_PU2PERMANENCE_DF['curr_norm_permanence_nrev']=CURR_PU2PERMANENCE_DF.apply(lambda x: (x['curr_permanence_nrev']+x['current'])/p2nrev[x['page']], axis=1)
    
    ########################################################################### Controversy scores
    if CONTRO_SCORES_FOUND:
        PU2INV_SCORES=INV_SCORES.groupby(['page','url']).sum().reset_index()\
        .drop(columns=['page_id','revid','revid_parent','flags','user_id'])

        PU2INV_SCORES['pu']=PU2INV_SCORES['page']+'&'+PU2INV_SCORES['url']
        PU2INV_SCORES=PU2INV_SCORES.set_index('pu')

    if CONTRO_SCORES_FOUND:
        PU2INV_SCORES=PU2INV_SCORES.drop(columns=['n_rev_valid'])
        
    ############################################################################ BUILDING URL2INFO
    URL2INFO=U_LT_DF.drop_duplicates(subset=['page','url'])[['page','url']]
    
    URL2INFO=URL2INFO.merge(URLS_INFO[['url', 'status_code', 'final_domain','perennial_status','mbfc_status']], on='url')
    
    # for compatibility, here second separator char is $ (will be changed back to & below)
    URL2INFO['pu']=URL2INFO['page']+'$'+URL2INFO['url']
    URL2INFO=URL2INFO.set_index('pu')

    URL2INFO['n_life']=pd.DataFrame.from_dict(pu2nlife, orient='index')
    
    
    URL2INFO['editors_start']=pd.DataFrame.from_dict(pu2editors_start, orient='index')
    URL2INFO['editors_end']=pd.DataFrame.from_dict(pu2editors_end, orient='index')
    URL2INFO['registerededitors_start']=pd.DataFrame.from_dict(pu2registerededitors_start, orient='index')
    URL2INFO['registerededitors_end']=pd.DataFrame.from_dict(pu2registerededitors_end, orient='index')
    # normalized version of these
    URL2INFO['prop_editors_start']=pd.DataFrame.from_dict(pu2norm_editors_start, orient='index')
    URL2INFO['prop_editors_end']=pd.DataFrame.from_dict(pu2norm_editors_end, orient='index')
    URL2INFO['prop_registerededitors_start']=pd.DataFrame.from_dict(pu2norm_registerededitors_start, orient='index')
    URL2INFO['prop_registerededitors_end']=pd.DataFrame.from_dict(pu2norm_registerededitors_end, orient='index')
    
    # editor/user_add/del
    URL2INFO['editors_add']=pd.DataFrame.from_dict(pu2editors_add, orient='index')
    URL2INFO['editors_rem']=pd.DataFrame.from_dict(pu2editors_rem, orient='index')
    URL2INFO['registerededitors_add']=pd.DataFrame.from_dict(pu2registerededitors_add, orient='index')
    URL2INFO['registerededitors_rem']=pd.DataFrame.from_dict(pu2registerededitors_rem, orient='index')
    # normalized version of these
    URL2INFO['prop_editors_add']=pd.DataFrame.from_dict(pu2norm_editors_add, orient='index')
    URL2INFO['prop_editors_rem']=pd.DataFrame.from_dict(pu2norm_editors_rem, orient='index')
    URL2INFO['prop_registerededitors_add']=pd.DataFrame.from_dict(pu2norm_registerededitors_add, orient='index')
    URL2INFO['prop_registerededitors_rem']=pd.DataFrame.from_dict(pu2norm_registerededitors_rem, orient='index')
    
    ################################### url ages
    URL2INFO['age']=pd.DataFrame.from_dict(pu2age, orient='index')
    URL2INFO['age_nrev']=pd.DataFrame.from_dict(pu2age_nrev, orient='index')

    ################################### adding permanence info
    URL2INFO['pu']=URL2INFO['page']+'&'+URL2INFO['url']
    URL2INFO=URL2INFO.set_index('pu')
    URL2INFO=URL2INFO.merge(PU2PERMANENCE_DF[[c for c in PU2PERMANENCE_DF.columns if 'permanence' in c]],
                            left_index=True, right_index=True)
    URL2INFO=URL2INFO.merge(CURR_PU2PERMANENCE_DF[[c for c in CURR_PU2PERMANENCE_DF.columns if 'permanence' in c]],
                            left_index=True, right_index=True, how='left')
    for cp in [c for c in CURR_PU2PERMANENCE_DF.columns if 'permanence' in c]:
        URL2INFO[cp].fillna(0, inplace=True)
    URL2INFO['current']=URL2INFO['curr_permanence'].map(lambda x: 1 if x>0 else 0)
    
    ################################### adding controversy scores info
    if CONTRO_SCORES_FOUND:
        URL2INFO=URL2INFO.merge(PU2INV_SCORES[[c for c in PU2INV_SCORES.columns if c not in ['page', 'url']]],
                            left_index=True, right_index=True, how='left')
        print(len(URL2INFO))
        
    if CONTRO_SCORES_FOUND:
        URL2INFO['involved_count']=URL2INFO['involved_count'].fillna(0)
        URL2INFO['involved_count_normalized']=URL2INFO['involved_count_normalized'].fillna(0)
        
    if CONTRO_SCORES_FOUND:
        URL2INFO['inv_score_globnorm']=URL2INFO['involved_count']/URL2INFO['permanence'].map(lambda x: 1 if x<=0 else x)
        URL2INFO['inv_score_globnorm_nrev']=URL2INFO['involved_count']/URL2INFO['permanence_nrev'].map(lambda x: 1 if x<=0 else x)

        URL2INFO.sort_values(by='inv_score_globnorm', ascending=False)
        
    if CONTRO_SCORES_FOUND:
        URL2INFO['edit_count']=URL2INFO['involved_count']+URL2INFO['n_life']
        
    if CONTRO_SCORES_FOUND:
        URL2INFO['edit_score_globnorm']=URL2INFO['edit_count']/URL2INFO['permanence'].map(lambda x: 1 if x<=0 else x)
        URL2INFO['edit_score_globnorm_nrev']=URL2INFO['edit_count']/URL2INFO['permanence_nrev'].map(lambda x: 1 if x<=0 else x)

    ########################################################### SAVING URL2INFO
    if ALLOW_SAVE:
        URL2INFO.to_csv(PATHNAME_URL)
        
        
    ########################################################### BUILDING DOM2INFO
    DOMAINS=URLS_INFO.final_domain.unique()
    CURR_DOMAINS=CURR_U_LT_DF.final_domain.unique()
    DOM2INFO=pd.DataFrame(index=DOMAINS)

    _GB_DD=URLS_INFO.groupby('final_domain')

    DOM2prop_ok={}        #<--- status=200/all the other       (currently working urls)
    DOM2prop_nonfailed={} #<--- status!= FAILED/all the others (currently working urls+HTTPError)

    for dom in tqdm(DOMAINS):
        dom_df=_GB_DD.get_group(dom)
        DOM2prop_ok[dom]=       len(dom_df[dom_df.status_code=='200'])   /len(dom_df)*100
        DOM2prop_nonfailed[dom]=len(dom_df[dom_df.status_code!='FAILED'])/len(dom_df)*100

    ########################### perennial status
    D2perennial=URLS_INFO[['final_domain','perennial_status']].set_index('final_domain').to_dict()['perennial_status']

    ########################### mbfc status
    D2mbfc=URLS_INFO[['final_domain','mbfc_status']].set_index('final_domain').to_dict()['mbfc_status']
    
    GB_DOM=U_LT_DF.groupby('final_domain')
    CURR_GB_DOM=CURR_U_LT_DF.groupby('final_domain')

    ######################################## page-dom-wise statistics
    DOM2NLIFE={dom: len(GB_DOM.get_group(dom)) for dom in tqdm(DOMAINS)}

    DOM2NURLS={dom: len(GB_DOM.get_group(dom).groupby('url')) for dom in tqdm(DOMAINS)}
    DOM2NCURRURLS={dom: len(CURR_GB_DOM.get_group(dom).groupby('url')) for dom in tqdm(CURR_DOMAINS)}

    DOM2NPAGES={dom: len(GB_DOM.get_group(dom).groupby('page')) for dom in tqdm(DOMAINS)}
    DOM2NCURRPAGES={dom: len(CURR_GB_DOM.get_group(dom).groupby('page')) for dom in tqdm(CURR_DOMAINS)}

    DOM2NPAGESURLS={dom: len(GB_DOM.get_group(dom).groupby(['page','url'])) for dom in tqdm(DOMAINS)}
    DOM2NCURRPAGESURLS={dom: len(CURR_GB_DOM.get_group(dom).groupby(['page','url'])) for dom in tqdm(CURR_DOMAINS)}

    DOM2INFO['n_life']=pd.DataFrame.from_dict(DOM2NLIFE, orient='index')
    DOM2INFO['n_urls']=pd.DataFrame.from_dict(DOM2NURLS, orient='index')
    DOM2INFO['n_pages']=pd.DataFrame.from_dict(DOM2NPAGES, orient='index')
    DOM2INFO['n_pageurls']=pd.DataFrame.from_dict(DOM2NPAGESURLS, orient='index')
    DOM2INFO['curr_n_urls']=pd.DataFrame.from_dict(DOM2NCURRURLS, orient='index')
    DOM2INFO['curr_n_pages']=pd.DataFrame.from_dict(DOM2NCURRPAGES, orient='index')
    DOM2INFO['curr_n_pageurls']=pd.DataFrame.from_dict(DOM2NCURRPAGESURLS, orient='index')
    DOM2INFO['prop_ok']=pd.DataFrame.from_dict(DOM2prop_ok, orient='index')
    DOM2INFO['prop_nonfail']=pd.DataFrame.from_dict(DOM2prop_nonfailed, orient='index')
    DOM2INFO['perennial']=pd.DataFrame.from_dict(D2perennial, orient='index')
    
    DOM2INFO['norm_n_pages']=DOM2INFO['n_pages']/TOT_PAGES
    DOM2INFO['norm_curr_n_pages']=DOM2INFO['curr_n_pages']/TOT_PAGES
    
    for c in DOM2INFO.columns:
        if not 'curr' in c: continue
        DOM2INFO[c].fillna(0, inplace=True)
    DOM2INFO['mbfc']=pd.DataFrame.from_dict(D2mbfc, orient='index')
    
    
    ################################################ global editors stats
    DOM2INFO['editors_start']=pd.DataFrame.from_dict(D2editors_start, orient='index')
    DOM2INFO['editors_end']=pd.DataFrame.from_dict(D2editors_end, orient='index')
    DOM2INFO['registerededitors_start']=pd.DataFrame.from_dict(D2registerededitors_start, orient='index')
    DOM2INFO['registerededitors_end']=pd.DataFrame.from_dict(D2registerededitors_end, orient='index')
    DOM2INFO['prop_editors_start']=pd.DataFrame.from_dict(D2norm_editors_start, orient='index')
    DOM2INFO['prop_editors_end']=pd.DataFrame.from_dict(D2norm_editors_end, orient='index')
    DOM2INFO['prop_registerededitors_start']=pd.DataFrame.from_dict(D2norm_registerededitors_start, orient='index')
    DOM2INFO['prop_registerededitors_end']=pd.DataFrame.from_dict(D2norm_registerededitors_end, orient='index')

    DOM2INFO['editors_add']=pd.DataFrame.from_dict(D2editors_add, orient='index')
    DOM2INFO['editors_rem']=pd.DataFrame.from_dict(D2editors_rem, orient='index')
    DOM2INFO['registerededitors_add']=pd.DataFrame.from_dict(D2registerededitors_add, orient='index')
    DOM2INFO['registerededitors_rem']=pd.DataFrame.from_dict(D2registerededitors_rem, orient='index')
    DOM2INFO['prop_editors_add']=pd.DataFrame.from_dict(D2norm_editors_add, orient='index')
    DOM2INFO['prop_editors_rem']=pd.DataFrame.from_dict(D2norm_editors_rem, orient='index')
    DOM2INFO['prop_registerededitors_add']=pd.DataFrame.from_dict(D2norm_registerededitors_add, orient='index')
    DOM2INFO['prop_registerededitors_rem']=pd.DataFrame.from_dict(D2norm_registerededitors_rem, orient='index')

    DOM2INFO['norm_editors_start']=DOM2INFO['editors_start']/TOT_EDITORS
    DOM2INFO['norm_editors_end']=DOM2INFO['editors_end']/TOT_EDITORS
    DOM2INFO['norm_registerededitors_start']=DOM2INFO['registerededitors_start']/TOT_REGISTEREDEDITORS
    DOM2INFO['norm_registerededitors_end']=DOM2INFO['registerededitors_end']/TOT_REGISTEREDEDITORS

    DOM2INFO['norm_editors_add']=DOM2INFO['editors_add']/TOT_EDITORS
    DOM2INFO['norm_editors_rem']=DOM2INFO['editors_rem']/TOT_EDITORS
    DOM2INFO['norm_registerededitors_add']=DOM2INFO['registerededitors_add']/TOT_REGISTEREDEDITORS
    DOM2INFO['norm_registerededitors_rem']=DOM2INFO['registerededitors_rem']/TOT_REGISTEREDEDITORS


    ############################################################################## url-aggregate metrics
    _UGB_MEAN=URL2INFO.groupby('final_domain').mean()
    _UGB_MEDIAN=URL2INFO.groupby('final_domain').median()
    _UGB_SUM=URL2INFO.groupby('final_domain').sum()

    DOM2LTAVG=GB_DOM.mean()#.sort_values(by='length', ascending=False)
    DOM2LTSTD=GB_DOM.std() #.sort_values(by='length', ascending=False)
    DOM2LTMDN=GB_DOM.median()
    
    ############################################# url-aggregate editors metrics
    DOM2INFO['editors_start_avg_urlwise']=_UGB_MEAN['editors_start']
    DOM2INFO['editors_end_avg_urlwise']=_UGB_MEAN['editors_end']
    DOM2INFO['registerededitors_start_avg_urlwise']=_UGB_MEAN['registerededitors_start']
    DOM2INFO['registerededitors_end_avg_urlwise']=_UGB_MEAN['registerededitors_end']
    DOM2INFO['prop_editors_start_avg_urlwise']=_UGB_MEAN['prop_editors_start']
    DOM2INFO['prop_editors_end_avg_urlwise']=_UGB_MEAN['prop_editors_end']
    DOM2INFO['prop_registerededitors_start_avg_urlwise']=_UGB_MEAN['prop_registerededitors_start']
    DOM2INFO['prop_registerededitors_end_avg_urlwise']=_UGB_MEAN['prop_registerededitors_end']

    DOM2INFO['editors_add_avg_urlwise']=_UGB_MEAN['editors_add']
    DOM2INFO['editors_rem_avg_urlwise']=_UGB_MEAN['editors_rem']
    DOM2INFO['registerededitors_add_avg_urlwise']=_UGB_MEAN['registerededitors_add']
    DOM2INFO['registerededitors_rem_avg_urlwise']=_UGB_MEAN['registerededitors_rem']
    DOM2INFO['prop_editors_add_avg_urlwise']=_UGB_MEAN['prop_editors_add']
    DOM2INFO['prop_editors_rem_avg_urlwise']=_UGB_MEAN['prop_editors_rem']
    DOM2INFO['prop_registerededitors_add_avg_urlwise']=_UGB_MEAN['prop_registerededitors_add']
    DOM2INFO['prop_registerededitors_rem_avg_urlwise']=_UGB_MEAN['prop_registerededitors_rem']

    DOM2INFO['editors_start_median_urlwise']=_UGB_MEDIAN['editors_start']
    DOM2INFO['editors_end_median_urlwise']=_UGB_MEDIAN['editors_end']
    DOM2INFO['registerededitors_start_median_urlwise']=_UGB_MEDIAN['registerededitors_start']
    DOM2INFO['registerededitors_end_median_urlwise']=_UGB_MEDIAN['registerededitors_end']
    DOM2INFO['prop_editors_start_median_urlwise']=_UGB_MEDIAN['prop_editors_start']
    DOM2INFO['prop_editors_end_median_urlwise']=_UGB_MEDIAN['prop_editors_end']
    DOM2INFO['prop_registerededitors_start_median_urlwise']=_UGB_MEDIAN['prop_registerededitors_start']
    DOM2INFO['prop_registerededitors_end_median_urlwise']=_UGB_MEDIAN['prop_registerededitors_end']

    DOM2INFO['editors_add_median_urlwise']=_UGB_MEDIAN['editors_add']
    DOM2INFO['editors_rem_median_urlwise']=_UGB_MEDIAN['editors_rem']
    DOM2INFO['registerededitors_add_median_urlwise']=_UGB_MEDIAN['registerededitors_add']
    DOM2INFO['registerededitors_rem_median_urlwise']=_UGB_MEDIAN['registerededitors_rem']
    DOM2INFO['prop_editors_add_median_urlwise']=_UGB_MEDIAN['prop_editors_add']
    DOM2INFO['prop_editors_rem_median_urlwise']=_UGB_MEDIAN['prop_editors_rem']
    DOM2INFO['prop_registerededitors_add_median_urlwise']=_UGB_MEDIAN['prop_registerededitors_add']
    DOM2INFO['prop_registerededitors_rem_median_urlwise']=_UGB_MEDIAN['prop_registerededitors_rem']
    
    #################################### age
    DOM2INFO['age_avg_urlwise']=_UGB_MEAN['age']
    DOM2INFO['age_median_urlwise']=_UGB_MEDIAN['age']
    DOM2INFO['age_sum_urlwise']=_UGB_SUM['age']

    DOM2INFO['age_nrev_avg_urlwise']=_UGB_MEAN['age_nrev']
    DOM2INFO['age_nrev_median_urlwise']=_UGB_MEDIAN['age_nrev']
    
    #################################### lifetime
    DOM2INFO['lifetime_avg_urlwise']=DOM2LTAVG['length']
    DOM2INFO['lifetime_std_urlwise']=DOM2LTSTD['length']
    DOM2INFO['lifetime_median_urlwise']=DOM2LTMDN['length']

    DOM2INFO['lifetime_nrev_avg_urlwise']=DOM2LTAVG['length_nrev']
    DOM2INFO['lifetime_nrev_std_urlwise']=DOM2LTSTD['length_nrev']
    DOM2INFO['lifetime_nrev_median_urlwise']=DOM2LTMDN['length_nrev']

    ############################################################# Permanence
    DOM2LTSUM=GB_DOM.sum('length')#.sort_values(by='length', ascending=False)
    DOM2INFO['permanence_sum_urlwise']=DOM2LTSUM['length'] #NB DOM2LT is obtained by U_LT_DF.groupby('domain')

    DOM2LTSUM=GB_DOM.sum('length_nrev')#.sort_values(by='length', ascending=False)
    DOM2INFO['permanence_nrev_sum_urlwise']=DOM2LTSUM['length_nrev'] #NB DOM2LT is obtained by U_LT_DF.groupby('domain')


    # in days
    DOM2INFO['permanence_avg_urlwise']   =URL2INFO.groupby('final_domain').mean()['permanence']
    DOM2INFO['permanence_median_urlwise']=URL2INFO.groupby('final_domain').median()['permanence']

    DOM2INFO['norm_permanence_avg_urlwise']   =URL2INFO.groupby('final_domain').mean()['norm_permanence']
    DOM2INFO['norm_permanence_median_urlwise']=URL2INFO.groupby('final_domain').median()['norm_permanence']

    DOM2INFO['selfnorm_permanence_avg_urlwise']=URL2INFO.groupby('final_domain').mean()['selfnorm_permanence']
    DOM2INFO['selfnorm_permanence_median_urlwise']=URL2INFO.groupby('final_domain').median()['selfnorm_permanence']

    # in nrev
    DOM2INFO['permanence_nrev_avg_urlwise']   =URL2INFO.groupby('final_domain').mean()['permanence_nrev']
    DOM2INFO['permanence_nrev_median_urlwise']=URL2INFO.groupby('final_domain').median()['permanence_nrev']

    DOM2INFO['norm_permanence_nrev_avg_urlwise']   =URL2INFO.groupby('final_domain').mean()['norm_permanence_nrev']
    DOM2INFO['norm_permanence_nrev_median_urlwise']=URL2INFO.groupby('final_domain').median()['norm_permanence_nrev']

    DOM2INFO['selfnorm_permanence_nrev_avg_urlwise']=URL2INFO.groupby('final_domain').mean()['selfnorm_permanence_nrev']
    DOM2INFO['selfnorm_permanence_nrev_median_urlwise']=URL2INFO.groupby('final_domain').median()['selfnorm_permanence_nrev']

    #------------------------------------------------- curr permanence
    DOM2INFO['curr_permanence_sum_urlwise']=URL2INFO.groupby('final_domain').sum()['curr_permanence']
    DOM2INFO['curr_permanence_nrev_sum_urlwise']=URL2INFO.groupby('final_domain').sum()['curr_permanence_nrev']

    CURR_URL2INFO=URL2INFO[URL2INFO['current']==1]

    DOM2INFO['curr_permanence_avg_urlwise']=CURR_URL2INFO.groupby('final_domain').mean()['curr_permanence']
    DOM2INFO['curr_permanence_nrev_avg_urlwise']=CURR_URL2INFO.groupby('final_domain').mean()['curr_permanence_nrev']

    DOM2INFO['curr_selfnorm_permanence_avg_urlwise']=CURR_URL2INFO.groupby('final_domain').mean()['selfnorm_permanence']
    DOM2INFO['curr_selfnorm_permanence_nrev_avg_urlwise']=CURR_URL2INFO.groupby('final_domain').mean()['selfnorm_permanence_nrev']

    for c in DOM2INFO.columns:
        if not 'curr' in c: continue
        DOM2INFO[c].fillna(0, inplace=True)
    DOM2INFO['age_nrev_sum_urlwise']=_UGB_SUM['age_nrev']
        
    ############################################################################## page-domain metrics
    GB_D_DOM=D_LT_DF.groupby('domain')
    
    ################################################## page-domain editors metrics
    PD_USERS_DF=pd.DataFrame.from_dict(pD2editors_start, orient='index', columns=['editors_start'])
    PD_USERS_DF['editors_end']=pd.DataFrame.from_dict(pD2editors_end, orient='index')[0]
    PD_USERS_DF['registerededitors_start']=pd.DataFrame.from_dict(pD2registerededitors_start, orient='index')
    PD_USERS_DF['registerededitors_end']=pd.DataFrame.from_dict(pD2registerededitors_end, orient='index')
    PD_USERS_DF['prop_editors_start']=pd.DataFrame.from_dict(pD2norm_editors_start, orient='index')
    PD_USERS_DF['prop_editors_end']=pd.DataFrame.from_dict(pD2norm_editors_end, orient='index')
    PD_USERS_DF['prop_registerededitors_start']=pd.DataFrame.from_dict(pD2norm_registerededitors_start, orient='index')
    PD_USERS_DF['prop_registerededitors_end']=pd.DataFrame.from_dict(pD2norm_registerededitors_end, orient='index')

    PD_USERS_DF['editors_add']=pd.DataFrame.from_dict(pD2editors_add, orient='index')
    PD_USERS_DF['editors_rem']=pd.DataFrame.from_dict(pD2editors_rem, orient='index')
    PD_USERS_DF['registerededitors_add']=pd.DataFrame.from_dict(pD2registerededitors_add, orient='index')
    PD_USERS_DF['registerededitors_rem']=pd.DataFrame.from_dict(pD2registerededitors_rem, orient='index')
    PD_USERS_DF['prop_editors_add']=pd.DataFrame.from_dict(pD2norm_editors_add, orient='index')
    PD_USERS_DF['prop_editors_rem']=pd.DataFrame.from_dict(pD2norm_editors_rem, orient='index')
    PD_USERS_DF['prop_registerededitors_add']=pd.DataFrame.from_dict(pD2norm_registerededitors_add, orient='index')
    PD_USERS_DF['prop_registerededitors_rem']=pd.DataFrame.from_dict(pD2norm_registerededitors_rem, orient='index')
    
    PD_USERS_DF['domain']=PD_USERS_DF.index.map(lambda x: x.split('$')[-1])
    _PDUGB_MEAN=PD_USERS_DF.groupby('domain').mean()
    _PDUGB_MEDIAN=PD_USERS_DF.groupby('domain').median()
    
    DOM2INFO['editors_start_avg_pagewise']=_PDUGB_MEAN['editors_start']
    DOM2INFO['editors_end_avg_pagewise']=_PDUGB_MEAN['editors_end']
    DOM2INFO['registerededitors_start_avg_pagewise']=_PDUGB_MEAN['registerededitors_start']
    DOM2INFO['registerededitors_end_avg_pagewise']=_PDUGB_MEAN['registerededitors_end']
    DOM2INFO['prop_editors_start_avg_pagewise']=_PDUGB_MEAN['prop_editors_start']
    DOM2INFO['prop_editors_end_avg_pagewise']=_PDUGB_MEAN['prop_editors_end']
    DOM2INFO['prop_registerededitors_start_avg_pagewise']=_PDUGB_MEAN['prop_registerededitors_start']
    DOM2INFO['prop_registerededitors_end_avg_pagewise']=_PDUGB_MEAN['prop_registerededitors_end']

    DOM2INFO['editors_add_avg_pagewise']=_PDUGB_MEAN['editors_add']
    DOM2INFO['editors_rem_avg_pagewise']=_PDUGB_MEAN['editors_rem']
    DOM2INFO['registerededitors_add_avg_pagewise']=_PDUGB_MEAN['registerededitors_add']
    DOM2INFO['registerededitors_rem_avg_pagewise']=_PDUGB_MEAN['registerededitors_rem']
    DOM2INFO['prop_editors_add_avg_pagewise']=_PDUGB_MEAN['prop_editors_add']
    DOM2INFO['prop_editors_rem_avg_pagewise']=_PDUGB_MEAN['prop_editors_rem']
    DOM2INFO['prop_registerededitors_add_avg_pagewise']=_PDUGB_MEAN['prop_registerededitors_add']
    DOM2INFO['prop_registerededitors_rem_avg_pagewise']=_PDUGB_MEAN['prop_registerededitors_rem']
    
    DOM2INFO['editors_start_median_pagewise']=_PDUGB_MEDIAN['editors_start']
    DOM2INFO['editors_end_median_pagewise']=_PDUGB_MEDIAN['editors_end']
    DOM2INFO['registerededitors_start_median_pagewise']=_PDUGB_MEDIAN['registerededitors_start']
    DOM2INFO['registerededitors_end_median_pagewise']=_PDUGB_MEDIAN['registerededitors_end']
    DOM2INFO['prop_editors_start_median_pagewise']=_PDUGB_MEDIAN['prop_editors_start']
    DOM2INFO['prop_editors_end_median_pagewise']=_PDUGB_MEDIAN['prop_editors_end']
    DOM2INFO['prop_registerededitors_start_median_pagewise']=_PDUGB_MEDIAN['prop_registerededitors_start']
    DOM2INFO['prop_registerededitors_end_median_pagewise']=_PDUGB_MEDIAN['prop_registerededitors_end']

    DOM2INFO['editors_add_median_pagewise']=_PDUGB_MEDIAN['editors_add']
    DOM2INFO['editors_rem_median_pagewise']=_PDUGB_MEDIAN['editors_rem']
    DOM2INFO['registerededitors_add_median_pagewise']=_PDUGB_MEDIAN['registerededitors_add']
    DOM2INFO['registerededitors_rem_median_pagewise']=_PDUGB_MEDIAN['registerededitors_rem']
    DOM2INFO['prop_editors_add_median_pagewise']=_PDUGB_MEDIAN['prop_editors_add']
    DOM2INFO['prop_editors_rem_median_pagewise']=_PDUGB_MEDIAN['prop_editors_rem']
    DOM2INFO['prop_registerededitors_add_median_pagewise']=_PDUGB_MEDIAN['prop_registerededitors_add']
    DOM2INFO['prop_registerededitors_rem_median_pagewise']=_PDUGB_MEDIAN['prop_registerededitors_rem']
    
    
    ######################################### age
    _AGE_DOM_DF=pd.DataFrame.from_dict(pD2age, orient='index', columns=['age'])
    _AGE_DOM_DF['age_nrev']=pd.DataFrame.from_dict(pD2age_nrev, orient='index')[0]

    _AGE_DOM_DF['domain']=_AGE_DOM_DF.index.map(lambda x: x.split('$')[-1])
    
    _AGE_DOM_DF_MEAN=_AGE_DOM_DF.groupby('domain').mean()
    _AGE_DOM_DF_MEDIAN=_AGE_DOM_DF.groupby('domain').median()
    _AGE_DOM_DF_SUM=_AGE_DOM_DF.groupby('domain').sum()
    
    DOM2INFO['age_avg_pagewise']=_AGE_DOM_DF_MEAN['age']
    DOM2INFO['age_median_pagewise']=_AGE_DOM_DF_MEDIAN['age']
    DOM2INFO['age_sum_pagewise']=_AGE_DOM_DF_SUM['age']

    DOM2INFO['age_nrev_avg_pagewise']=_AGE_DOM_DF_MEAN['age_nrev']
    DOM2INFO['age_nrev_median_pagewise']=_AGE_DOM_DF_MEDIAN['age_nrev']
    DOM2INFO['age_nrev_sum_pagewise']=_AGE_DOM_DF_SUM['age_nrev']
    
    ######################################## lifetimes
    D_DOM2LTAVG=GB_D_DOM.mean()#.sort_values(by='length', ascending=False)
    D_DOM2LTSTD=GB_D_DOM.std() #.sort_values(by='length', ascending=False)
    D_DOM2LTMDN=GB_D_DOM.median()

    D_DOM2LTSUM=GB_D_DOM.sum()    
    
    DOM2INFO['lifetime_avg_pagewise']=D_DOM2LTAVG['length']
    DOM2INFO['lifetime_std_pagewise']=D_DOM2LTSTD['length']
    DOM2INFO['lifetime_median_pagewise']=D_DOM2LTMDN['length']

    # DOM2INFO['lifetime_sum_pagewise']=D_DOM2LTSUM['length'] renamed permanence_sum...

    DOM2INFO['lifetime_nrev_avg_pagewise']=D_DOM2LTAVG['length_nrev']
    DOM2INFO['lifetime_nrev_std_pagewise']=D_DOM2LTSTD['length_nrev']
    DOM2INFO['lifetime_nrev_median_pagewise']=D_DOM2LTMDN['length_nrev']

    # DOM2INFO['lifetime_nrev_sum_pagewise']=D_DOM2LTSUM['length_nrev'] renamed permanence_sum...
    
    
    #######################################################################à Permanence
    
    DOM2INFO['permanence_sum_pagewise']=D_DOM2LTSUM['length']
    DOM2INFO['permanence_nrev_sum_pagewise']=D_DOM2LTSUM['length_nrev']
    DOM2INFO['norm_permanence_sum_pagewise']=DOM2INFO['permanence_sum_pagewise']/TOT_DAYS
    DOM2INFO['norm_permanence_nrev_sum_pagewise']=DOM2INFO['permanence_nrev_sum_pagewise']/TOT_NREV
    
    PD2PERMANENCE_DF=D_LT_DF.groupby(['page','domain']).sum().reset_index()
    PD2PERMANENCE_DF['perennial']=PD2PERMANENCE_DF['domain'].map(lambda x: D2perennial[x])
    PD2PERMANENCE_DF['mbfc']     =PD2PERMANENCE_DF['domain'].map(lambda x: D2mbfc[x])

    PD2PERMANENCE_DF['permanence']=PD2PERMANENCE_DF['length']
    PD2PERMANENCE_DF['norm_permanence']=PD2PERMANENCE_DF.apply(lambda x: x['permanence']/max(1,p2time[x['page']]), axis=1)

    PD2PERMANENCE_DF['permanence_nrev']=PD2PERMANENCE_DF['length_nrev']
    PD2PERMANENCE_DF['norm_permanence_nrev']=PD2PERMANENCE_DF.apply(lambda x: x['permanence_nrev']/p2nrev[x['page']], axis=1)

    PD2PERMANENCE_DF['pd']=PD2PERMANENCE_DF['page']+'$'+PD2PERMANENCE_DF['domain']

    PD2PERMANENCE_DF['age']=PD2PERMANENCE_DF['pd'].map(lambda x: pD2age[x])
    PD2PERMANENCE_DF['age_nrev']=PD2PERMANENCE_DF['pd'].map(lambda x: pD2age_nrev[x])

    PD2PERMANENCE_DF['selfnorm_permanence']=PD2PERMANENCE_DF.apply(
        lambda x: x['permanence']/(x['age'] if x['age']>0 else 1), axis=1)
    PD2PERMANENCE_DF['selfnorm_permanence_nrev']=PD2PERMANENCE_DF.apply(
        lambda x: x['permanence_nrev']/(x['age_nrev'] if x['age_nrev']>0 else 1), axis=1)

    PD2PERMANENCE_DF=PD2PERMANENCE_DF.drop(columns=['pd']) #temporary column created in this cell

    ### scores computed only on the lifetimes that are currently present
    CURR_PD2PERMANENCE_DF = CURR_D_LT_DF.groupby(['page', 'domain']).sum().reset_index()
    CURR_PD2PERMANENCE_DF['pd'] = CURR_PD2PERMANENCE_DF['page'] + '&' + CURR_PD2PERMANENCE_DF['domain']  # Change & to $ here
    CURR_PD2PERMANENCE_DF = CURR_PD2PERMANENCE_DF.set_index('pd')
    CURR_PD2PERMANENCE_DF = CURR_PD2PERMANENCE_DF.rename(columns={'length': 'curr_permanence'})
    # CURR_PD2PERMANENCE_DF['curr_norm_permanence'] = CURR_PD2PERMANENCE_DF.apply(lambda x: x['curr_permanence'] / max(1, p2time[x['page']]), axis=1)
    CURR_PD2PERMANENCE_DF = CURR_PD2PERMANENCE_DF.rename(columns={'length_nrev': 'curr_permanence_nrev'})
    # CURR_PD2PERMANENCE_DF['curr_norm_permanence_nrev'] = CURR_PD2PERMANENCE_DF.apply(lambda x: (x['curr_permanence_nrev'] + x['current']) / p2nrev[x['page']], axis=1)

    ### current pD but globally computed features
    GLOBCURR_PD2PERMANENCE_DF=PD2PERMANENCE_DF[PD2PERMANENCE_DF['current']==1]
    GLOBCURR_PD2PERMANENCE_DF['pd']=GLOBCURR_PD2PERMANENCE_DF['page']+'&'+GLOBCURR_PD2PERMANENCE_DF['domain']
    GLOBCURR_PD2PERMANENCE_DF=GLOBCURR_PD2PERMANENCE_DF.set_index('pd')
    
    ####################-------------------------------- adding this info
    DOM2INFO['permanence_avg_pagewise']   =PD2PERMANENCE_DF.groupby('domain').mean()['permanence']
    DOM2INFO['permanence_median_pagewise']=PD2PERMANENCE_DF.groupby('domain').median()['permanence']

    DOM2INFO['norm_permanence_avg_pagewise']   =PD2PERMANENCE_DF.groupby('domain').mean()['norm_permanence']
    DOM2INFO['norm_permanence_median_pagewise']=PD2PERMANENCE_DF.groupby('domain').median()['norm_permanence']

    DOM2INFO['selfnorm_permanence_avg_pagewise']   =PD2PERMANENCE_DF.groupby('domain').mean()['selfnorm_permanence']
    DOM2INFO['selfnorm_permanence_median_pagewise']=PD2PERMANENCE_DF.groupby('domain').median()['selfnorm_permanence']

    DOM2INFO['permanence_nrev_avg_pagewise']   =PD2PERMANENCE_DF.groupby('domain').mean()['permanence_nrev']
    DOM2INFO['permanence_nrev_median_pagewise']=PD2PERMANENCE_DF.groupby('domain').median()['permanence_nrev']

    DOM2INFO['norm_permanence_nrev_avg_pagewise']   =PD2PERMANENCE_DF.groupby('domain').mean()['norm_permanence_nrev']
    DOM2INFO['norm_permanence_nrev_median_pagewise']=PD2PERMANENCE_DF.groupby('domain').median()['norm_permanence_nrev']

    DOM2INFO['selfnorm_permanence_nrev_avg_pagewise']   =PD2PERMANENCE_DF.groupby('domain').mean()['selfnorm_permanence_nrev']
    DOM2INFO['selfnorm_permanence_nrev_median_pagewise']=PD2PERMANENCE_DF.groupby('domain').median()['selfnorm_permanence_nrev']

    DOM2INFO['curr_permanence_sum_pagewise']=CURR_PD2PERMANENCE_DF.groupby('domain').sum()['curr_permanence']
    DOM2INFO['curr_permanence_nrev_sum_pagewise']=CURR_PD2PERMANENCE_DF.groupby('domain').sum()['curr_permanence_nrev']

    DOM2INFO['norm_curr_permanence_sum_pagewise']=DOM2INFO['curr_permanence_sum_pagewise']/TOT_DAYS
    DOM2INFO['norm_curr_permanence_nrev_sum_pagewise']=DOM2INFO['curr_permanence_nrev_sum_pagewise']/TOT_NREV
    
    DOM2INFO['curr_permanence_avg_pagewise']=CURR_PD2PERMANENCE_DF.groupby('domain').mean()['curr_permanence']
    DOM2INFO['curr_permanence_nrev_avg_pagewise']=CURR_PD2PERMANENCE_DF.groupby('domain').mean()['curr_permanence_nrev']
    # FLAG
    DOM2INFO['curr_selfnorm_permanence_avg_pagewise']=GLOBCURR_PD2PERMANENCE_DF.groupby('domain').mean()['selfnorm_permanence']
    DOM2INFO['curr_selfnorm_permanence_nrev_avg_pagewise']=GLOBCURR_PD2PERMANENCE_DF.groupby('domain').mean()['selfnorm_permanence_nrev']
    
    for c in DOM2INFO:
        if not 'curr' in c: continue
        DOM2INFO[c].fillna(0, inplace=True)
        
    ######################################################## Controversy scores
    PD2INV_SCORES=URL2INFO.groupby(['page','final_domain']).sum().reset_index()\
    # .drop(columns=['page_id','revid','revid_parent','flags','user_id'])
    
    PD2INV_SCORES['pd']   =PD2INV_SCORES['page']   +'&'+PD2INV_SCORES['final_domain']
    PD2PERMANENCE_DF['pd']=PD2PERMANENCE_DF['page']+'&'+PD2PERMANENCE_DF['domain']
    
    if CONTRO_SCORES_FOUND:
        PD2INV_SCORES['inv_score_globnorm']=PD2INV_SCORES['involved_count']/PD2INV_SCORES['permanence'].map(lambda x: 1 if x<=0 else x)
        PD2INV_SCORES['inv_score_globnorm_nrev']=PD2INV_SCORES['involved_count']/PD2INV_SCORES['permanence_nrev'].map(lambda x: 1 if x<=0 else x)
        
    if CONTRO_SCORES_FOUND:
        PD2INV_SCORES['edit_count']=PD2INV_SCORES['involved_count']+PD2INV_SCORES['n_life']

    if CONTRO_SCORES_FOUND:
        PD2INV_SCORES['edit_score_globnorm']=PD2INV_SCORES['edit_count']/PD2INV_SCORES['permanence'].map(lambda x: 1 if x<=0 else x)
        PD2INV_SCORES['edit_score_globnorm_nrev']=PD2INV_SCORES['edit_count']/PD2INV_SCORES['permanence_nrev'].map(lambda x: 1 if x<=0 else x)

    if CONTRO_SCORES_FOUND:
        PD2INV_SCORES.groupby('final_domain').sum().sort_values(by='involved_count', ascending=False)

    if CONTRO_SCORES_FOUND:
        DOM2INV_SUM=URL2INFO.groupby('final_domain').sum()['involved_count'].to_dict() #the same for pd and pu
        DOM2INV_SUM={dom: DOM2INV_SUM.get(dom, 0) for dom in DOMAINS}

        DOM2INFO['inv_count_sum']=pd.DataFrame.from_dict(DOM2INV_SUM, orient='index')

    if CONTRO_SCORES_FOUND:
        PD_GB_INV=PD2INV_SCORES.groupby('final_domain')

        # domain level aggregate
        DOM2INV_PD_AVG=PD_GB_INV.mean()['inv_score_globnorm']
        DOM2INV_PD_AVG={dom: DOM2INV_PD_AVG.get(dom, 0) for dom in DOMAINS}
        # domain level aggregate
        DOM2INV_PD_AVG_NREV=PD_GB_INV.mean()['inv_score_globnorm_nrev']
        DOM2INV_PD_AVG_NREV={dom: DOM2INV_PD_AVG_NREV.get(dom, 0) for dom in DOMAINS}

    if CONTRO_SCORES_FOUND:
        DOM2INFO['inv_score_globnorm_avg_pagewise'] =pd.DataFrame.from_dict(DOM2INV_PD_AVG, orient='index')
        DOM2INFO['inv_score_nrev_globnorm_avg_pagewise'] =pd.DataFrame.from_dict(DOM2INV_PD_AVG_NREV, orient='index')

    if CONTRO_SCORES_FOUND:
        # domain level aggregate
        DOM2EDIT_PD_AVG=PD_GB_INV.mean()['edit_score_globnorm']
        DOM2EDIT_PD_AVG={dom: DOM2EDIT_PD_AVG.get(dom, 0) for dom in DOMAINS}
        # domain level aggregate
        DOM2EDIT_PD_AVG_NREV=PD_GB_INV.mean()['edit_score_globnorm_nrev']
        DOM2EDIT_PD_AVG_NREV={dom: DOM2EDIT_PD_AVG_NREV.get(dom, 0) for dom in DOMAINS}

    if CONTRO_SCORES_FOUND:
        DOM2INFO['edit_score_globnorm_avg_pagewise'] =pd.DataFrame.from_dict(DOM2EDIT_PD_AVG, orient='index')
        DOM2INFO['edit_score_nrev_globnorm_avg_pagewise'] =pd.DataFrame.from_dict(DOM2EDIT_PD_AVG_NREV, orient='index')

    if CONTRO_SCORES_FOUND:
        PU_GB_INV=URL2INFO.groupby('final_domain')

        # url aggregate
        DOM2INV_PU_AVG=PU_GB_INV.mean()['inv_score_globnorm']
        DOM2INV_PU_AVG={dom: DOM2INV_PU_AVG.get(dom, 0) for dom in DOMAINS}
        # url aggregate
        DOM2INV_PU_AVG_NREV=PU_GB_INV.mean()['inv_score_globnorm_nrev']
        DOM2INV_PU_AVG_NREV={dom: DOM2INV_PU_AVG_NREV.get(dom, 0) for dom in DOMAINS}

    if CONTRO_SCORES_FOUND:
        DOM2INFO['inv_score_globnorm_avg_urlwise']=pd.DataFrame.from_dict(DOM2INV_PU_AVG, orient='index')
        DOM2INFO['inv_score_nrev_globnorm_avg_urlwise']=pd.DataFrame.from_dict(DOM2INV_PU_AVG_NREV, orient='index')

    if CONTRO_SCORES_FOUND:
        PU_GB_INV=URL2INFO.groupby('final_domain')

        # url aggregate
        DOM2EDIT_PU_AVG=PU_GB_INV.mean()['edit_score_globnorm']
        DOM2EDIT_PU_AVG={dom: DOM2EDIT_PU_AVG.get(dom, 0) for dom in DOMAINS}
        # url aggregate
        DOM2EDIT_PU_AVG_NREV=PU_GB_INV.mean()['edit_score_globnorm_nrev']
        DOM2EDIT_PU_AVG_NREV={dom: DOM2EDIT_PU_AVG_NREV.get(dom, 0) for dom in DOMAINS}

    if CONTRO_SCORES_FOUND:
        DOM2INFO['edit_score_globnorm_avg_urlwise']=pd.DataFrame.from_dict(DOM2EDIT_PU_AVG, orient='index')
        DOM2INFO['edit_score_nrev_globnorm_avg_urlwise']=pd.DataFrame.from_dict(DOM2EDIT_PU_AVG_NREV, orient='index')
    
    ##################################################################### SAVING DOM2INFO
    if ALLOW_SAVE:
        DOM2INFO.to_csv(PATHNAME_DOM)
    
    return 0
    
    
    