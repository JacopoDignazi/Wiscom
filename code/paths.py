GLOBAL_PATH='../data/'
# assumes code and data folders are inside the same folder

def get_path_wpp(proj):
    return GLOBAL_PATH+f'collected/{proj}_'

def get_path_collected(proj, lang):
    return GLOBAL_PATH+f'collected/{proj}/{lang}/'

def get_path_urlsinfo():
    return GLOBAL_PATH+f'urlsinfo/'

def get_path_processed(proj, lang):
    return GLOBAL_PATH+f'processed/{proj}/{lang}/'