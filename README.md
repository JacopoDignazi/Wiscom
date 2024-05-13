This repository contains all code necessary for running the Wiscom project collection and feature extraction. Before running it, make sure to install a correct environment using the .yml file in requirements folder
The code folder, contains the notebook for each step of the pipeline, where the .py files are source codes used in these. In data/paths.py there are functions responsible for managing input and output folder: make sure to select a suitable root folder and that each folder called required for output actually exists before running the code. 

The order of execution of notebooks is:
1) wpp_to_query_sources_revisions.ipynb
this notebook runs the revision data collection process for the pages in input file then save the all information in data/collected/english/ for english data and data/collected/other_languages/ for all other languages. This notebook uses the source code in utils_collection.py.
It can collect revision data for around 1000 pages a day, although timing can vary a lot due to popularity of different pages
Notes:
- takes in input data/collected/{file containing pages selection}
- gives in output several file like data/collected/{topic name}/{english|other_languages}/{filename} which will contain source revision data and metadata
- when running it, make sure the input file is set up correctly (as an existing one in the correct folder), and that the output folder exists.
2) fix_file.ipynb 
Since some formatting error can happend due to comments and titles in collected revisions, this file fixes possible mistakes using regular expression
Notes:
- both input and output occur in folder data/collected/{topic name}/{english|other_languages}/{filename}
- for safety, before fixing the file, it create a backup in the same input folder, adding suffix -backup to its name
3) source_revisions_to_url_info
This notebook reads all urls found in data/collected/{topic name}/{english|other_languages}/{filename} and analyse if the urls is still active and/or redirect somewhere else. It output the analysis results on data/urlsinfo/urlsinfo.csv
Notes:
- The process runs independently for each topic and for english/other_languages, so in has to be run one time for each collection iteration
4) urls_info_to_add_domain_axis.ipynb
This notebook takes data/urlsinfo/urlsinfo.csv to extract domain and assign Perennial/MBFC labels, then save this analysis in data/urlsinfo/urlsinfo_and_domain.csv
Notes:
- It reads the Perennial classification and MBFC labels from two separate files in data/urlsinfo/
5) aggregate_source_identifiers.ipynb
This notebook reads source revision info, combine it with urls info and creates a mapping of urls to an equivalent url, that then save in data/collected/{topic name}/{english|other_languages}/. The equivalence is decided on rules based on url-redirect, url truncation and archive urls, making sure that different urls leading to the same page actually share the same "identifier" (an "equivalent" url indeed)
6) process_dataset.ipynb 
This notebook combine knowledge from urls info, source revision data and url equivalency to process all the features in our study. In the notebook, the topic of the extraction must be specified, which sets the input and output folder for the process. Ex for parameter PROJECT_NAME='Climate change' it will take as input revision data from data/collected/Climate change/ and output it to data/processed/CL/ ("CL" is a shorthand for that project name, specified in utils_domain.py )
Notes:
- once the topic is specified, the process run on its own for each language it finds in revision data, creating a folder for each language structured as data/processed/{project shorthand}/{language}/
- The process can create data/processed/{project shorthand}/{language}/ folders that remain empty, which happends if there is data collected for that language but no information about revisions - happends often on low resource languages
- For each topic and language, the file data/processed/{project shorthand}/{language}/dom2info_{topic}{lang}.csv contains the domain-feature dataset used in our work
