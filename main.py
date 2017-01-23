import urllib
import os.path
import zipfile
import pandas as pd
import numpy as np
from sklearn import linear_model
from scipy.stats import ks_2samp
import matplotlib.pyplot as plt
import json
import pickle
import matplotlib
matplotlib.style.use('ggplot')
from util_fns import progress_bar

def fit_mortality():
    """Loads WB indicators and AD financial aid flows and fits the
    infant mortality as a function of aid and other indicators to
    determine if aid is effective"""
    aid_data = pd.DataFrame(pickle.load(open('./ad-data/health.pkl', \
        'rb')))['SD']

    codes_to_open = ['SH.MED.PHYS.ZS', 'SE.ADT.LITR.ZS', \
    'SE.PRM.CMPT.ZS', 'SE.SEC.NENR', 'SE.XPD.TOTL.GD.ZS', \
    'SH.DYN.MORT']
    wb_data_list = [] # pd.DataFrame({'SD':[]})
    data = pd.DataFrame(index=range(1960, 2016))
    for code in codes_to_open:
        temp_data = pickle.load(open('./wb-data/' + code + \
            '.pkl', 'rb'))

        data[code] = np.nan
        if len(temp_data) > 0:
            data[code].loc[temp_data['SD']['year']] = \
                temp_data['SD'][code]

    data['health'] = np.nan
    data['health'].loc[aid_data.loc['health'].keys()] = \
        aid_data.loc['health'].values()
    # drop rows that contain nothing but np.nan
    data = data.dropna(how='all')




def _add_wb_entry(country_df, entry, code):
    """Adds World Bank information from 'entry' to dictionary 'data',
    checking first whether 'entry' is useable"""
    val = None
    date = None
    # check if value is useable or not
    try:
        val = float(entry['value'])
        date = int(entry['date'])
    except TypeError:
        return
    else:
        country_df.loc[date, code] = val


def get_wb_data():
    """Uses World Bank Open Data API to save data recording the
    evolution of select indicator variables over time


    Code descriptions:

    Mortality rate, under-5 (per 1,000 live births): SH.DYN.MORT
    Government expenditure on education, total (% of GDP): SE.XPD.TOTL.GD.ZS
    Net enrolment rate, secondary, both sexes (%): SE.SEC.NENR
    Primary completion rate, total (% of relevant age group): SE.PRM.CMPT.ZS
    Adult literacy rate, population 15+ years, both sexes (%): SE.ADT.LITR.ZS
    Physicians (per 1,000 people): SH.MED.PHYS.ZS
    """

    wb_topics = \
        json.load(urllib.urlopen('http://api.worldbank.org/topics?format=json'))[1]
    # education = 4; health = 8; poverty = 11

    wb_health_indicator_ids = \
        [item['id'] for item in \
        json.load(urllib.urlopen('http://api.worldbank.org/topic/8/indicator?format=json&per_page=600'))[1]]


    wb_iso2_country_codes = {code['name']:code['iso2Code'] for code in \
        json.load(urllib.urlopen('http://api.worldbank.org/countries?format=json&per_page=400'))[1]}
    
    # countries_to_download = ';'.join([wb_iso2_country_codes['Brazil'], \
    #     wb_iso2_country_codes['South Africa']])

    countries_to_download = [wb_iso2_country_codes['Brazil'], \
                             wb_iso2_country_codes['Argentina'], \
                             wb_iso2_country_codes['Peru'], \
                             wb_iso2_country_codes['Mexico'], \
                             wb_iso2_country_codes['Nigeria'], \
                             wb_iso2_country_codes['South Africa'], \
                             wb_iso2_country_codes['Zimbabwe'], \
                             wb_iso2_country_codes['Kenya'], \
                             wb_iso2_country_codes['Vietnam'], \
                             wb_iso2_country_codes['Indonesia'], \
                             wb_iso2_country_codes['Thailand']]

    codes_to_download = wb_health_indicator_ids # ['SH.MED.PHYS.ZS', 'SE.ADT.LITR.ZS', 'SE.PRM.CMPT.ZS', 'SE.SEC.NENR', 'SE.XPD.TOTL.GD.ZS', 'SH.DYN.MORT'] # descriptions above

    for country_iso2 in countries_to_download:
        filename = './country-data/' + country_iso2 + '.pkl' # name of file \
            # to dump pickle to
        
        country_df = None
        # start downloading if not already on disk
        if os.path.exists(filename):
            country_df = pd.read_pickle(filename)
        else:
            country_df = pd.DataFrame(index=range(1960,2017))
        

        for code in codes_to_download:

            # skip if code already in column of country df
            if code not in country_df.columns.values:
                print 'Downloading data for code', code, \
                    'in country', country_iso2

                query = 'http://api.worldbank.org/countries/' + \
                        country_iso2 + '/indicators/' + code + \
                        '?date=1960:2016&format=json&per_page=1000&page=1' \
                        # url for first page of data 
                json_data = json.load(urllib.urlopen(query))
                # separate json into metadata and actual useable indicator values
                metadata = json_data[0]
                indicator_data = json_data[1]
                
                # ensure there is data for this country and code, else
                # skip
                if indicator_data is not None:
                    # zero out the sector column for this country
                    country_df.loc[:,code] = np.nan


                    # get number of pages
                    ntotal_pages = metadata['pages']
                    nresults_per_page = metadata['per_page']
                    ntotal_results = metadata['total'] # may not be nresults_per_page*ntotal_pages as last page may contain fewer results

                    # load data from first page
                    for entry in indicator_data:
                        _add_wb_entry(country_df, entry, code)

                    # load data from all the other pages
                    current_page = 1
                    while current_page < ntotal_pages:
                        progress_bar(current_page, ntotal_pages)
                        current_page += 1
                        page_index = query.find('page=')
                        query = query[:page_index+5] + str(current_page) # append relevant page number to query


                        error = True
                        while error:
                            try:
                                json_data = \
                                    json.load(urllib.urlopen(query))
                            except ValueError:
                                print 'Could not load url, trying again'
                                continue
                            else:
                                error = False

                        json_data = json.load(urllib.urlopen(query))
                        indicator_data = json_data[1]

                        # load data from first page
                        for entry in indicator_data:
                            _add_wb_entry(country_df, entry, code)
                    print ''
                else:
                    print 'Skipping code', code, 'for country' \
                        , country_iso2, 'due to lack of data'

            else:
                print 'Skipping code', code, 'for country', country_iso2

            country_df.to_pickle(filename)

def _add_ad_entry(country_df, entry, sector):
    """Adds AidData information from 'entry' to dictionary 'data',
    checking first whether 'entry' is useable"""

    for transaction in entry['transactions']:
        tr_val = None
        tr_year = None
        # check if value is useable or not
        try:
            tr_val = transaction['tr_constant_value']
            tr_year = transaction['tr_year']
        except TypeError:
            continue
        else:
            if np.isnan(country_df.loc[tr_year, sector]):
                country_df.loc[tr_year, sector] = tr_val
            else:
                country_df.loc[tr_year, sector] += tr_val

def get_ad_data():
    """Uses AidData API to save data recording the aid projects in a
    given country over time


    Code descriptions:

    """
    country_codes = \
        {code['name']:(code['iso2'], code['id']) for code in \
         json.load(urllib.urlopen('http://api.aiddata.org/data/destination/organizations'))['hits']}

    countries_to_download = [country_codes['Brazil'], \
                             country_codes['Argentina'], \
                             country_codes['Peru'], \
                             country_codes['Mexico'], \
                             country_codes['Nigeria'], \
                             country_codes['South Africa'], \
                             country_codes['Zimbabwe'], \
                             country_codes['Kenya'], \
                             country_codes['Viet Nam'], \
                             country_codes['Indonesia'], \
                             country_codes['Thailand']]

    
    aid_codes = \
        json.load(urllib.urlopen('http://api.aiddata.org/data/sectors/3')) # 3-digit codes and corresponding names for all projects

    for country_iso2, country_id in countries_to_download:

        filename = './country-data/' + str(country_iso2) + '.pkl'

        country_df = None
        # start downloading if not already on disk
        if os.path.exists(filename):
            country_df = pd.read_pickle(filename)
        else:
            country_df = pd.DataFrame(index=range(1960,2017))

        sectors = ['health' , 'education'] #, 'economics']
        for sector in sectors:

            # if column for sector is already in the dataframe, do not
            # add it again
            if sector not in country_df.columns.values:
                print 'Downloading data for', sector, \
                    'sector in country', country_iso2
                

                # zero out the sector column for this country
                country_df.loc[:,sector] = np.nan

                sector_codes = ','.join([code['code'] for code in \
                    aid_codes['hits'] if sector in code['name'].lower()]) \
                    # extracted sector codes


                years = ','.join([str(i) for i in range(1960,2017)])
                query = \
                    'http://api.aiddata.org/aid/project?t=1&y=' + years \
                    + '&ro=' + str(country_id) + '&size=50&sg=' \
                    + sector_codes + '&from=0'
                json_data = json.load(urllib.urlopen(query))
                ntotal_results = json_data['project_count']
                npages = ntotal_results/50 + 1 # total number of urls to \
                    # query based on ntotal_results and 50 results per page

                # add first page
                for entry in json_data['items']:
                    _add_ad_entry(country_df, entry, sector)

                current_page = 0
                while current_page < npages:
                    progress_bar(current_page, npages)
                    current_page += 1
                    current_starting_index = 50*current_page

                    # make new url based on updated starting index and load data
                    from_index = query.find('from=')
                    query = query[:from_index+5] + \
                        str(current_starting_index)
                    error = True
                    while error:
                        try:
                            json_data = \
                                json.load(urllib.urlopen(query))
                        except ValueError:
                            print 'Could not load url, trying again'
                            continue
                        else:
                            error = False
                    

                    # add entries to dictionary
                    for entry in json_data['items']:
                        _add_ad_entry(country_df, entry, \
                            sector)
                print ''

            else:
                print 'Skipping', sector, 'sector for country', country_iso2

        country_df.to_pickle(filename)
    

def analyze_data():
    wb_health_indicators = \
        {item['id']:item['name'] for item in \
        json.load(urllib.urlopen('http://api.worldbank.org/topic/8/indicator?format=json&per_page=600'))[1]}

    wb_health_indicators['health'] = 'health'

    brazil_df = pd.read_pickle('./country-data/BR.pkl')

    health_aid_years = ~np.isnan(brazil_df['health'])

    health_cols = []
    for col_name in brazil_df.columns.values:
        if not np.any(np.isnan(brazil_df.loc[health_aid_years, \
            col_name])):
            health_cols.append(col_name)

    health_cols.remove('SH.DYN.MORT')

    print 'Retained', len(health_cols), 'columns with which to ' + \
        'analyze health data'
    brazil_health_df = brazil_df.loc[health_aid_years]

    reg_engine = linear_model.LinearRegression()
    reg_engine.fit(brazil_health_df[health_cols], brazil_health_df['SH.DYN.MORT'])

    print zip(reg_engine.coef_, [wb_health_indicators[health_id] for \
        health_id in health_cols])

    

    fig = plt.figure()
    ax = fig.add_subplot(111)
    lw = 2
    fs = 36
    for h in health_cols:
        ax.plot(brazil_health_df.index, \
                brazil_health_df[h])
    ax.plot(brazil_health_df.index, brazil_health_df['SH.DYN.MORT'])
    ax.plot(brazil_health_df.index, \
            np.log10(brazil_health_df['health']))
    plt.show()

    # ax.set_xlim(right=2015)
    # ax.tick_params(labelsize=fs)
    # ax.legend(fontsize=fs)
    # ax.set_title('Childhood mortality over time (per 1,000 live births)', \
    #              fontsize=fs)
    # plt.show()
    
    


if __name__=='__main__':
    # analyze_data()
    get_wb_data()
    get_ad_data()
    # fit_mortality()
