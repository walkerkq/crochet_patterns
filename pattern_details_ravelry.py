import json
import os
import pandas as pd
import requests
from pandas.io.json import json_normalize

def ravelry_get(method, params) :
    '''
    Pass credentials and parameters to the Ravelry API
    Return a pandas Dataframe
    '''
    ravelry_user = os.environ['RAVELRY_USER']
    ravelry_pw = os.environ['RAVELRY_PW']

    api_url = 'https://api.ravelry.com' + method

    response = requests.get(
      api_url,
      params = params,
      auth=(ravelry_user,ravelry_pw))

    return response.json()

def ravelry_pattern_search(sub_category) :
    '''
    Search patterns in the Ravelry API using a sub-category filter
    Return a pandas DataFrame of pattern ids and basic details
    '''
    search_results = ravelry_get(
      method = '/patterns/search.json',
      params = {
      'pc': sub_category,
      'page_size' : 100
      })

    search_result_df = pd.read_json(json.dumps(search_results['patterns']))

    return search_result_df[['id', 'name', 'permalink']]

def ravelry_pattern_details(input_id) :
    '''
    Return pattern details by Ravelry pattern id
    '''
    pattern_result = ravelry_get(
      method = '/patterns.json',
      params = {'ids' : input_id})

    return json_normalize(pattern_result['patterns'][str(input_id)], max_level = 1)

# Get a list of pattern categories for searching
pattern_categories_meta = ravelry_get(
  method = '/pattern_categories/list.json',
  params = None)

# Unnest the sub_categories
pattern_categories = json_normalize(
  data = pattern_categories_meta['pattern_categories']['children'],
  record_path = 'children',
  meta = ['id', 'long_name', 'name', 'permalink'],
  record_prefix = 'sub_category_',
  meta_prefix = 'category_')

# Search for each category
pattern_search_results_list = [ravelry_pattern_search(item) for item in pattern_categories['sub_category_permalink']]
pattern_search_results = pd.concat(pattern_search_results_list, sort = False, ignore_index = True)

# Return pattern details
pattern_details_list = [ravelry_pattern_details(input_id) for input_id in pattern_search_results['id']]
pattern_details = pd.concat(pattern_details_list, sort = False, ignore_index = True)

# Write the df to csv
pattern_details.to_csv('pattern_details.csv', index = False)
