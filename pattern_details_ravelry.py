import json
import os
import pandas as pd
import requests
from pandas.io.json import json_normalize

def ravelry_get(url, params, json_object, transpose = False) :
    '''
    Pass credentials and parameters to the Ravelry API
    Return a pandas Dataframe
    '''
    ravelry_user = os.environ['RAVELRY_USER']
    ravelry_pw = os.environ['RAVELRY_PW']

    api_url = 'https://api.ravelry.com' + url

    response = requests.get(
      api_url,
      params = params,
      auth=(ravelry_user,ravelry_pw))

    # convert response to json
    response_json = response.json()

    # convert json into a string, then to pandas df
    response_df = pd.read_json(json.dumps(response_json[json_object]))

    if transpose :
      response_df = response_df.transpose()

    return response_df

def ravelry_pattern_search(sub_category) :
    '''
    Search patterns in the Ravelry API using a sub-category filter
    Return a pandas DataFrame of pattern ids and basic details
    '''

    search_results = ravelry_get(
      url = '/patterns/search.json',
      params = {
      'pc': sub_category,
      'page_size' : 100
      },
      json_object = 'patterns')

    search_results['sub_category_permalink'] = sub_category

    return search_results

def ravelry_pattern_details(input_id) :
    '''
    Return pattern details by Ravelry pattern id
    '''

    return ravelry_get(
      url = '/patterns.json',
      params = {'ids' : input_id},
      json_object = 'patterns',
      transpose = True)


# Get a list of pattern categories for searching
pattern_categories_meta = ravelry_get(
  url = '/pattern_categories/list.json',
  params = None,
  json_object = 'pattern_categories')

# Unnest the sub_categories
pattern_categories = json_normalize(
  data = pattern_categories_meta['children'],
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

# Retain sub category name
pattern_details = pattern_search_results[['id', 'sub_category_permalink']].merge(pattern_details, on = 'id')
pattern_details = pattern_categories[['sub_category_permalink', 'category_permalink']].merge(pattern_details, on = 'sub_category_permalink')

## Unpack a few variables from json
pattern_details['craft_permalink'] = pattern_details['craft'].map(lambda x: x.get('permalink'))
pattern_details['author_permalink'] = pattern_details['pattern_author'].map(lambda x: x.get('permalink'))
pattern_details['author_id'] = pattern_details['pattern_author'].map(lambda x: x.get('id'))
pattern_details['author_patterns_count'] = pattern_details['pattern_author'].map(lambda x: x.get('patterns_count'))

# Drop redundant vars
pattern_details = pattern_details.drop(columns = ['craft', 'pattern_author'], axis = 1)

# Write the df to csv
pattern_details.to_csv('pattern_details.csv', index = False)
