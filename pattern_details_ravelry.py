import json
import os
import pandas as pd
import requests

from pandas.io.json import json_normalize

def ravelry_get(url, params, json_object, transpose, output_object = 'dataframe') :

    ravelry_user = os.environ['RAVELRY_USER']
    ravelry_pw = os.environ['RAVELRY_PW']

    api_url = 'https://api.ravelry.com' + url

    response = requests.get(
      api_url,
      params = params,
      auth=(ravelry_user,ravelry_pw))

    response_json = response.json()

    if output_object == 'json' :

      response_df = response_json[json_object]

    else : # return dataframe
      # turn json into a string, convert to pandas df, transpose
      response_df = pd.read_json(json.dumps(response_json[json_object]))
      if transpose :
        response_df = response_df.transpose()

    return response_df

def ravelry_pattern_search(sub_category) :

    search_results = ravelry_get(
      url = '/patterns/search.json',
      params = {
      'pc': sub_category,
      'page_size' : 100
      },
      json_object = 'patterns',
      transpose = False)

    search_results['sub_category_permalink'] = sub_category

    return search_results

def ravelry_pattern_details(input_id) :

    return ravelry_get(
      url = '/patterns.json',
      params = {'ids' : input_id},
      json_object = 'patterns',
      transpose = True)


# Get a list of pattern categories for searching
pattern_categories_meta = ravelry_get(
  url = '/pattern_categories/list.json',
  params = None,
  json_object = 'pattern_categories',
  transpose = False)

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

### appendix
# cols that may need to be unpacked
# lists
#pattern_attributes - id/permalink (tags basically)
#pattern_needle_sizes
#yarn_weight

# def expand_json_attributes(column, item_index = None) :

#   if type(pattern_details.loc[0,column]) is list :

#     attributes = [json_normalize(item[item_index]) if item_index else json_normalize(item) for item in pattern_details[column]]
#     attributes = pd.concat(attributes, sort = False, ignore_index = True).add_prefix(column + '_')

#   else :
#     attributes = json_normalize(data = pattern_details[column]).add_prefix(column + '_')

#   return attributes

# categories = pd.DataFrame()
# for index, row in pattern_categories.iterrows() :

#     # isolate one nested dictionary (the category)
#     outer_children = pd.DataFrame(row.get('children'))
#     category = outer_children.get('permalink')

#     # isolate its nested dictionaries (the subcategory)
#     inner_children = outer_children.get('children')

#     # return each category + sub-category pair
#     for child in inner_children :
#         sub_category = child.get('permalink')
#         new_category = pd.DataFrame(data = {'category' : [category[0]], 'sub_category' : [sub_category]})
#         categories = categories.append(new_category, ignore_index = True)
