import pandas as pd
import json
import requests

"""
1. Dataset with permutations of feature values
2. Feed to trained catboost model (predict probability)
3. create 'value' column by f(p) -> value (ex. f = lambda p: p * 3.25)
4. Drop probability column
5. Write to CSV to local machine
6. Call bid_list_lines_json_from_csv('filename', value='value', sep=',', show=True).
7. Delete Other bucket where it exists from output
8. Pass output to ttd_api_authenticate.post_bid_list along with authentication token

Expects column names in bid list

"""
def bid_list_json(bidlist_id, 
                  name, 
                  bidlist_source, 
                  bidlist_adjustment_type, 
                  resolution_type, 
                  bidlines_csv, 
                  bidlist_owner, 
                  bidlist_owner_id, 
                  is_available_for_library_use,
                  bidlist_type='BidAdjustment',
                  csv_value_col='value', 
                  csv_sep=',',
                  show=True):
    """
    This function orchestrates the creation of a payload JSON string 
    for a call to the TTD post: /bidlist endpoint
    """
    blj = {}
    blj["BidListId"] = bidlist_id
    blj["Name"] = name
    blj["BidListSource"] = bidlist_source
    blj["BidListAdjustmentType"] = bidlist_adjustment_type
    blj["ResolutionType"] = resolution_type
    blj["BidLines"] = bid_list_lines_json_from_csv(bidlines_csv, bidlist_type, csv_value_col, csv_sep)
    blj["BidListOwner"] = bidlist_owner
    blj["BidListOwnerId"] = bidlist_owner_id
    blj["IsAvailableForLibraryUse"] = is_available_for_library_use

    if show:
        print(json.dumps(blj, indent=4))
    return blj

def bid_list_lines_json_from_csv_batch(filename, start, numrows=10000, value='value', sep=',', show=False):
    """
    This function generates the BidLines field in a call to the post: /bidlist endpoint. Reads from a CSV
    """
    lines = []
    with open(filename, 'r') as f:

        # Capture header of file. Can do validation here
        header_cols = f.readline().strip().split(sep)

        # If the specified value column doesn't exist throw an error
        try:
            val_index =  header_cols.index(value)
        except ValueError as e:
            print(value in header_cols)
            raise ValueError(f"Value column '{value}' is not in file header. Value must be one of {', '.join(header_cols)}")
            


        # Read the rest of the file in one line at a time. i keeps track of ID
        i = 0
        while i < start:
            i += 1
            ln = f.readline()
            while ln :

                lineparts = ln.strip().split(sep)
                lines.append({})

                # Boilerplate bid line fields
                lines[-1]["BidLineId"] = i
                lines[-1]["BidAdjustment"] = float(lineparts[val_index])

                # Add value for each field for given line
                for idx, elt in enumerate(lineparts):
                    if idx == val_index:
                        continue
                    lines[-1][header_cols[idx]] = elt
#                i += 1
                if i >=start + numrows:
                    break
            # ln = f.readline()
    if show:
        print(json.dumps(lines, indent=4))

    return lines, i

if __name__ == '__main__':
    # bid_list_lines_json_from_csv('hourofdayvaluelist.csv', show=True)
    # bid_list_json("20211025_PM1", 
    #               "Sample Bid List", 
    #               "User", 
    #               "Optimized", 
    #               "ApplyMinimumAdjustment", 
    #               "hourofdayvaluelist.csv", 
    #               "Advertiser", 
    #               "wy4hdxa", 
    #               True)

    l, e = bid_list_lines_json_from_csv_batch(filename=filename, 
                                        start=0,
                                        numrows=10000,
                                        sep=',',
                                        show=True)
    print(l, e)


def bid_list_json_batch(bidlist_id, 
                  name, 
                  bidlist_source, 
                  bidlist_adjustment_type, 
                  resolution_type, 
                  bidlines_csv, 
                  bidlist_owner, 
                  bidlist_owner_id, 
                  is_available_for_library_use, 
                  numrows=10000,
                  csv_value_col='value', 
                  csv_sep=',',
                  show=True):
    """
    This function orchestrates the creation of a payload JSON string 
    for a call to the TTD post: /bidlist endpoint
    """

    bidlists = []

    i = 0
    while True:
        lines, end = bid_list_lines_json_from_csv_batch(filename=bidlines_csv,
                                                        start=i,
                                                        numrows=numrows, 
                                                        value=csv_value_col, 
                                                        sep=csv_sep, 
                                                        show=show)
        blj = {}
        blj["BidListId"] = bidlist_id
        blj["Name"] = name
        blj["BidListSource"] = bidlist_source
        blj["BidListAdjustmentType"] = bidlist_adjustment_type
        blj["ResolutionType"] = resolution_type
        blj["BidLines"] = lines
        blj["BidListOwner"] = bidlist_owner
        blj["BidListOwnerId"] = bidlist_owner_id
        blj["IsAvailableForLibraryUse"] = is_available_for_library_use
        bidlists.append(blj)
        if end < i + numrows:
            print('end:',end, 'i+numrows:', i+numrows)
            break
        i += numrows


        if show:
            print('showing:')
            print(json.dumps(blj, indent=4))
    return bidlists

def post_bid_list(token, payload):
  type = 'application/json'
  headers={'Content-Type': type, 'TTD-Auth': token}
  url = "https://api.thetradedesk.com/v3/bidlist"

  response = requests.post(url, headers=headers, json=payload)
  if response.ok:
    return response
  else:
    try:
        print(response.json())
    except Exception as e:
        print(json.dumps(payload, indent=4))
    response.raise_for_status()



def bid_list_lines_json_from_csv(filename, adjustment_type='BidAdjustment', value='value', sep=',', show=False):
    """
    This function generates the BidLines field in a call to the post: /bidlist endpoint. Reads from a CSV
    """
    lines = []
    with open(filename, 'r') as f:

        # Capture header of file. Can do validation here
        header_cols = f.readline().strip().split(sep)

        # If the specified value column doesn't exist throw an error
        try:
            val_index =  header_cols.index(value)
        except ValueError as e:
            print(value in header_cols)
            raise ValueError(f"Value column '{value}' is not in file header. Value must be one of {', '.join(header_cols)}")

        # Read the rest of the file in one line at a time. i keeps track of ID
        i = 0
        ln = f.readline()
        while ln:
            lineparts = ln.strip().split(sep)
            lines.append({})

            # Boilerplate bid line fields
            lines[-1]["BidLineId"] = i
            lines[-1][adjustment_type] = lineparts[val_index]
            
            if adjustment_type == 'VolumeControl':
                lines[-1]['BidAdjustment'] = 1

            # Add value for each field for given line
            for idx, elt in enumerate(lineparts):
                if idx == val_index:
                    continue
                lines[-1][header_cols[idx]] = elt
            ln = f.readline()
            i += 1
    if show:
        print(json.dumps(lines, indent=4))

    return lines

if __name__ == '__main__':
    # bid_list_lines_json_from_csv('hourofdayvaluelist.csv', show=True)
    bid_list_json("20211025_PM1", 
                  "Sample Bid List", 
                  "User", 
                  "Optimized", 
                  "ApplyMinimumAdjustment", 
                  "hourofdayvaluelist.csv", 
                  "Advertiser", 
                  "wy4hdxa", 
                  True)





# class BidList:
#     def __init__(self, features, value='value', )