### Define Bidlist Functions
import pandas as pd
from ttd_api_authenticate import *
from TradedeskCredentials import *
from bid_lists_generator import *

# load functions...
def bid_col (mydf, col, target, num):
    df1 = mydf.loc[mydf[target] == 1, [col, target]]
    df2 = df1.groupby(col).agg(np.count_nonzero).sort_values(by = target, ascending=False)
    df3 = df2.iloc[0:num,]
    df3 = df3.reset_index()
    df3['key'] = 0
    return df3
def df_list_make(train, col_list, targeted_id):
    df_list = []
    for i in col_list:
        j = bid_col (train, col = i, target = targeted_id, num = 100000)
        df_list.append(j)
    return df_list

def cross_join (df_list, col_list):
    lenth = len(df_list)-1
    i = 0
    mydf = df_list[i]
    while i < lenth:
        mydf = pd.merge(mydf, df_list[i+1], on= 'key')
        i = i+1
    mydf = mydf[col_list]
    return mydf

def apply_bid_lists_to_adgroups(token, bid_list_ids, adgroupids, enable_on_apply=True, 
                                on_error='continue', verbose=False):
    #on_error in ['continue', 'raise']
    fails = []
    results = []
    for i, adgroup in enumerate(adgroupids):
        if verbose:
            printstr = f"Adding {len(bid_list_ids)} to adgroup {adgroup}"
            print(printstr, end='\r')
            print(' '*len(printstr), end='\r')
        try:
            current_state = generic_get(f'adgroup/{adgroup}', token=token).json()
        except Exception as e:
            if on_error == 'continue':
                fails.append(adgroup)
                continue
            elif on_error == 'raise':
                raise ValueError(f"Error getting adgroup id '{adgroup}' (index {i})")
        
        current_bid_lists = current_state['AssociatedBidLists']
        current_bid_lists.extend([{'BidListId':x, 
                                'IsEnabled':enable_on_apply} for x in bid_list_ids])
        payload={'AdGroupId':adgroup, 'AssociatedBidLists':current_bid_lists}
        try:
            result = generic_put('adgroup', token=token, payload=payload).json()
            results.append({adgroup:result['AssociatedBidLists']})
        except Exception as e:
            if on_error == 'continue':
                fails.append(adgroup)
                continue
            elif on_error == 'raise':
                raise ValueError(f"Error updating adgroup id '{adgroup}' (index {i})")
        print('Uploaded Successfully')