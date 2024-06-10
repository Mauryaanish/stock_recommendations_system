import pandas as pd
import numpy as np
from glob import glob
from datetime import datetime, timedelta
from tabulate import tabulate
import warnings
warnings.filterwarnings('ignore')

def pre_processing_current_data():
    # Define paths
    log_term_client_data_path = 'D:\\Stock Recommendation data\\long_term_client_name.csv'
    
    # Load data
    data = pd.read_html('https://www.moneycontrol.com/stocks/marketstats/bulk-deals/nse/',header=None )[0]
    data.columns = data.columns.droplevel(0)
    columns_name = {
    'Unnamed: 0_level_1' : 'Date',
    'Unnamed: 1_level_1' : 'Symbol',
    'Unnamed: 2_level_1' : 'ClientName',
    'Unnamed: 3_level_1' : 'Buy/Sell',
    'Unnamed: 4_level_1' : 'QuantityTraded',
    'Traded' : 'TradePrice/Wght.Avg.Price' ,
    'Closed' : 'Closed'
        }

    data.rename(columns_name , axis =1 , inplace = True)
    data.drop(['Closed'] , axis= 1 , inplace = True)

    
    log_term_client = pd.read_csv(log_term_client_data_path)
    
    # Filter data for long-term clients
    client_names = set(log_term_client['ClientName'])
    data = data[data['ClientName'].isin(client_names)].reset_index(drop=True)
    
    # Standardize and convert Buy/Sell column
    data['Buy/Sell'] = data['Buy/Sell'].str.upper().replace({'BUY': 1, 'SELL': -1})
    
    # Correct negative quantities for SELL transactions
    data['QuantityTraded'] = data['QuantityTraded'].abs() * data['Buy/Sell']
    
    # Convert date column to datetime
    data['Date'] = pd.to_datetime(data['Date'])
    
    # Calculate trade values
    data['Trade_values'] = data['QuantityTraded'] * data['TradePrice/Wght.Avg.Price']
    
    return data


def current_data_merge(current_data):
    # Path to the historical data CSV file
    historical_data_path = 'D:/Stock Recommendation data/Bulk_deal_data.csv'
    
    # Read the historical data
    historical_data = pd.read_csv(historical_data_path)
    
    # Convert 'Date' columns to datetime format
    historical_data['Date'] = pd.to_datetime(historical_data['Date'], format='%Y-%m-%d')
    current_data['Date'] = pd.to_datetime(current_data['Date'], format='%d-%m-%Y')
    
    # Check if there are any dates in current_data that are not in historical_data
    if not current_data['Date'].isin(historical_data['Date']).all():
        # Concatenate historical and current data
        main_data = pd.concat([historical_data, current_data], ignore_index=True)
        
        # Save the updated data back to the CSV file
        main_data.to_csv(historical_data_path, index=False)
    else:
        # If all dates in current_data are already in historical_data, return the historical data
        main_data = historical_data
    
    return main_data


def recommendations_system(data):
    today_date = datetime.today().date()
    previous_30 = today_date - timedelta(days=60)
    
    # Filter data for the last 30 days
    data['Date'] = pd.to_datetime(data['Date'])
    recent_data = data[data['Date'].dt.date > previous_30]

    # Group by 'SecurityName' and calculate total trade values
    position_data = recent_data.groupby('Symbol')['Trade_values'].sum().reset_index()
    
    Buy_record = []
    Sell_record = []

    for stock_name in position_data['Symbol'].unique():
        pos = recent_data[recent_data['Symbol'] == stock_name]
        total_value = round(pos['Trade_values'].sum(), 2)
        last_three_position = pos['Buy/Sell'].iloc[-3:].sum()

        if total_value > 0 and last_three_position >= 3:
            Buy_record.append({
                'Single': 'Buy',
                'Last Position Date': pos.iloc[-1]['Date'],
                'Stock Name': pos.iloc[-1]['Symbol'],
                'Last Position Client Name': pos.iloc[-1]['ClientName'],
                'Last Position Trade Price': pos.iloc[-1]['TradePrice/Wght.Avg.Price'],
                'Last Position Quantity': pos.iloc[-1]['QuantityTraded'],
                'Total Buy Trade value': total_value
            })
        elif total_value < 0 and last_three_position <= -3:
            Sell_record.append({
                'Single': 'Sell',
                'Last Position Date': pos.iloc[-1]['Date'],
                'Stock Name': pos.iloc[-1]['Symbol'],
                'Last Position Client Name': pos.iloc[-1]['ClientName'],
                'Last Position Trade Price': pos.iloc[-1]['TradePrice/Wght.Avg.Price'],
                'Last Position Quantity': pos.iloc[-1]['QuantityTraded'],
                'Total Sell Trade value': total_value
            })

    # Create DataFrames from records
    buy = pd.DataFrame(Buy_record)
    sell = pd.DataFrame(Sell_record)

    # Sort and filter records by date
    if not buy.empty:
        buy = buy.sort_values(by='Last Position Date').reset_index(drop=True)
        buy = buy[buy['Last Position Date'].dt.date > previous_30]

    if not sell.empty:
        sell = sell.sort_values(by='Last Position Date').reset_index(drop=True)
        sell = sell[sell['Last Position Date'].dt.date > previous_30]

    # Convert to list of dictionaries
    Buy_recommendations = buy.to_dict(orient='records') if not buy.empty else []
    Sell_recommendations = sell.to_dict(orient='records') if not sell.empty else []

    return Buy_recommendations, Sell_recommendations


if __name__ == '__main__':
    data_record_path = 'D:/Stock Recommendation data/Recommendation_stock_data.csv'
    data = pre_processing_current_data()
    main_data = current_data_merge(current_data=data)
    Buy_recommendations, Sell_recommendations = recommendations_system(data=main_data)
    today_date_str = datetime.today().strftime("%d-%m-%Y")
    record = pd.read_csv(data_record_path)
    data_list = []
    print('************************BUY RECOMMENDATION SHARE****************************')
    for recommendation in Buy_recommendations:
        print('- Recommendation:', recommendation['Single'])
        print('  Last Position Date:', recommendation['Last Position Date'])
        print('  Stock Name:', recommendation['Stock Name'])
        print('  Last Position Client Name:', recommendation['Last Position Client Name'])
        print('  Last Position Trade Price:', recommendation['Last Position Trade Price'])
        print('  Last Position Quantity:', '{:,}'.format(recommendation['Last Position Quantity']))
        print('  Total Buy Trade value:', '{:,.2f}'.format(recommendation['Total Buy Trade value']))
        print()
        data_buy = {
            'Recommendation_Date' : today_date_str,
            'Recommendation' : recommendation['Single'],
            'Stock_Name' : recommendation['Stock Name'],
            'Client_Name' : recommendation['Last Position Client Name'],
            'Last_Position_Trade_Price' : recommendation['Last Position Trade Price'],
           'Last_Position_Quantity': '{:,}'.format(recommendation['Last Position Quantity']),
            'Total_Value' : '{:,.2f}'.format(recommendation['Total Buy Trade value']),
            'Last Position Date' : recommendation['Last Position Date'].date().strftime("%d-%m-%Y")
        }
        data_list.append(data_buy)
    print('************************SELL RECOMMENDATION SHARE****************************')
    for recommendation in Sell_recommendations:
        print('- Recommendation:', recommendation['Single'])
        print('  Last Position Date:', recommendation['Last Position Date'])
        print('  Stock Name:', recommendation['Stock Name'])
        print('  Last Position Client Name:', recommendation['Last Position Client Name'])
        print('  Last Position Trade Price:', recommendation['Last Position Trade Price'])
        print('  Last Position Quantity:', '{:,}'.format(recommendation['Last Position Quantity']))
        print('  Total Sell Trade value:', '{:,.2f}'.format(recommendation['Total Sell Trade value']))
        print()
        data_sell = {
            'Recommendation_Date' : today_date_str,
            'Recommendation' : recommendation['Single'],
            'Stock_Name' : recommendation['Stock Name'],
            'Client_Name' : recommendation['Last Position Client Name'],
            'Last_Position_Trade_Price' : recommendation['Last Position Trade Price'],
           'Last_Position_Quantity': '{:,}'.format(recommendation['Last Position Quantity']),
            'Total_Value' : '{:,.2f}'.format(recommendation['Total Sell Trade value']),
            'Last Position Date' : recommendation['Last Position Date'].date().strftime("%d-%m-%Y")
        }
        data_list.append(data_sell)
    data = pd.DataFrame(data_list)
    df = pd.concat([record , data] ,ignore_index=True)
    if not df['Recommendation_Date'].isin(record['Recommendation_Date']).all():
        df.to_csv(data_record_path , index= False)
    else:
        pass
    print('-----------------------------------------------------------------------------')
