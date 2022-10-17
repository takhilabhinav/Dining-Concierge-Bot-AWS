import requests
import json
import os
from decimal import Decimal
import datetime
import boto3
from decimal import Decimal
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection

#Dictionary for checking for duplicate restaurants
restaurant_dict={}

def check_for_none(value):
        
    try:
        if value is None or len(str(value)) == 0:
            return True
        return False
    except:
        return True

def format_restaurants(restaurant, location, cuisine_type):
    """
    This Function is used to Restaurant Dictionary
  
    Parameters:
    restaurant (dictionary): Dictionary containing restaurant information
    
    location(string): The location of the restaurants
    
    cuisine_type(String): cuisine type of the restaurant
    
    Returns:
    restaurant_format(Dictionary): Formatted Dictionary  of restaurants
  
    """
    restaurant_format={}
    restaurant_dict[restaurant['id']]=1
    restaurant_format['id']=restaurant['id']
    restaurant_format['insertedAtTimestamp']=str(datetime.datetime.now())
    if cuisine_type=='indpak':
        cuisine_type='indian'
    restaurant_format['cuisine_type']=cuisine_type
    restaurant_format['name']=restaurant['name']
    restaurant_format['url']=restaurant['url']
    if not check_for_none(restaurant.get("rating",None)):
        restaurant_format["rating"] = Decimal(restaurant["rating"])
    if not check_for_none(restaurant.get("coordinates",None)):
        restaurant_format["latitude"]=Decimal(str(restaurant["coordinates"]["latitude"]))
        restaurant_format["longitude"]=Decimal(str(restaurant["coordinates"]["longitude"]))
    if not check_for_none(restaurant.get("phone",None)):
        restaurant_format["contact"] = restaurant["phone"]
    if not check_for_none(restaurant.get("review_count",None)):
        restaurant_format["review_count"] = restaurant["review_count"]
    if not check_for_none(restaurant.get("price",None)):
        restaurant_format["price"] = restaurant["price"]
    if restaurant.get('location', None) is not None:
        address=""
        for i in restaurant['location']['display_address']:
            address+=i
        restaurant_format['address']=address
        restaurant_format["zip_code"]=restaurant['location']['zip_code']
    return restaurant_format


def get_yelp_data(api,api_key):
    """
    This Function is used to scrap data from Yelp API
  
    Parameters:
    api (string): API of yelp
    
    api_key(string): API key for using Yelp API
    
    Returns:
    restaurant_list(list): returns list of restaurants
  
    """
    headers= {"Authorization": "Bearer " + api_key}
    cuisine_list=['indpak','italian','mexican','chinese','japanese','french','greek']
    location='manhattan'
    #list to store all the restaurant dictionaries
    restaurant_list=[]
    for cuisine in cuisine_list:
        responses_total=1000
        offset=0
        query= "?location={}".format(location)+"&categories={}".format(cuisine)+"&limit=50&offset="+str(offset)
        response= requests.get(api+query, headers=headers).json()
        while(responses_total>=0):
            if response.get("businesses", None) is not None:
                restaurants_in_current_page=response["businesses"]
                responses_in_current_page=len(restaurants_in_current_page)
                for restaurant in restaurants_in_current_page:
                    if restaurant['id'] in restaurant_dict:
                        #Checking for duplicate restaurants
                        continue
                    #formating restaurent dictionary
                    formatted_restaurant=format_restaurants(restaurant,location,cuisine)
                    restaurant_list.append(formatted_restaurant)
                responses_total=responses_total-responses_in_current_page
                if responses_in_current_page==0:
                    #checking if there are no restaurants in current page
                    break
                # updating offset to go to another page
                offset+=responses_in_current_page
                query= "?location={}".format(location)+"&categories={}".format(cuisine)+"&limit=50&offset="+str(offset)
                #going to another page
                response=requests.get(api+query, headers=headers).json()
            else:
                break
        
    return restaurant_list


def send_to_dynamodb(aws_access_key_id,aws_secret_access_key,region_name,restaurant_list):
    """
    This Function sends restaurant information into dynamodb
  
    Parameters:
    aws_access_key_id (string): AWS ACCESS key id
    
    aws_secret_access_key(string): AWS Secrete Access Key
    
    region_name(string): Region Name 
    
    restaurant_list(string): List of restaurant dictionaries
    """
    
    dynamodb = boto3.resource('dynamodb',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    table = dynamodb.Table('yelp-restraunts')
    for restaurant in restaurant_list:
        #sending the restaurant dictionaries to dynamodb
        table.put_item(Item=restaurant)
        
        
def send_to_es(restaurant_list,es_host,es_port,es_http_auth):
    """
    This Function sends restaurant information into Open Search Index
  
    Parameters:
    restaurant_list(string): List of restaurant dictionaries
    
    es_host (string): Endpoint of Open Search
    
    es_port(string): Port number of Open Search Endpoint
    
    es_http_auth(string): Tuple containing username and password
    """
    
    index_name = 'restaurants'
    index_body = {
        'settings': {
        'index': {
            'number_of_shards': 4
            }
        }
    }
    #Create an Open Search Client
    client = OpenSearch(
    hosts = [{'host': es_host, 'port': es_port}],
    http_auth = es_http_auth,
    use_ssl = True,
    verify_certs = True,
    connection_class=RequestsHttpConnection
)
    # Creating an index
    client.indices.create(index_name, body=index_body)
    for restaurant in restaurant_list:
        #Sending restaurant information to index
            client.index(index='restaurants', id=restaurant["id"],body={
                "cuisine" : restaurant["cuisine_type"],
            })

    
    

if __name__=='__main__':
    api_key = 'YyjDbQhunFps4GhAon4n-6b0ksZnajrkyFidNnYyMaLgnwr4trB8N6qc0jOBMtyCinD9Ytm7Lux2PTfBGna-fFzSqWoqBUxFutyCa4TS8_2DKls6rIA7--7DJzJHY3Yx'

    api='https://api.yelp.com/v3/businesses/search'
    restaurant_list=get_yelp_data(api,api_key)
    restaurant_len=len(restaurant_list)
    if restaurant_len>=5000:
      aws_access_key_id=''
      aws_secret_access_key=''
      region_name='us-east-1'
      send_to_dynamodb(aws_access_key_id,aws_secret_access_key,region_name,restaurant_list)
      es_host='search-restraunteat-axtv5ugylo65y6xkczhcwdol6y.us-east-1.es.amazonaws.com'
      es_port=443
      es_http_auth = ('abhinavdwarkani','Abhinav123*')
      send_to_es(restaurant_list,es_host,es_port,es_http_auth)    
