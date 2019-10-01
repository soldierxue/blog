import json
import boto3
import pprint
import time
import os.path

ec2types_file = "ec2types.json"
ec2price_file_base = "ec2price.json"
FILE_NAME_FORMAT = "{}_{}_{}"
pricing_client = boto3.client('pricing', region_name='us-east-1')
pp = pprint.PrettyPrinter(indent=1, width=300)  

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

def load_json_from_file(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as json_file:
            json_data = json.load(json_file)
            return json_data
    return {}

def save_json_to_file(jsonObj,filename):
    with open(filename, 'w') as outfile:
        json.dump(jsonObj, outfile,cls=SetEncoder)

def get_local_ec2instypes(tenancy='Shared'):
    fileToRead = FILE_NAME_FORMAT.format("",tenancy,ec2types_file)
    ec2types = load_json_from_file(fileToRead)
    # pp.pprint(tenancy)
    for k,v in ec2types.items():
        numberOfEC2 = len(v)
        pp.pprint(k+":"+str(numberOfEC2))
    return ec2types

def get_lastest_ec2instypes(tenancy='Shared'):
    paginator = pricing_client.get_paginator('get_products')
    response_iterator = paginator.paginate(
        ServiceCode="AmazonEC2",
        Filters=[
            {
                'Type': 'TERM_MATCH',
                'Field': 'productFamily',
                'Value': 'Compute Instance'
            },
            {
                'Type': 'TERM_MATCH',
                'Field': 'operatingSystem',
                'Value': 'Linux'
            },
            {
                'Type': 'TERM_MATCH',
                'Field': 'tenancy',
                'Value': tenancy
            },
            {
                'Type':'TERM_MATCH',
                "Field":'preInstalledSw',
                "Value":'NA'
            },
            {
                'Type':'TERM_MATCH',
                "Field":'capacitystatus',
                "Value":'Used'
            }
        ],
        PaginationConfig={
            'PageSize': 100
        }
    )
    instanceTypesByRegion = {}
    for response in response_iterator:
        for priceItem in response["PriceList"]:
            priceItemJson = json.loads(priceItem)
            product = priceItemJson['product']
            regionName = product['attributes']['location']
            instanceType = product['attributes']['instanceType']
            sku = product['sku']
            if regionName in instanceTypesByRegion:
                instanceTypesByRegion[regionName].add(instanceType)
            else:
                instanceTypesByRegion[regionName] = set()
    fileToSave = FILE_NAME_FORMAT.format("",tenancy,ec2types_file)
    with open(fileToSave, 'w') as outfile:
        json.dump(instanceTypesByRegion, outfile,cls=SetEncoder)
    # pp = pprint.PrettyPrinter(indent=1, width=300)            
    # pp.pprint(instanceTypesByRegion)
    
def get_ec2price_linux(region='US West (N. California)',tenancy='Shared'):
    paginator = pricing_client.get_paginator('get_products')

    response_iterator = paginator.paginate(
        ServiceCode="AmazonEC2",
        Filters=[
            {
                'Type': 'TERM_MATCH',
                'Field': 'location',
                'Value': region
            },
            {
                'Type': 'TERM_MATCH',
                'Field': 'operatingSystem',
                'Value': 'Linux'
            },
            {
                'Type': 'TERM_MATCH',
                'Field': 'tenancy',
                'Value': tenancy
            },
            {
                'Type':'TERM_MATCH',
                "Field":'preInstalledSw',
                "Value":'NA'
            },
            {
                'Type':'TERM_MATCH',
                "Field":'capacitystatus',
                "Value":'Used'
            }
        ],
        PaginationConfig={
            'PageSize': 100
        }
    ) 
    
    pp = pprint.PrettyPrinter(indent=1, width=300)
    
    products = []

    for response in response_iterator:
        for priceItem in response["PriceList"]:
            priceItemJson = json.loads(priceItem)
            
            sku = priceItemJson['product']['sku']
            # pp.pprint("Sku="+sku)
            termsJson = priceItemJson['terms']
            
            odPriceHourly =''
            riPricingOptions = []
            for k,v in termsJson.items():
                # pp.pprint(k)
                if k == 'OnDemand':
                    # pp.pprint(v)
                    for k2,v2 in v.items():
                        # pp.pprint(k2)
                        if k2.startswith(sku):
                            pd = {}
                            if 'priceDimensions' in v2:
                                pd = v2['priceDimensions']
                                for k3,v3 in pd.items():
                                    if v3['unit'] == 'Hrs':
                                        odPriceHourly = v3['pricePerUnit']['USD']
                                        # pp.pprint(odPriceHourly)
                                        break
                            break
                elif k == 'Reserved':
                    for k1,v1 in v.items():
                        # pp.pprint(k1)
                        term =  v1['termAttributes']['LeaseContractLength']#1yr 3yr
                        convertable = v1['termAttributes']['OfferingClass']#starndard or convertible
                        upfront = v1['termAttributes']['PurchaseOption']#none, 'Partial Upfront' or 'All Upfront'
                        upfrontFee = ''
                        pricePerUnit = ''
                        riPriceHourly = ''
                        for k3,v3 in v1['priceDimensions'].items():
                            if v3['unit'] == 'Quantity':
                                upfrontFee=v3['pricePerUnit']['USD']
                            elif v3['unit'] == 'Hrs':
                                pricePerUnit=v3['pricePerUnit']['USD']
                        
                        hoursUnit = 365 * 24 # By default, one year RI
                        if term == '3yr':
                            hoursUnit = hoursUnit * 3
                        
                        if upfront == 'All Upfront':
                            riPriceHourly = float(upfrontFee) / hoursUnit
                        elif upfront == 'Partial Upfront':
                            riPriceHourly = float(upfrontFee) / hoursUnit + float(pricePerUnit)
                        else:
                            riPriceHourly = pricePerUnit
                        
                        riPricingOptions.append({"riPriceHourly":str(riPriceHourly),"term":term,"convertable":convertable,"upfront":upfront,"upfrontFee":upfrontFee,"pricePerUnit":pricePerUnit})        
            
            newProd = {"sku":sku,"attrs":priceItemJson['product']['attributes'],"odPriceHourly":odPriceHourly,"ri":riPricingOptions}
            products.append(newProd)
    return products;

'''
GET ALL Price information for all regions
Parameter : tenancy ["Shared","Dedicated","Host"]
'''
def hourly_price_all_regions(tenancy = "Shared"):
    ec2types = get_local_ec2instypes(tenancy)
    if len(ec2types.items()) == 0:
        get_lastest_ec2instypes(tenancy)
        ec2types = get_local_ec2instypes(tenancy)
    
    for region,ec2instances in ec2types.items():
        pp.pprint(region+"_"+tenancy)
        products = get_ec2price_linux(region,tenancy)
        fileToSave = FILE_NAME_FORMAT.format(region,tenancy,ec2price_file_base)
        # Save to file
        save_json_to_file(products,fileToSave)
        
    

# if __name__ == '__main__':
#     # startTime = time.time()
#     # get_lastest_ec2instypes()
#     # get_lastest_ec2instypes("Dedicated")
#     # get_lastest_ec2instypes("Host")
#     # endTime = time.time()
#     # elapsed_time = endTime - startTime
#     # print time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
    
#     startTime = time.time()
#     hourly_price_all_regions()
#     endTime = time.time()
#     elapsed_time = endTime - startTime
#     print time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
