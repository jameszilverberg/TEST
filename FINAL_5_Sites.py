import requests
from bs4 import BeautifulSoup
import timeit
from threading import Thread
import queue
import json
import PyPDF2
import urllib
import re
import os
import xlrd
import datetime
startTime = timeit.default_timer()
print('BlackRock Started')
##############################################################################
################################## BlackRock #################################
##############################################################################

############################ Parsing Data ############################

#Parsing json file from BlackRock website:
url = "http://www.blackrock.com/investing//product-list.jsn"
req_url = requests.get(url)
DATA_FIELD = "aaData"
json_data = json.loads(req_url.text)[DATA_FIELD]

#Prettifies json data:
#print(json.dumps(json_data, sort_keys=True, indent=4, separators=(',', ': ')))

#Retreiving PDF from BlackRock site and writing a file we can use: 
url_pdf = 'http://www.blackrock.com/investing/literature/investor-education/cef-municipal-closed-end-fund-data-usd-en-us.pdf'
file = requests.get(url_pdf)
filename= "BlackRockPDF.pdf"
output = open(filename, "wb")
output.write(file.content)
output.close()

#Parsing PDF file:
pdfFileObj = open("BlackRockPDF.pdf", 'rb')
pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
pageObj = pdfReader.getPage(0)
pageObj2 = pdfReader.getPage(1)
data = pageObj.extractText() + pageObj2.extractText()
pdfFileObj.close()
split_data = (data.split(" "))

############# Finding NAV, Closing Price, and Premium Discount #############

tickers = [BeautifulSoup(n['colTicker']).text for n in json_data for x in n if n[x] == "@3@31@311@3112@" or n[x] == "@3@31@311@3111@" or n[x] == "@3@31@312@3129@" or n[x] == "@3@31@312@3127@" or n[x] == "@3@31@312@3122@" or n[x] == "@3@31@312@31210@" or n[x] == "@3@31@312@3126@" or n[x] == "@3@31@312@3123@" or n[x] == "@3@31@312@3124@" or n[x] == "@3@31@312@3125@" or n[x] == "@3@31@312@3128@" or n[x] == "@3@31@312@3121@"]
nav = [n['colFundSeriesNavAmount'] for n in json_data for x in n if n[x] == "@3@31@311@3112@" or n[x] == "@3@31@311@3111@" or n[x] == "@3@31@312@3129@" or n[x] == "@3@31@312@3127@" or n[x] == "@3@31@312@3122@" or n[x] == "@3@31@312@31210@" or n[x] == "@3@31@312@3126@" or n[x] == "@3@31@312@3123@" or n[x] == "@3@31@312@3124@" or n[x] == "@3@31@312@3125@" or n[x] == "@3@31@312@3128@" or n[x] == "@3@31@312@3121@"]
price2 = [n['colClosingPrice'] for n in json_data for x in n if n[x] == "@3@31@311@3112@" or n[x] == "@3@31@311@3111@" or n[x] == "@3@31@312@3129@" or n[x] == "@3@31@312@3127@" or n[x] == "@3@31@312@3122@" or n[x] == "@3@31@312@31210@" or n[x] == "@3@31@312@3126@" or n[x] == "@3@31@312@3123@" or n[x] == "@3@31@312@3124@" or n[x] == "@3@31@312@3125@" or n[x] == "@3@31@312@3128@" or n[x] == "@3@31@312@3121@"]
pd = [n['colPremiumDiscount'] for n in json_data for x in n if n[x] == "@3@31@311@3112@" or n[x] == "@3@31@311@3111@" or n[x] == "@3@31@312@3129@" or n[x] == "@3@31@312@3127@" or n[x] == "@3@31@312@3122@" or n[x] == "@3@31@312@31210@" or n[x] == "@3@31@312@3126@" or n[x] == "@3@31@312@3123@" or n[x] == "@3@31@312@3124@" or n[x] == "@3@31@312@3125@" or n[x] == "@3@31@312@3128@" or n[x] == "@3@31@312@3121@"]


         
############## Finding Average Volume, Duration and Exposures ################          

ticker_urls = ['http://www.blackrock.com' + x.split('"')[1] for x in [n['colTicker'] for n in json_data for x in n if n[x] == "@3@31@311@3112@" or n[x] == "@3@31@311@3111@" or n[x] == "@3@31@312@3129@" or n[x] == "@3@31@312@3127@" or n[x] == "@3@31@312@3122@" or n[x] == "@3@31@312@31210@" or n[x] == "@3@31@312@3126@" or n[x] == "@3@31@312@3123@" or n[x] == "@3@31@312@3124@" or n[x] == "@3@31@312@3125@" or n[x] == "@3@31@312@3128@" or n[x] == "@3@31@312@3121@"]]



q = queue.LifoQueue()

def multiple_url_parser(multi_url):
    for x in multi_url:
        q.put(x)

multiple_url_parser(ticker_urls)

fund_name = []
ave_vol = []
duration = []
shares = []
ill = []
nj = []
pr = []
tob = []
health = []
prere = []
house = []
st_go = []
loc_go = []
maturity03 = []
def grab_data_from_queue():
    
    while not q.empty(): # check that the queue isn't empty
        
        url = q.get() # get the item from the queue

        url_req = requests.get(url)
        soup = BeautifulSoup(url_req.content)
    
        for k in soup.find_all('div', {'id':'identifierWrapper'}):
            for i in k.find_all('p'):
                fund_name.append(i.text)
        
        for k in soup.find_all('tr', {'class':'visible-data threeMonAvgVol'}):
            for i in k.find_all('td', {'class':'data'}):
                ave_vol.append(i.text)
        
        for k in soup.find_all('tr', {'class':'visible-data effectiveDuration'}):
            for i in k.find_all('td', {'class':'data'}):
                duration.append(i.text)
            
        label = []
        values = []
      
        for k in soup.find_all('dt', {'class':'label '}):
            label.append(k.text)

        for k in soup.find_all('dt', {'class':'label active-data display-none'}):
            label.append(k.text)
            
        for k in soup.find_all('dd', {'class':'value ui-helper-clearfix '}):
            for i in k.find_all('p', {'class':'label'}):
                values.append(i.text)

        for k in soup.find_all('dd', {'class':'value ui-helper-clearfix active-data display-none'}):
            for i in k.find_all('p', {'class':'label'}):
                values.append(i.text)
        
        label = [item.strip() for item in label if str(item)] 
        values = [item.strip() for item in values if str(item)]
       
        for x in label:
            if x == 'Illinois':
                ill.append(values[label.index(x)])
    
        
        for x in label:
            if x == 'New Jersey':
                nj.append(values[label.index(x)])
   
        
        for x in label:
            if x == 'Puerto Rico':
                pr.append(values[label.index(x)])

        for x in label:
            if x == 'Tobacco':
                tob.append(values[label.index(x)])
            
        for x in label:
            if x == 'Health':
                health.append(values[label.index(x)])
            
        for x in label:
            if x == 'Prerefund/Escrow':
                prere.append(values[label.index(x)])
    
        
        for x in label:
            if x == 'State Tax-Backed':
                st_go.append(values[label.index(x)])
        
        for x in label:
            if x == 'Local Tax-Backed':
                loc_go.append(values[label.index(x)])
        
        for x in label:
            if x == 'Housing':
                house.append(values[label.index(x)])
                
        for x in label:
            if x == '0 - 3 Years':
                maturity03.append(values[label.index(x)] + ' (3 yrs)')

        if 'Illinois' not in label:
            ill.append('0.0%')
        if 'New Jersey' not in label:
            nj.append('0.0%')        
        if 'Puerto Rico' not in label:
            pr.append('0.0%')
        if 'Tobacco' not in label:
            tob.append('0.0%')
        if 'Health' not in label:
            health.append('0.0%')
        if 'Prerefund/Escrow' not in label:
            prere.append('0.0%')
        if 'Housing' not in label:
            house.append('0.0%')
        if 'State Tax-Backed' not in label:
            st_go.append('0.0%')
        if 'Local Tax-Backed' not in label:
            loc_go.append('0.0%')
        if '0 - 3 Years' not in label:
            maturity03.append('0.0% (3 yrs)')
    
        q.task_done() # specify that you are done with the item

for i in range(10): # aka number of threadtex
    t1 = Thread(target = grab_data_from_queue) # target is the above function
    t1.start() # start the thread

q.join()

fund_name = [item.strip() for item in fund_name if str(item)]
ave_vol = [item.strip() for item in ave_vol if str(item)]
duration = [item.strip() for item in duration if str(item)]
duration2 = [k.split()[0] for k in duration]

go = []
for x in range(len(tickers)):
    go.append(str((float(st_go[x][:-1]) + float(loc_go[x][:-1]))) + '%')

############# Finding Current Dividend, Earnings per Share, and UNII per Share #############

tickers_pdf = [x[4:7] for x in split_data if x[0:4] == 'Fund' and x[4:5] == 'M'] + [y[2:5] for y in split_data if y[0:2] == 'II' and y[0:3] != "III"] + [z[3:6] for z in split_data if z[0:3] == 'III'] + [i[5:8] for i in split_data if i[0:5] == 'Trust' and len(i) > 5]
div = [x[7:15] for x in split_data if x[0:4] == 'Fund' and x[4:5] == 'M'] + [y[5:13] for y in split_data if y[0:2] == 'II' and y[0:3] != "III"] + [z[6:14] for z in split_data if z[0:3] == 'III'] + [i[8:16] for i in split_data if i[0:5] == 'Trust' and len(i) > 5]
eps = [x[15:23] for x in split_data if x[0:4] == 'Fund' and x[4:5] == 'M'] + [y[13:21] for y in split_data if y[0:2] == 'II' and y[0:3] != "III"] + [z[14:22] for z in split_data if z[0:3] == 'III'] + [i[16:24] for i in split_data if i[0:5] == 'Trust' and len(i) > 5]
unii = [x[23:31] for x in split_data if x[0:4] == 'Fund' and x[4:5] == 'M'] + [y[21:29] for y in split_data if y[0:2] == 'II' and y[0:3] != "III"] + [z[22:30] for z in split_data if z[0:3] == 'III'] + [i[24:32] for i in split_data if i[0:5] == 'Trust' and len(i) > 5]
  

     
################### Organizing PDF data to match json data ###################

div2 = [div[tickers_pdf.index(y)] for x in tickers for y in tickers_pdf if x == y]
eps2 = [eps[tickers_pdf.index(y)] for x in tickers for y in tickers_pdf if x == y]
unii2 = [unii[tickers_pdf.index(y)] for x in tickers for y in tickers_pdf if x == y]

ave_vol2 = [ave_vol[fund_name.index(y)] for x in tickers for y in fund_name if x == y]
duration3 = [duration2[fund_name.index(y)] for x in tickers for y in fund_name if x == y]
ill2 = [ill[fund_name.index(y)] for x in tickers for y in fund_name if x == y]
nj2 = [nj[fund_name.index(y)] for x in tickers for y in fund_name if x == y]
pr2 = [pr[fund_name.index(y)] for x in tickers for y in fund_name if x == y]
tob2 = [tob[fund_name.index(y)] for x in tickers for y in fund_name if x == y]
health2 = [health[fund_name.index(y)] for x in tickers for y in fund_name if x == y]
prere2 = [prere[fund_name.index(y)] for x in tickers for y in fund_name if x == y]
house2 = [house[fund_name.index(y)] for x in tickers for y in fund_name if x == y]
go2 = [go[fund_name.index(y)] for x in tickers for y in fund_name if x == y]
maturity3 = [maturity03[fund_name.index(y)] for x in tickers for y in fund_name if x == y]

stopTime2 = timeit.default_timer()
brTime = 'BlackRock: ' + str(stopTime2 - startTime)
print('BlackRock Completed')
print('Eaton Vance Started')
##############################################################################
############################## Eaton Vance ###################################
##############################################################################

url = 'http://funds.eatonvance.com/closed-end-fund-documents.php'
url_req = requests.get(url)
soup = BeautifulSoup(url_req.content)

all_urls = [z.get('href') for x in soup.find_all('tr') for y in x.find_all('td') for z in y.find_all('a') if 'Municipal' in str(x)]
ticker_urls = ['http://funds.eatonvance.com' + x for x in all_urls if x[-3:] == 'php']

del ticker_urls[3]

ev_ticker = []
ev_nav = []
ev_price = []
ev_pd = []
ev_duration = []
ev_go = []
ev_hc = []
ev_ec = []
ev_ho = []
ev_callmat = []

for j in ticker_urls:

    url_req = requests.get(j)
    ev_soup = BeautifulSoup(url_req.content)

    ev_web_data = [x.text for x in ev_soup.find_all('span', {'class':'stock'})]

    for x in ev_soup.find_all('div', {'class':'fund_title'}):
        for y in x.find_all('div', {'class':'item'}):    
            z = y.text.split(' ')
            ev_ticker.append(z[2])
            break

    ev_nav.append(ev_web_data[0][1:]) 
    ev_price.append(ev_web_data[1][1:])
    ev_pd.append(ev_web_data[2][:-1])
    
    fs_url = []

    for x in ev_soup.find_all('ul', {'class':'fund_docs'}):
        for y in x.find_all('a'):
            if y.text == 'Fact Sheet':
                java = (y.get('href'))
                java2 = java.split("'")
                fs_url.append('http://funds.eatonvance.com/includes/loadDocument.php?fn=' + java2[1])

    file = requests.get(fs_url[0])
    filename= "EatonFactSheet.pdf"
    output = open(filename, "wb")
    output.write(file.content)
    output.close()

    pdfFileObj = open("EatonFactSheet.pdf", 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
    pageObj = pdfReader.getPage(0)
    pageObj2 = pdfReader.getPage(1)
    data = pageObj.extractText() + pageObj2.extractText()
    pdfFileObj.close()
    data3 = data.split(" ")
    data4 = data.split('Sectors')
    sector = data4[1]
    sector2 = sector.replace(' ', '.')
    sector3 = sector2.split('.')
    #print(sector3)
    call_data = ''
    for x in data3:
        if x[0:8] == 'Duration' and data3[data3.index(x) - 1] == 'Effective':
            ev_duration.append(x[8:])
        if x == 'Schedule':
            call_data = data3[data3.index(x) + 5]
        
    call_data = call_data.split('201')
    ev_callmat.append(str(float(call_data[1][1:]) + float(call_data[2][1:])) + '%' + ' (2 yrs)')
    
    GO = 0.00
    HC = 0.00
    EC = 0.00
    HO = 0.00
    for x in sector3:
        if x[0:2] == 'GO':
            GO = GO + float(x[2:] + '.' + sector3[sector3.index(x) + 1][0:2])
        
        if x[0:10] == 'HealthCare':
            hc = x[-2:] + '.' + sector3[sector3.index(x) + 1][0:2]
            hclet = hc[0]
            if hclet.isalpha() == True:
                hc = hc[1:]
            HC = HC + float(hc)
        if x[2:12] == 'HealthCare':
            hc1 = x[-2:] + '.' + sector3[sector3.index(x) + 1][0:2]
            hclet1 = hc1[0]
            if hclet1.isalpha() == True:
                hc1 = hc1[1:]
            HC = HC + float(hc1)
      
        if x[0:8] == 'Escrowed':
            ec = x[-2:] + '.' + sector3[sector3.index(x) + 1][0:2]
            eclet = ec[0]
            if eclet.isalpha() == True:
                ec = ec[1:]
            EC = EC + float(ec)
        if x[2:10] == 'Escrowed':
            ec1 = x[-2:] + '.' + sector3[sector3.index(x) + 1][0:2]
            eclet1 = ec1[0]
            if eclet1.isalpha() == True:
                ec1 = ec1[1:]
            EC = EC + float(ec1)       

        if x[0:7] == 'Housing':
            HO = HO + float(x[7:] + '.' + sector3[sector3.index(x) + 1][0:2])

    if GO == 0:
        ev_go.append('Not Top 10')
    else:
        ev_go.append(str(GO) + '%')
    if HC == 0:
        ev_hc.append('Not Top 10')
    else:
        ev_hc.append(str(HC) + '%')
    if EC == 0:
        ev_ec.append('Not Top 10')
    else:
        ev_ec.append(str(EC) + '%')
    if HO == 0:
        ev_ho.append('Not Top 10')
    else:
        ev_ho.append(str(HO) + '%')  

#############################################################################

url = 'http://funds.eatonvance.com/Municipal-Bond-Fund-EIM.php'

response = requests.get(url).content
textlinks=[]
the_links=[]
text='Eaton Vance Closed-End Municipal Bond Funds Fund Data Now Available'
soup = BeautifulSoup(response)
u=soup.find_all('a', {'class':'SiteCatalystRR'})
for i in u:
    for k in i:
        textlinks=[k,i.get('href')]
        if textlinks[0]==text:
            the_links.append(textlinks[1])
            
link=the_links[0]
split_link=link.split("'")
url_pdf='http://funds.eatonvance.com/includes/loadDocument.php?fn='+(split_link[1])

file = requests.get(url_pdf)
filename= "EatonUNII.pdf"
output = open(filename, "wb")
output.write(file.content)
output.close()

#Parsing PDF file:
pdfFileObj = open("EatonUNII.pdf", 'rb')
pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
pageObj = pdfReader.getPage(0)
data = pageObj.extractText()
pdfFileObj.close()
data2 = data.split(' ')
#print(data2)

ev_eps = []
ev_div = []
ev_unii = []

for x in ev_ticker:
    for y in data2:
        if x == y:
            ev_eps.append(data2[data2.index(y) + 1][1:])
            ev_div.append(data2[data2.index(y) + 3][1:])
            ev_unii.append(data2[data2.index(y) + 5][1:])

stopTime3 = timeit.default_timer()
evTime = 'Eaton Vance: ' + str(stopTime3 - stopTime2)
print('Eaton Vance Completed')
print('Pimco Started')

##############################################################################
################################# Pimco ######################################
##############################################################################

#*******************************************************************************
# HTML Parser - Find basic data
#*******************************************************************************
#little function that cleans data
def cleanText(value):
    return value.replace("$", "").replace("%", "").replace(",", "").replace("\r", "").replace("\n", "").replace(" ", "");

pimcohtmlurl= 'https://investments.pimco.com/Products/Pages/PlCEF.aspx'
# beautifuksoup looking for tr and td tags
soup = BeautifulSoup(urllib.request.urlopen(pimcohtmlurl).read())
soup('table')[0].prettify()
pimcohtml = []
for tr in soup('table')[0].findAll('tr'):
    tds = tr.findAll('td')
    if len(tds) > 1:
        # get fundname, marketprice, NAV, premiumdiscount, add more fields if necessary
        pimcohtml.append([tds[0].text, cleanText(tds[1].text), cleanText(tds[3].text), cleanText(tds[5].text)])

# build a dictionary for individual fund web site
pimcofundhtmlurl= 'https://investments.pimco.com/Products/pages/000.aspx'
fundtowebsite = {'PCQ':'666', 'PCK':'670', 'PZC':'671', 'PMF':'664', 'PML':'669', 'PMX':'672', 'PNF':'665', 'PNI':'668', 'PYN':'673'}

def ExtractFundInformation(htmlurl):
    soup = BeautifulSoup(urllib.request.urlopen(htmlurl).read())
    soup('table')[0].prettify()
    fundvalues = {}
    for tr in soup('table')[0].findAll('tr'):
        tds = tr.findAll('td')
        if len(tds) > 1:
            fundvalues[cleanText(tds[0].text)] = cleanText(tds[1].text)
    return [fundvalues['AverageDailyVolume'], fundvalues['SharesOutstanding']]

postData = {
    'ctl00$ScriptManager1': 'ctl00$PHM$TabControl2$ctl00$PHM$TabControl2$pnlRADPanel|ctl00$PHM$TabControl2$RadTabStrip1',
    '__SPSCEditMenu': 'TRUE',
    'MSOWebPartPage_PostbackSource	': '',
    'MSOTlPn_SelectedWpId': '',
    'MSOTlPn_View	': '0',
    'MSOTlPn_ShowSettings': 'FALSE',
    'MSOGallery_SelectedLibrary': '',
    'MSOGallery_FilterString': '',
    'MSOTlPn_Button': 'none',
    '__REQUESTDIGEST': '0x15B0DDC0B47563A64565C21169A2622C36F8D28FF65BE62E9045BF8F39B2A4D586A6DEA01335AB963E072C3A8A70E80259B9957649DD3F439DA71375667BEF3F,24 Jun 2015 20:14:18 -0000',
    'MSOAuthoringConsole_FormContext': '',
    'MSOAC_EditDuringWorkflow': '',
    'MSOSPWebPartManager_DisplayModeName': 'Browse',
    'MSOWebPartPage_Shared': '',
    'MSOLayout_LayoutChanges': '',
    'MSOLayout_InDesignMode': '',
    'MSOSPWebPartManager_OldDisplayModeName': 'Browse',
    'MSOSPWebPartManager_StartWebPartEditingName': 'FALSE',
    'ctl00$ShoppingHeaderBaseControl1$ctl00$facetHiddenControl': '',
    'ctl00$basicSearch$txtSearch': 'Search Site',
    'ctl00$basicSearch$hdnAdvancedSearchUrl': '/Pages/advanceSearch.aspx',
    'ctl00$basicSearch$hdnBasicSearchUrl': '/SearchCenter/Pages/SearchResult.aspx',
    'ctl00$basicSearch$hdnDefaulValue': 'Search Site',
    'ctl00$basicSearch$hdnDefaultScope': 'Entire Site',
    'ctl00$basicSearch$hdnUserType	Investor': '',
    'ctl00_PHM_TabControl2_RadTabStrip1_ClientState': '{"selectedIndexes":["2"],"logEntries":[],"scrollState":{}}',
    'ctl00_PHM_TabControl2_RadMultiPage1_ClientState': '',
    'ctl00$PHM$TabControl2$hdnfselectedTabName': 'PORTFOLIO',
    'ctl00$PHM$TabControl2$hdnShowSideBar': '1',
    'ctl00$PHM$TabControl2$tabSelect': '',
    'ctl00$PHM$TabControl2$hdnSelected': '',
    'ctl00$PHM$TabControl2$pageView': 'FALSE',
    'ctl00$PHM$TabControl2$Load': 'FALSE',
    'ctl00$PHM$TabControl2$TabForMeta': 'PORTFOLIO',
    'ctl00$PHM$TabControl2$currentSelectedTab	Leverage': '',
    'ctl00$PHM$ProductFinderProduct$radComboSymbolSearch': 'Enter name, symbol or CUSIP',
    'ctl00_PHM_ProductFinderProduct_radComboSymbolSearch_ClientState': '{"logEntries":[],"value":"","text":"","enabled":true}',
    'ctl00$PHM$ProductFinderProduct$productFindSharedClass': '',
    'ctl00$txtChildNode': 'http://pimco.com/investments/ml;https://ml.investments.pimco.com;http://banners.pimco.com;https://investments.pimco.com;http://solutionscentral/;http://pe.newriver.com;https://www.icsdelivery.com;https://www3.financialtrans.com;http://pe.newriver.com;https://order.pimco.com/api/PI/SSO/Authenticate.aspx;http://banners.pimco.com;http://perfcal.pimco-funds.com;http://pimcoetfs.com;http://pvit.pimco-funds.com;http://vit.pimco-funds.com;http://www.pimco.com/investments;http://pimco.com/investments;http://www.pimcoetfs.com/;http://www.addthis.com;http://info.pimco.com/income;www2.pimco.com;www.pimco.com;http://media.pimco.com/Media/;http://media.pimco.com/media/video/;http://www.workedit.com;http://www.workedit.com/;http://us-pimcopal/Pages/default.aspx;http://secular.pimco.com;http://ideas.investments.pimco.com/;http://facts.investments.pimco.com/',
    '__EVENTTARGET': 'ctl00$PHM$TabControl2$RadTabStrip1',
    '__EVENTARGUMENT': '2',
    '__VIEWSTATE': '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MC4xLjMwOS4yMCwgQ3VsdHVyZT1uZXV0cmFsLCBQdWJsaWNLZXlUb2tlbj0xMjFmYWU3ODE2NWJhM2Q0ZBYKZg8PFgwfCAUIU05BUFNIT1QfDQUCMzMfDhsAAAAAAABZQAEAAAAfDwUHQ0VGVGFicx8QBQIzMx8RAoICZGQCAQ8PFgwfCAULUGVyZm9ybWFuY2UfDQUCMzQfDhsAAAAAAEBVQAEAAAAfDwUHQ0VGVGFicx8RAoICHxAFAjM0ZGQCAg8PFgwfCAUJUG9ydGZvbGlvHw0FAjM1Hw4bAAAAAACATEABAAAAHw8FB0NFRlRhYnMfEQKCAh8QBQIzNWRkAgMPDxYMHwgFCExldmVyYWdlHw0FAzE1MB8OGwAAAAAAAElAAQAAAB8PBQdDRUZUYWJzHxECggIfEAUDMTUwZGQCBA8PFgofCAUJRG9jdW1lbnRzHw0FAjM3Hw4bAAAAAABAVEABAAAAHw8FB0NFRlRhYnMfEQKCAmRkAgMPFCsAAg8WBB8MZh8KaGQVAQMxNTBkAhkPZBYCZg8PFgIfAWhkZAIbD2QWAmYPZBYCAgEPZBYMAgUPZBYCZg9kFgYCAQ9kFgJmDw8WAh8BaGRkAgMPDxYGHgtEZXNjcmlwdGlvbgUXRmluZCBhIFByb2R1Y3Qgd2VicGFydC4eBVRpdGxlBQtGaW5kIGEgRnVuZB8JAgJkFgICAQ9kFgJmD2QWBGYPFCsAAg8WBB8KaB8IZRYGHgdvbmtleXVwBYIBamF2YXNjcmlwdDpFbmFibGVHb0J1dHRvbignY3RsMDBfUEhNX1Byb2R1Y3RGaW5kZXJQcm9kdWN0X3JhZENvbWJvU3ltYm9sU2VhcmNoJywnY3RsMDBfUEhNX1Byb2R1Y3RGaW5kZXJQcm9kdWN0X2J0blN5bWJvbFNlYXJjaCcpOx4Kb25tb3VzZW91dAWCAWphdmFzY3JpcHQ6RW5hYmxlR29CdXR0b24oJ2N0bDAwX1BITV9Qcm9kdWN0RmluZGVyUHJvZHVjdF9yYWRDb21ib1N5bWJvbFNlYXJjaCcsJ2N0bDAwX1BITV9Qcm9kdWN0RmluZGVyUHJvZHVjdF9idG5TeW1ib2xTZWFyY2gnKTseB29ucGFzdGUFgAFqYXZhc2NyaXB0OkVuYWJsZUJ1dHRvbignY3RsMDBfUEhNX1Byb2R1Y3RGaW5kZXJQcm9kdWN0X3JhZENvbWJvU3ltYm9sU2VhcmNoJywnY3RsMDBfUEhNX1Byb2R1Y3RGaW5kZXJQcm9kdWN0X2J0blN5bWJvbFNlYXJjaCcpO2QWBGYPDxYEHw8FCXJjYkhlYWRlch8RAgJkZAIBDw8WBB8PBQlyY2JGb290ZXIfEQICZGQCAQ8PFgIfAmgWAh8GBTRqYXZhc2NyaXB0OlBGV2VidHJlbmRzQ2FsbCgnMCcsJ1Byb2R1Y3Qgc2VhcmNoIGJveCcpZAIFD2QWAmYPDxYCHwFoZGQCCQ8PFgIfAWdkFgICAQ8WAh8ACysEAWQCCw9kFgQCAQ8WAh8ACysEAWQCAw8WAh8ACysEAWQCDQ9kFgQCAQ8WAh8ACysEAWQCAw8WAh8ACysEAWQCDw9kFgJmDw8WAh8BaGRkAhEPZBYCZg8PFgIfAWhkZAIdD2QWAmYPDxYCHwFoZGQCHw9kFgJmDw8WAh8BaGRkAiEPZBYCZg8PFgIfAWhkZAIlD2QWAmYPZBYCAgMPDxYCHwkCAmRkGAIFHl9fQ29udHJvbHNSZXF1aXJlUG9zdEJhY2tLZXlfXxYHBSVjdGwwMCRiYXNpY1NlYXJjaCRCYXNpY1NlYXJjaFNjb3BlcyQwBSVjdGwwMCRiYXNpY1NlYXJjaCRCYXNpY1NlYXJjaFNjb3BlcyQxBSVjdGwwMCRiYXNpY1NlYXJjaCRCYXNpY1NlYXJjaFNjb3BlcyQxBSJjdGwwMCRQSE0kVGFiQ29udHJvbDIkUmFkVGFiU3RyaXAxBSNjdGwwMCRQSE0kVGFiQ29udHJvbDIkUmFkTXVsdGlQYWdlMQUzY3RsMDAkUEhNJFByb2R1Y3RGaW5kZXJQcm9kdWN0JHJhZENvbWJvU3ltYm9sU2VhcmNoBS5jdGwwMCRQSE0kUHJvZHVjdEZpbmRlclByb2R1Y3QkYnRuU3ltYm9sU2VhcmNoBTNjdGwwMCRQSE0kUHJvZHVjdEZpbmRlclByb2R1Y3QkcmFkQ29tYm9TeW1ib2xTZWFyY2gPFCsAAmVlZC8T6D4lGUpVtIj2GYsuIX4ocssA',
    '__LASTFOCUS': '',
    '__SCROLLPOSITIONX': '0',
    '__SCROLLPOSITIONY': '0',
    '__ASYNCPOST': 'TRUE',
    'RadAJAXControlID': 'ctl00_PHM_TabControl2_pnlRAD',
    'RadAJAXControlID': 'ctl00_PHM_TabControl2_pnlRAD'
    }

def ExtractFundOtherInfo(htmlurl):
    soup = BeautifulSoup(requests.post(htmlurl, postData, None, verify=False).content)
    soup('table')[0].prettify()
    fundvalues = {}
    for tr in soup('table')[0].findAll('tr'):
        tds = tr.findAll('td')
        if len(tds) > 1:
            fundvalues[cleanText(tds[0].text)] = cleanText(tds[1].text)    
    soup('table')[1].prettify()
    fundDurations = 0.0
    for tr in soup('table')[1].findAll('tr'):
        tds = tr.findAll('td')
        if len(tds) > 1:
            if cleanText(tds[0].text) == 'TotalLeveraged-AdjustedEffectiveDurations(yrs.)':
                fundDurations = cleanText(tds[1].text)
                break
    maturity1Year = 0.0
    for tr in soup('table')[3].findAll('tr'):
        tds = tr.findAll('td')
        if len(tds) > 1:
            if cleanText(tds[0].text) == '0-1yrs':
                maturity1Year = cleanText(tds[1].text)
                break
    call5Year = 0.0
    for tr in soup('table')[2].findAll('tr'):
        tds = tr.findAll('td')
        if len(tds) > 1:
            if cleanText(tds[0].text) == '0-5Years':
                call5Year = cleanText(tds[1].text)
                break
    return [fundvalues['Tobacco'], fundvalues['State/LocalGO'], fundvalues['Pre-Refunded'], fundvalues['HospitalRev.'], fundDurations, maturity1Year, call5Year]

pimcoPDFile = 'PIMCOUNIIFile.pdf' 
# download pdf file
urllib.request.urlretrieve("https://investments.pimco.com/ShareholderCommunications/External%20Documents/UNII%20Website%20File.pdf", pimcoPDFile)

# read the pdf file
#Parsing PDF file:
pdfFileObj = open(pimcoPDFile, 'rb')
pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
pageObj = pdfReader.getPage(0)
data = pageObj.extractText()
pdfFileObj.close()

# strptime() Behavior https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior

# Find date For Dividends
dateForDividendsPattern = r"As of ([\w, ]+) for Dividends Declared on"
dateForDividendsString = re.findall(dateForDividendsPattern, data, re.M)
dateForDividends = datetime.datetime.strptime(dateForDividendsString[0], '%B %d, %Y').date()

# Get all Previous Fiscal Year End
datePreviousEndPattern = r'(\d{2}-\w{3}-\d{2})\d{2}-\w{3}-\d{2}'
datePreviousEndStringList = re.findall(datePreviousEndPattern, data, re.M)
datePreviousEndList = [datetime.datetime.strptime(dateString, '%d-%b-%y').date() for dateString in datePreviousEndStringList]

# Hard-coded list position for each fund.  
fundNames = {'PCQ':0, 'PCK':1, 'PZC':2, 'PMF':3, 'PML':4, 'PMX':5, 'PNF':6, 'PNI':7, 'PYN':8}

def diff_month(d1, d2):
    return (d1.year - d2.year)*12 + d1.month - d2.month

# Find values for each fund
rePattern = r'(AAA)\$([\d.]+)\$([\d.]+) \$([\d.]{8})'
fundvalues = []
for fundName in fundNames.keys():
    fundPattern = rePattern.replace('AAA', fundName)
    searchObj = re.findall(fundPattern, data, re.M)
    months = diff_month(dateForDividends, datePreviousEndList[fundNames[fundName]])
    monthsModified = 12 if months == 0 else months
    fundvalues.append([searchObj[0][0], float(searchObj[0][1])/monthsModified, float(searchObj[0][2]), float(searchObj[0][3])])
     
#Includes fundname, Net InvestmentIncome (NII), Undistributed Net Investment Income (UNII), Monthly Distribution per Share


mergedResult = []
for fund in pimcohtml:
    fundotherInfo = ExtractFundOtherInfo(pimcofundhtmlurl.replace('000', fundtowebsite[fund[0]]))
    fundInformation = ExtractFundInformation(pimcofundhtmlurl.replace('000', fundtowebsite[fund[0]]))
    mergedResult.append(fund + fundInformation + fundotherInfo)

for x in mergedResult:
    for y in fundvalues:
        if x[0] == y[0]:
            x.append(y[1])
            x.append(y[2])
            x.append(y[3])
            
mergedResult2 = []
for x in mergedResult:
    sort = []
    sort.append(x[0])
    sort.append(x[15])
    sort.append(x[13])
    sort.append(x[14])
    sort.append(x[10])
    sort.append('-')
    sort.append('-')
    sort.append('-')
    sort.append(str(x[6]) + '%')
    sort.append(str(x[8]) + '%')
    sort.append(str(x[7]) + '%')
    sort.append(str(x[9]) + '%')
    sort.append('-')
    sort.append(str(x[12]) + '% (5 yrs)')
    sort.append(str(x[11]) + '% (1 yr)')
    sort.append(x[2])
    sort.append(x[1])
    sort.append(x[3])
    sort.append(x[4])
    mergedResult2.append(sort)
# mergedResult item includes fundname, marketprice, NAV, premiumdiscount, AverageDailyVolume, SharesOutstanding, Tobacco, State/Local GO, Pre-Refunded, Hospital Rev, Durations, 0-1 yr maturity, 5 Year call

stopTime4 = timeit.default_timer()
pcTime = 'Pimco: ' + str(stopTime4 - stopTime3)
print('Pimco Completed')
print('Investco Started')
##################################################################################
#################################### Investco ####################################
##################################################################################

############################# HTML Parser ######################################

url = 'https://www.invesco.com/portal/site/us/template.PAGE/investors/closed-end/performance/?javax.portlet.tpst=12bb63b527f92eea60d73a31524e2ca0&javax.portlet.prp_12bb63b527f92eea60d73a31524e2ca0_FilterList=DOCUMENT%252FFCLASS_ASSET_TYPE%2526FIXED%2BINCOME&javax.portlet.prp_12bb63b527f92eea60d73a31524e2ca0_FilterGGA=IassetType%20I0%20I3%20I7%20Ib%20&javax.portlet.begCacheTok=com.vignette.cachetoken&javax.portlet.endCacheTok=com.vignette.cachetoken'
response = requests.get(url).content # I request url's content
soup = BeautifulSoup(response) #convert into Python accessible type
data = soup('table')[0]

# getting relevent data

relevant = [name.text for i in data.find_all('tr') for name in i.find_all('td')]
funds_data = [[relevant[i],relevant[i+2],relevant[i+3],relevant[i+5]] for i in range(len(relevant)) if len(relevant[i]) ==3 and relevant[i].isalpha() is True ] #order is name of the fund, nav, closing price, premium disciount
names = [funds_data[i][0] for i in range(len(funds_data))]

############################# Income ###########################################

urls_income = [(hf['href'].replace('\n','')).replace(' ','') for i in data.find_all('td') for hf in i.find_all('a', href = True) ]

numbers= [[names[i],re.findall(re.compile('productId=(.+?)&productType'),urls_income[i])] for i in range(len(urls_income))]
dividends = []
for fund_n in numbers:
    url = "https://www.invesco.com/portal/site/us/template.RAW/investors/closed-end/product-detail/?javax.portlet.tpst=a1a3b921fe45ac55c876dfc67d1ffba0_ws_RW&javax.portlet.prp_a1a3b921fe45ac55c876dfc67d1ffba0_fund="
    url = url + fund_n[1][0]
    url = url + "&javax.portlet.begCacheTok=com.vignette.cachetoken&javax.portlet.endCacheTok=com.vignette.cachetoken"
    
    r = requests.get(url, verify=False).text
    soup = BeautifulSoup(r)
    
    data = soup('table')[0]
    
    income=(data.find_all('td')[1].text).replace('\t','').replace('\n','')
    dividends.append([fund_n[0],income])


############################# PDFs Parser ######################################

url_pdf = 'https://www.invesco.com/portal/site/us/template.PAGE/investors/closed-end/performance/?javax.portlet.tpst=12bb63b527f92eea60d73a31524e2ca0&javax.portlet.prp_12bb63b527f92eea60d73a31524e2ca0_action=fundMaterials&javax.portlet.prp_12bb63b527f92eea60d73a31524e2ca0_FilterList=DOCUMENT%252FFCLASS_ASSET_TYPE%2526FIXED%2BINCOME&javax.portlet.prp_12bb63b527f92eea60d73a31524e2ca0_FilterGGA=IassetType%20I0%20I3%20I7%20Ib%20&javax.portlet.begCacheTok=com.vignette.cachetoken&javax.portlet.endCacheTok=com.vignette.cachetoken'
response_pdf = requests.get(url_pdf).content # I request url's content
soup_pdf = BeautifulSoup(response_pdf) #convert into Python accessible type
data_pdf = soup_pdf('table')[0]

names_funds = [name.text for i in data_pdf.find_all('tr') for name in i.find_all('td',{'class':'text-left'})][::2]
pdf_urls = ['https://www.invesco.com'+hf['href'] for i in data_pdf.find_all('td',{'class':'text-center'}) for hf in i.find_all('a',{'class':'pdf'}, href = True) ][::2]
pdf_data = []

for i in pdf_urls:
        temp= []
        file_name = 'PDF.pdf'
        pdf = requests.get(i).content
        output = open(file_name, 'wb')
        output.write(pdf)
        output.close()
        
        #Parsing PDF
        pdf_file = open(file_name,'rb')
        PDF = PyPDF2.PdfFileReader(pdf_file)

        if PDF.isEncrypted:
                PDF.decrypt("")
        
        page_obj = PDF.getPage(1)
        data = page_obj.extractText()
        
        name_re = re.compile('Ticker: .+Inception')
        ticker = re.findall(name_re,data)[0][8:11]
        temp.append(ticker)
        
        ############## Fund Operation/Management ################3             
        
        
        ear_re = re.compile('Earnings*(.+?)U')
        earnings = re.findall(ear_re,data)[0].strip('Earnings*')[0:5]
        temp.append(earnings)
        
        unii_re = re.compile('UNII Balance*(.+?)P')
        unii = re.findall(unii_re,data)[0].strip('UNII Balance*')[0:5]
        temp.append(unii)      
        
        
        ############## Cash Flows ################3     
        
        lev_re=re.compile('Leverage Adjusted OAD(.+?)A')
        lev_dur = re.findall(lev_re,data)
        if lev_dur == []:
                temp.append(0)
        else:
                temp.append(lev_dur[0].strip('Leverage Adjusted OAD')[0:5])        
        
        
        ############## State Exposure ################3             
        
        il_re = re.compile('Illinois\d+.\d+')        
        if re.findall(il_re,data) ==[]:
                temp.append("Not Top 5")
                        
        else:
                il=re.findall(il_re,data)[0].strip('Illinios')
                temp.append(il + '%')        
        
        pr_re = re.compile('Puerto Rico\d+.\d+')        
        if re.findall(pr_re,data) ==[]:
                temp.append("Not Top 5")
                
        else:
                pr=re.findall(pr_re,data)[0].strip('Puerto Rico')
                temp.append(pr + '%')           
        
        nj_re = re.compile('New Jersey\d+.\d+')        
        if re.findall(nj_re,data) ==[]:
            temp.append("Not Top 5")
            
        else:
            nj=re.findall(nj_re,data)[0].strip('New Jersey')
            temp.append(nj + '%')                          
        
    ##############Tobacco Exposure ################3             
        
        tob_re = re.compile('Tobacco\d+.\d+')
        if re.findall(tob_re,data) ==[]:
            temp.append("0.00%")                                
        else:
            tobacco=re.findall(tob_re,data)[0].strip('Tobacco')
            temp.append(tobacco + '%')
      
    ############## Prerefunded Exposure ################3
        prere_re = re.compile('Prerefunded/ETM\d+.\d+')
        if re.findall(prere_re,data) ==[]:
            temp.append(0)                                
        else:
            prere=re.findall(prere_re,data)[0].strip('Prerefunded/ETM')
            temp.append(prere + '%')              
      
        
        ############## General Obligations Exposure ################3     
        
        gen_ob = []
        
        local_re = re.compile('Local GO\d+.\d+')
        if re.findall(local_re,data) ==[]:
            gen_ob.append(0)                                
        else:
            local=re.findall(local_re,data)[0].strip('Local GO')
            gen_ob.append(float(local))
                
        state_re = re.compile('State GO\d+.\d+')
        if re.findall(state_re,data) ==[]:
            gen_ob.append(0)                                
        else:
            state=re.findall(state_re,data)[0].strip('State GO')
            gen_ob.append(float(state))        
            
        gen_obl = sum(gen_ob)
        temp.append(str(gen_obl) + '%')        
        
        ############## Health Sector Exposure ################3        
                
        health = []
        
        hos_re = re.compile('Hospital\d+.\d+')
        if re.findall(hos_re,data) ==[]:
            health.append(0)                                
        else:
            hos=re.findall(hos_re,data)[0].strip('Hospital')
            health.append(float(hos))
                
        life_re = re.compile('Life Care\d+.\d+')
        if re.findall(life_re,data) ==[]:
            health.append(0)                                
        else:
            life=re.findall(life_re,data)[0].strip('Life Care')
            health.append(float(life))        
                        
        nurh_re = re.compile('Nursing Home\d+.\d+')
        if re.findall(nurh_re,data) ==[]:
            health.append(0)                                
        else:
            nurse=re.findall(nurh_re,data)[0].strip('Nursing Home')
            health.append(float(nurse))        
        
        health = sum(health)
        temp.append(str(health) + '%')
        
        ############## Housing Exposure ################3
        house = []
                
        sing_re = re.compile('Single-Family\d+.\d+')
        if re.findall(sing_re,data) ==[]:
            house.append(0)                                
        else:
            sing=re.findall(sing_re,data)[0].strip('Single-Family')
            house.append(float(sing))
            
        multi_re = re.compile('Muli-Family\d+.\d+')
        if re.findall(multi_re,data) ==[]:
            house.append(0)                                
        else:
            multi=re.findall(multi_re,data)[0].strip('Multi-Family')
            house.append(float(multi))        
                    
        house = sum(house)
        temp.append(str(house) + '%')          
        
        ############## <3 Call #######################
        closecall = []
        
        y2015_re = re.compile('Next Call Date.+2015(.+?)2016')
        if re.findall(y2015_re,data) == []:
            closecall.append(0)
        else:
            y2015 = re.findall(y2015_re,data)[0].replace('2015','').replace('2016','')
            closecall.append(float(y2015))
        
        y2016_re = re.compile('Next Call Date.+2016(.+?)2017')
        if re.findall(y2016_re,data) == []:
            closecall.append(0)
        else:
            y2016 = re.findall(y2016_re,data)[0].replace('2016','').replace('2017','')
            closecall.append(float(y2016))        
        y2017_re = re.compile('Next Call Date.+2017(.+?)2018')
        if re.findall(y2017_re,data) == []:
            closecall.append(0)
        else:
            y2017 = re.findall(y2017_re,data)[0].replace('2017','').replace('2018','')
            closecall.append(float(y2017))        
            
        closecall = sum(closecall)
        temp.append(str(float(int(closecall*100))/100) + '% (2 yrs)')
        temp.append('-')
        
        pdf_file.close()
        os.remove(file_name)
        pdf_data.append(temp) 

empty = [0]
html_and_pdf_data= [j + i[1:] for i in pdf_data for j in dividends if i[0] == j[0] and i[3]!= 0]
combined_data = [ i +j[1:] for i in html_and_pdf_data for j in funds_data if i[0] == j[0]]

url = 'http://www.cefconnect.com/Details/Summary.aspx?Ticker='

for fund in range(len(combined_data)):
    url_ind = url + combined_data[fund][0]
    name = combined_data[fund][0]
    req = requests.get(url_ind).content
    soup = BeautifulSoup(req)
    data = soup('table',{'id':'ContentPlaceHolder1_cph_main_cph_main_ucFundBasics_dvFB2'})[0]
    tds =data.findAll('td')
    for i in range(len(tds)):
        if tds[i].text == "Average Daily Volume (shares):":
            combined_data[fund].append(tds[i+1].text)

stopTime5 = timeit.default_timer()
icTime = 'Investco: ' + str(stopTime5 - stopTime4)
print('Investco Completed')
print('Nuveen Started')

##################################################################################
#################################### Nuveen ######################################
##################################################################################

#Excel Parser
url ='http://www.nuveen.com/Home/Documents/Viewer.aspx?fileId=65923'
file_name = 'Nuveen_Data_xl.xls'
file = requests.get(url) # I request to open the webpage
output = open(file_name,'wb') # I open the file where the data is going to be stored
output.write(file.content) # I write the content of the web file into the output
output.close()

#working with the file

wb = xlrd.open_workbook(file_name) #open a file 

sheet_name = 'Municipal Funds'

sheet = wb.sheet_by_name(sheet_name) # sheet that I will be working with

data_nuveen_xl = [ [sheet.cell_value(i,0),sheet.cell_value(i,3), str(sheet.cell_value(i,5))[:8], str(sheet.cell_value(i,6))[:8]] for i in range(sheet.nrows) if len(sheet.cell_value(i,0)) == 3]

tickers_order = [sheet.cell_value(i,0) for i in range(sheet.nrows) if len(sheet.cell_value(i,0))==3]
# data that I get is in the form: name of the fund, full name, current dividend, earnings per share, UNII


#*******************************************************************************  


#HTML Parser

url = 'http://www.nuveen.com/CEF/DailyPricingTaxExempt.aspx'

response = requests.get(url).content

soup = BeautifulSoup(response) #now we have a string of html values

data = soup('table')[0].find_all('tr')

nuveen_data_web = []

for tr in data:
     
     td = tr.find_all('td') # set of all the elements containing 'td' tags, can be throught of as a list
     
     if len(td) > 10: #because there are some td which length is 0...
          
          nuveen_data_web.append([td[0].text,td[12].text, td[3].text.strip('$') , td[5].text.strip('$'), td[7].text.strip('%'), td[11].text])
# nuveen_data is in the form: name of the fund, duration, NAV, closing price, premium discount, average daily volume

############################ Risk Online #######################################
risk_urls = ['http://www.nuveen.com/CEF/Product/Holdings.aspx?fundcode='+i for i in tickers_order]
risk_data = []
for i in range(len(risk_urls)):
       
       #### Maturity Exposure and  Call#######
       
          url = risk_urls[i]
          response = requests.get(url).content    
          soup = BeautifulSoup(response)
          div=soup.find_all('script')[9]
          replaced=div.text.replace("}",":")
          splitter=replaced.split(':')
          first=splitter[2][1:-1]
          second=splitter[5][1:-1]
          x=float(first)
          y=float(second)
          call_exposure=(x+y)
    
          diva=soup.find_all('script')[11]
          replaced=diva.text.replace("}",":")
          splittera=replaced.split(':')
          nuv_maturity=(splittera[14][1:-1])    
        
        ##############################



          name = tickers_order[i]
          url_risk1 = risk_urls[i]
          r = requests.get(url_risk1).content
          soup = BeautifulSoup(r)
          ################################ State Exposure ################################
          top_states = [st.text for i in soup.find_all('table',{'id':'cph_root_body_form_cph_body_cph_body_region_cph_body_region_TopAllocation6_TopGridView'}) for st in i.find_all('td',{'class':'first-col'})]
          nums = [float(num.text.strip('%')) for i in soup.find_all('table',{'id':'cph_root_body_form_cph_body_cph_body_region_cph_body_region_TopAllocation6_TopGridView'}) for num in i.find_all('td',{'class':'right-align'})]
          il=[]
          pr = []
          nj = []
          for i in range(len(top_states)):
               if top_states[i] == "Illinois":
                    il.append(nums[i])
               else:
                    il.append(0)
               if top_states[i] == "Puerto Rico":
                    pr.append(nums[i])
               else:
                    pr.append(0)
               if top_states[i] == "New Jersey":
                    nj.append(nums[i])
               else:
                    nj.append(0)
                    
          s_pr = sum(pr)
          s_nj = sum(nj)
          s_il = sum(il)
          
          ############################## Sector Exposure #################################
          top_sec = [st.text for i in soup.find_all('table',{'id':'cph_root_body_form_cph_body_cph_body_region_cph_body_region_TopAllocation3_TopGridView'}) for st in i.find_all('td',{'class':'first-col'})]
          sec_num = [float(num.text.strip('%')) for i in soup.find_all('table',{'id':'cph_root_body_form_cph_body_cph_body_region_cph_body_region_TopAllocation3_TopGridView'}) for num in i.find_all('td',{'class':'right-align'})]
                 
          s_health=[]
          s_go = []
          s_guarant = []
          s_housing = []
          s_tob = []
          for i in range(len(top_sec)):
               if top_sec[i] == "Health Care" or top_sec[i] == "Long Term Care":
                    s_health.append(sec_num[i])
               else:
                    s_health.append(0)
               if top_sec[i] == "Tax Obligation/General" or top_sec[i] == "Tax Obligation/Limited":
                    s_go.append(sec_num[i])
               else:
                    s_go.append(0)
               if top_sec[i] == "US Guaranteed":
                    s_guarant.append(sec_num[i])
               else:
                    s_guarant.append(0)
                                
               if top_sec[i] == "Housing/Single Family" or top_sec[i] == "Housing/Multifamily":
                    s_housing.append(sec_num[i])
               else:
                    s_housing.append(0)
               if top_sec[i] == "Consumer Staples":
                    s_tob.append(sec_num[i])
               else:
                    s_tob.append(0)               
               guarant = sum(s_guarant)
               health = sum(s_health)
               housing = sum(s_housing)
               go = sum(s_go)
               tob = sum(s_tob)
               if guarant == 0:
                   guarant = 'Not Top 5'
               if health == 0:
                   health = 'Not Top 5'
               if housing == 0:
                   housing = 'Not Top 5'
               if go == 0:
                   go = 'Not Top 5'
               if tob == 0:
                   tob = 'Not Top 5'
               if s_il == 0:
                   s_il = 'Not Top 5'
               if s_pr == 0:
                   s_pr = 'Not Top 5'
               if s_nj == 0:
                   s_nj = 'Not Top 5'
          r_data = []
          r_data2 = []
          r_data.append(name)
          r_data.append(str(s_il) + '%')
          r_data.append(str(s_pr) + '%')
          r_data.append(str(s_nj) + '%') 
          r_data.append(str(tob) + '%')
          r_data.append(str(guarant) + '%')
          r_data.append(str(go) + '%')
          r_data.append(str(health) + '%')
          r_data.append(str(housing) + '%')          
          r_data.append(str(float(int(call_exposure*100)/100)) + '% (2 yrs)')
          r_data.append(nuv_maturity + '% (4 yrs)')
          
          for x in r_data:
              if x[0:-1] == 'Not Top 5':
                  x = x[0:-1]
                  r_data2.append(x)
              else:
                  r_data2.append(x)

          risk_data.append(r_data2)          
combined_data2 = []
final2 = []
for i in data_nuveen_xl:
     for j in nuveen_data_web:
          if i[0] == j[0]:
               i.append((j[1]))

for m in data_nuveen_xl:
     for i in risk_data:
          if i[0] == m[0]:
               combined_data2.append(m +i[1::])

for i in combined_data2:
     for j in nuveen_data_web:
          if i[0] == j[0]:
               final2.append(i + j[2::])

stopTime6 = timeit.default_timer()
nuvTime = 'Nuveen: ' + str(stopTime6 - stopTime5)
print('Nuveen Completed')
print('Pioneer Started')
#################################################################################
################################### Pioneer #####################################
#################################################################################

########################## Getting PDFs #######################################

url_with_pdfs = 'http://us.pioneerinvestments.com/misc/prospectus_closed.jsp'
req = requests.get(url_with_pdfs).content
soup = BeautifulSoup(req)
urls = []
for i in soup.find_all('a'):
        if 'factsheets' in str(i['href']) and 'muni' in str(i['href']):
                urls.append('http://us.pioneerinvestments.com'+i['href'].split(';')[0])

########################## PDF parser ##########################################
def find(string, where):
        pattern = re.compile(string)
        result = re.findall(pattern,where)
        if result == []:
                result = '-'
        return result

def get_info(url):
        file_name = 'Pioneer_pdf.pdf'
        pdf = requests.get(url).content
        output = open(file_name, 'wb')
        output.write(pdf)
        output.close()
        
        pdf_file = open(file_name,'rb')
        PDF = PyPDF2.PdfFileReader(pdf_file)
        if PDF.isEncrypted:
                PDF.decrypt("")
        
        data =PDF.getPage(0).extractText()+ PDF.getPage(1).extractText() 
        first = []
        first.append(find('Symbol(.+?)Total',data)[0])
        first.append(find('Dividends',data)[0])
        first.append(find('EPS',data)[0])
        first.append(find('UNII',data)[0])
        first.append(find('Duration(.+?) Years',data)[0]) # NOT LEVERAGED
        first.append(find('IL',data)[0] + '%')
        first.append(find('PR',data)[0] + '%')
        first.append(find('NJ',data)[0] + '%')
        first.append(find('TOB',data)[0] + '%')
        first.append(find('Prerefunded(.+?)%',data)[0] + '%')
        first.append(find('General Obligation(.+?)%',data)[0] + '%')
        first.append(find('Health(.+?)%',data)[0] + '%')
        first.append(find('Housing(.+?)%',data)[0] + '%')
        first.append(find('Under 5 Years(.+?)%',data)[0].strip("%") + '% (5 yrs)')
        first.append(find('0 to 2 Years(.+?)%',data)[0] + '% (2 yrs)')      
        first.append(find('Net Asset Value(.+?)Mar',data)[0].strip("$"))
        first.append(find('Market Price (.+?)Premium/',data)[0].strip("$"))
        first.append(find('Premium/Discount(.+?)%Inception',data)[0])        

        
        
        pdf_file.close()
        os.remove('Pioneer_pdf.pdf')
        return first


       
pioneer = []     
for i in urls:
        pioneer.append(get_info(i))

###################### Missing Data with CEF Connect ###########################
url_cef = 'http://www.cefconnect.com/Details/Summary.aspx?Ticker='

for fund in range(len(pioneer)):
        url_ind = url_cef + pioneer[fund][0]
        name = pioneer[fund][0]
        req = requests.get(url_ind).content
        soup = BeautifulSoup(req)
        data = soup('table',{'id':'ContentPlaceHolder1_cph_main_cph_main_ucFundBasics_dvFB2'})[0]
        tds =data.findAll('td')
        ###### Average Volume ##############33
        for i in range(len(tds)):
                if tds[i].text == "Average Daily Volume (shares):":
                        pioneer[fund].append(tds[i+1].text)
        ###### EPS and UNII#############
        data_eps = soup('table',{'id':'ContentPlaceHolder1_cph_main_cph_main_ucDistributions_dvFB1'})[0]
        eps =data_eps.find_all('span',{'id':'ContentPlaceHolder1_cph_main_cph_main_ucDistributions_dvFB1_lblLatestEPS'})  
        pioneer[fund][2]=eps[0].text.strip("$")
        unii =data_eps.find_all('span',{'id':'ContentPlaceHolder1_cph_main_cph_main_ucDistributions_dvFB1_lblUNII'})  
        pioneer[fund][3] = unii[0].text.strip("$")
        
        ###### Dividend ############
        
        data_div = soup('table',{'id':'ContentPlaceHolder1_cph_main_cph_main_DistrDetails'})[0]
        tds1 = data_div.find_all('td')
        for i in range(len(tds1)):
                if tds1[i].text == 'Distribution Amount':
                        pioneer[fund][1]=tds1[i+1].text.strip("$")

stopTime7 = timeit.default_timer()
pinTime = 'Pioneer: ' + str(stopTime7 - stopTime6)
print('Pioneer Complete')
#################################################################################
############################## All Data Compilation #############################
#################################################################################

all_data = []

#BlackRock:
for x in range(len(tickers)):
    index = []
    index.append(tickers[x])
    index.append(div2[x])
    index.append(eps2[x])
    index.append(unii2[x])
    index.append(duration3[x])
    index.append(ill2[x])
    index.append(pr2[x])
    index.append(nj2[x])
    index.append(tob2[x])
    index.append(prere2[x])
    index.append(go2[x])
    index.append(health2[x])
    index.append(house2[x])
    index.append('-')
    index.append(maturity3[x])
    index.append(nav[x])
    index.append(price2[x])
    index.append(pd[x])
    index.append(ave_vol[x])
    
    all_data.append(index)

#Eaton Vance:    
for x in range(len(ev_ticker)):
    index = []
    index.append(ev_ticker[x])
    index.append(ev_div[x])
    index.append(ev_eps[x])
    index.append(ev_unii[x])
    index.append(ev_duration[x])
    index.append('-')
    index.append('-')
    index.append('-')
    index.append('-')
    index.append(ev_ec[x])
    index.append(ev_go[x])
    index.append(ev_hc[x])
    index.append(ev_ho[x])
    index.append(ev_callmat[x])
    index.append('-')
    index.append(ev_nav[x])
    index.append(ev_price[x])
    index.append(ev_pd[x])
    index.append('-')
    
    all_data.append(index)

#Pimco    
for x in mergedResult2:
    all_data.append(x)

#Investco!:
for x in combined_data:
    all_data.append(x)
  
#Nuveen:
for x in final2:
    all_data.append(x)
  
#Pioneer:
for x in pioneer:
    all_data.append(x)
################################################################################
################################### Timer ######################################
################################################################################
 
stopTime = timeit.default_timer()
tTime = 'Total Time: ' + str(stopTime - startTime)
print('Data Nitro Completed')

#################################################################################
################################# Data Nitro ####################################
#################################################################################

#Fund, Current Dividend, Earnings per share, UNII per share, Average Duration, Illinois, Puerto Rico, NJ, Tobacco, Prere, GenObl, Health, NAV, Closing Price, Premium Discount, Average Daily Volume
Cell('A1').horizontal = ['Fund'] + ['Div'] + ['EPS'] + ['UNII'] + ['Duration'] + ['IL'] + ['PR'] + ['NJ'] + ['Tob'] + ['Pre-re'] + ['GO'] + ['Health'] + ['House'] + ['Near Call %'] + ['Near Mat %'] + ['NAV'] + ['Price'] + ['Disc'] + ['Ave Vol'] 
Cell('A2').table = all_data

CellRange('A1:S1').font.bold = True

autofit()

#Timers:
Cell('U2').value = tTime
Cell('U3').value = brTime
Cell('U4').value = evTime
Cell('U5').value = pcTime
Cell('U6').value = icTime
Cell('U7').value = nuvTime
Cell('U8').value = pinTime