import selenium
import time
import pandas as pd

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
driver = webdriver.Chrome(ChromeDriverManager().install())
browser = driver 

### we should grab these from The Graph
dao_starter = pd.read_csv('daohaus_daos.csv')
daof = pd.DataFrame()


def grab_dao_metadata(vari=None):
    # this is for me tho
    # nav to home
    browser.get('https://app.daohaus.club/explore')
    time.sleep(5)
    # deal with bullshit wallet connect popup
    browser.find_element_by_xpath("//*[@id='walletconnect-qrcode-modal']/div/div[2]/a[2]").click()
    browser.find_element_by_xpath("//*[@id='walletconnect-qrcode-modal']/div/div[1]/div").click()
    time.sleep(2)
    list_dicts = list()
    for index, row in dao_starter.iloc[1702:].iterrows():
        address = row['address']
        chain = row['network_suffix']
        url = "https://app.daohaus.club/dao/" + chain + '/' + address  + '/settings'
        print(url)
        try:
            browser.get(url)
            time.sleep(3)
            browser.find_element_by_xpath('//*[@id="walletconnect-qrcode-modal"]/div/div[2]/a[2]').click()
            time.sleep(3)
            browser.find_element_by_xpath('//*[@id="walletconnect-qrcode-close"]/div[2]').click()
            dao_dict = {}
            name = browser.find_element_by_xpath('//*[@id="root"]/div/div[3]/div[2]/div/div/div[4]/div[1]/div').text
            print(name)
            elems = browser.find_elements_by_xpath("//a[@href]")
            socials = [i.get_attribute('href') for i in elems]
            socials = [i for i in socials if not "daohaus" in i]
            socials = [i for i in socials if not "blockscout" in i]
            socials = [i for i in socials if not "bridge.walletconnect" in i]
            socials = [i for i in socials if not "tokenary" in i]
            socials = [i for i in socials if not "wallet3://wc?uri=wc%3Af24ae9cc" in i]
            socials = [i for i in socials if not "wallet.ambire" in i]
            socials = [i for i in socials if not "etherscan" in i]
            socials = [i for i in socials if not 'encrypted' in i]
            socials = [i for i in socials if not 'ledger' in i]
            socials = [i for i in socials if not "infinitywallet" in i]
            socials = [i for i in socials if not "nowdaoit" in i]
            dao_dict['contract'] = address
            dao_dict['identifiers'] = socials
            dao_dict['name'] = name
            dao_dict['twitter'] = [x for x in socials if "twitter" in x]
            dao_dict['forum'] = [x for x in socials if "forum" in x]
            dao_dict['discord'] = [x for x in socials if "discord" in x]
            dao_dict['blog'] = [x for x in socials if "medium" in x  or "blog" in x or "mirror" in x or "substack" in x]
            dao_dict['documentation'] = [x for x in socials if 'docs' in x or "gitbook" in x]
            dao_dict['opensea'] = [x for x in socials if "opensea" in x]
            dao_dict['gallery'] = [x for x in socials if "gallery" in x]
            dao_dict['github'] = [x for x in socials if "github" in x]
            list_dicts.append(dao_dict)
            daof = pd.DataFrame(list_dicts)
            daof.to_csv('daohaus_checkpoint_seventeen.csv')
        except Exception as e:
            print(e)
            pass

grab_dao_metadata()








