import requests
from bs4 import BeautifulSoup
import json

class I3:
    def __init__(self):
        self.dividend_url = 'https://klse.i3investor.com/web/entitlement/dividend/latest'
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 OPR/92.0.0.0'}
        self.status_code = ''
        self.dividend_data = ''

    def extract_data(self, text, start, end):
        return text[text.index(start)+len(start):text.index(end)]

    

    def get_dividend(self):
        start_string = 'var dtdata = '
        end_string = ';'

        r = requests.get(self.dividend_url, headers=self.headers)
        self.status_code = r.status_code
        if r.status_code != 200:
            raise ValueError(f'HTTP return { r.status_code }')
        soup = BeautifulSoup(r.text)
        
        div = soup.find('div', class_='col-xl-8 me-2 mb-2 flex-grow-1')
        if str(div) == 'None':
            raise ValueError('division not found')
        scripts = BeautifulSoup(str(div)).findAll('script')
        for script in scripts:
            script = str(script)
            if start_string in script:
                data = self.extract_data(script, start_string, end_string)
                if data == '':
                    raise ValueError('Dividend data not found')
                try:
                    self.dividend_data = json.loads(data)
                except ValueError as e:
                    e('Not able to convert dividend data into JSON')