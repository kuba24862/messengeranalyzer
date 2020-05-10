# Python 3.8.2


import pandas as pd
import os
import json
from datetime import timedelta


class JsonParser():
    def __init__(self, files_src):
        self.file_src = files_src  
        self._data = pd.DataFrame()
        self._grpData = pd.DataFrame()     
    
    # setter function
    def set_all_Json(self):
        all_Json = []
        for file in os.listdir(self.file_src):
            if file.endswith(".json") and file.startswith('message_'):
                all_Json.append(file)
        return all_Json

    all_Json = property(set_all_Json, doc='Cesta k souborům které se budou analyzovat.')

    @property
    def data(self):
        if len(self._data) == 0:
            self.__get_data__()
        return self._data

    def __get_data__(self):
        # start_time = time.time()
        noparsetData = []
        parsetData = []
        for nameofjson in self.all_Json:
            json_directory = '{}\\{}'.format(self.file_src, nameofjson)
            with open(json_directory) as jsonFile:
                noparsetData = json.load(jsonFile)['messages']
                # print(noparsetData)
            parsetData = parsetData + noparsetData
        
        parsetData = pd.DataFrame().from_dict(parsetData)
        parsetData['sender_name'] = parsetData['sender_name'].str.encode('latin-1').str.decode('utf-8')
        parsetData['content'] = parsetData['content'].str.encode('latin-1').str.decode('utf-8')

        parsetData['timestamp_ms'] = pd.to_datetime(parsetData['timestamp_ms'],unit='ms')

        try:
            parsetData['call_duration'] = parsetData['call_duration'].fillna(0)
            parsetData['call_duration'] = pd.to_timedelta(parsetData['call_duration'], unit='s')
        except:
            pass
        print(parsetData)

        # 											

        # parsetData = parsetData[['content','photos','type','reactions','share','videos','audio_files','gifs','sticker','files','call_duration','missed']].isnull().fillna('')
        
        # parsetData['call_duration'] = parsetData[parsetData['call_duration'] != 'Nan']
        # elapsed_time = time.time() - start_time
        # print(elapsed_time)
        
        self._data = parsetData

    def toExcel(self,src):
        self.data.to_excel(src, engine='xlsxwriter')  

class CountFunction(JsonParser):
    
    @property
    def countedByuser(self):
        if len(self._grpData) == 0:
            self.__calcgrpData__()
        print(self.__grpData__)
        return self.__grpData__

    def __calcgrpData__(self,):
        '''Vypočítá počet kolikrát jsou jednotlivé prvky použity'''
        data = self.data.drop(['timestamp_ms','type'],axis=1)
        print(data)
        self.__grpData__ = data.groupby(by='sender_name',).agg('count')
        if 'call_duration' in data:
            self.__grpData__['call_duration'] = data[['call_duration','sender_name']].groupby(by='sender_name',).sum()


    def get_countedByuser(self, column:str=None , user:str=None):
            return self.countedByuser[column].loc(user)

    def set_counted2gether(self):
        data = self.countedByuser
        return data.sum()

    counted2gether = property(set_counted2gether, doc='')
        


    






