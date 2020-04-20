# Python 3.8.2


import pandas as pd
import os
import json

class JsonParser():
    def __init__(self, file_src):
        self.file_src = file_src
        self.data = self.getDataFrame(file_src)
        

    @property
    def listOfjson(self):
        filelist = []
        for root, dirs, files in os.walk(self.file_src):
            for i in files:
                if i.endswith("json") and i.startswith('message'):
                    filelist.append(i)
        return filelist

    def getDataFrame(self, file_src):
        out = []

        for y in self.listOfjson:
            # Fileslist = názvy jsonu které jsou 
            json_location = file_src +'\\' +y 
            # creating the entire path 
            # example: G:/jan-novak/message_1.json

            with open(json_location) as soubor:
                data = json.load(soubor)['messages']
                # In json there are other things besides the news
                #   see jsons themselves
                for i in range(len(data)):
                    # It is needed to go through all the data to set the correct encoding
                    if not "content" in data[i]:
                        continue
                        # In addition to messages, the date list also includes pictures, stickers, etc.
                        # There is no need to change encodings for the picture, etc.
                    data[i]["content"] = data[i]["content"].encode('latin-1').decode('utf8')
                    # content is messagess
                    data[i]["sender_name"] = data[i]["sender_name"].encode('latin-1').decode('utf8')            
            
            out = out + data
            #  merge data obtained from each list

        items = pd.DataFrame().from_dict(out)
        # conversion to the DataFrame data type

        items['timestamp_ms'] = pd.to_datetime(items['timestamp_ms'],unit='ms')
        # convert Unix time to "normal time"
        return items
        
zpravy = JsonParser('G:\source\messages\inbox\AntoninJarolim_BxELcrnTNQ')
print(zpravy.data)