import yaml
import os

class PersistentList:

    def __init__(self, path):
        self.path = path
        if os.path.exists(path):
            with open(self.path) as ymlfile:
                self.savedList = yaml.load(ymlfile)
        else:
            self.savedList = []

    def save(self):
        pass
            
    def __getitem__(self, key):
        return self.savedList[key]

    def __len__(self):
        return len(self.savedList)

    def __setitem__(self, key, val):
        self.savedList[key] = val
        self.save()
        
    def append(self, val):
        self.savedList.append(val)
        self.save()

    def __delitem__(self, key):
        del self.savedList[key]
        self.save()

    def __contains__(self, key):
        return key in self.savedList
