import yaml
import os

class PersistentDict:

    def __init__(self, path):
        self.path = path
        if os.path.exists(path):
            with open(self.path) as ymlfile:
                self.savedDict = yaml.load(ymlfile)
        else:
            self.savedDict = {}

    def save(self):
        with open(self.path, 'w') as f:
            f.write( yaml.dump( self.savedDict))
        
            
    def __getitem__(self, key):
        return self.savedDict[key]

    def __len__(self):
        return len(self.savedDict)

    def __setitem__(self, key, val):
        self.savedDict[key] = val
        self.save()
        
    def append(self, val):
        self.savedDict.append(val)
        self.save()

    def __delitem__(self, key):
        del self.savedDict[key]
        self.save()

    def __contains__(self, key):
        return key in self.savedDict

    def keys(self):
        return self.savedDict.keys()

    def items(self):
        return self.savedDict.items()
