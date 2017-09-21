import yaml
import os

class PersistentSet:

    def __init__(self, path):
        self.path = path
        self.savedSet = set()
        if os.path.exists(path):
            with open(self.path) as ymlfile:
                savedList = yaml.load(ymlfile)
                for el in savedList:
                    self.savedSet.add(el)
                    
    def save(self):
        with open(self.path, 'w') as f:
            f.write( yaml.dump( [el for el in self.savedSet]))
            
    def __len__(self):
        return len(self.savedSet)

    def __setitem__(self, key, val):
        self.savedList[key] = val
        self.save()
        
    def add(self, val):
        self.savedSet.add(val)
        self.save()

    def __delitem__(self, key):
        assert False

    def __contains__(self, key):
        return key in self.savedSet

    def __iter__(self):
        return iter(self.savedSet)
