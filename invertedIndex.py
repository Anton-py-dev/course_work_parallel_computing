import multiprocessing as mp
from pathlib import Path
import re
import time


class InvertedIndex:
    def __init__(self, treads=4, path='data'):
        self.smallIndexes = None
        self.index = dict()
        self.threads = treads
        self.path = path

    def createIndex(self):
        pathList = list(Path(self.path).glob('**/*.txt')) # Рекурсивно проходимо по всіх текстових файлах і робимо з них список
        fileNum = len(pathList) # Рахуємо кількість файлів
        oneProcessNum = fileNum / self.threads # Розраховуємо скільки файлів має обробити один процес

        processes_args = []
        for i in range(self.threads): # Визначаємо які файли має обробити кожен з процесів
            startIndex = int(i * oneProcessNum)
            endIndex = int((i + 1) * oneProcessNum)
            processes_args.append((self.path, startIndex, endIndex))

        pool = mp.Pool(self.threads) # створюємо пул з необхідною к-стю порцесів
        self.smallIndexes = pool.starmap(self.oneProcessTask, processes_args)
        self.mergeIndex()

    @staticmethod
    def oneProcessTask(path, startIndex, endIndex):
        pathList = list(Path(path).glob('**/*.txt'))
        listOfDoc = pathList[startIndex:endIndex]
        tempDict = dict()
        for name in listOfDoc:
            with open(name, encoding='utf-8') as f:
                text = f.read()
                li = re.findall(r'\b\w+\b', text)
                for w in li:
                    if tempDict.get(w) is None:
                        tempDict[w] = set()
                    tempDict[w].add(str(name))
        return tempDict

    def getListOfDoc(self, keyWord):
        return self.index[keyWord]

    @staticmethod
    def printListOfDoc(list):
        i = 1
        for doc in list:
            print(f'{i}. \t{doc}')
            i += 1
    
    def mergeIndex(self):
        if len(self.smallIndexes) == 1:
            self.index = self.smallIndexes[0]
            return
        key = set()
        for d in self.smallIndexes:
            key |= set(d.keys())
        for k in key:
            self.index[k] = set()
            for d in self.smallIndexes:
                try:
                    self.index[k] |= d[k]
                except KeyError:
                    pass
        return