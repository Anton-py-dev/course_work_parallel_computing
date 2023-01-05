import multiprocessing as mp
from pathlib import Path
import os
import re
import time


class InvertedIndex:
    def __init__(self):
        self.index = dict()

    def createIndex(self, path='data', threads_num=4):
        processes = []
        pathList = list(Path(path).glob('**/*.txt')) # Рекурсивно проходимо по всіх текстових файлах і робимо з них список
        fileNum = len(pathList)
        oneProcessNum = fileNum / threads_num # Розраховуємо скільки файлів має обробити один процес
        # self.oneProcessTask(pathList)
        for i in range(threads_num):
            startIndex = int(i * oneProcessNum)
            endIndex = int((i + 1) * oneProcessNum)
            currLi = pathList[startIndex:endIndex]

            p = mp.Process(target=self.oneProcessTask, args=(currLi, return_li)) # Даємо завдання кожному процесу
            processes.append(p)

        [x.start() for x in processes]
        [x.join() for x in processes]

    @staticmethod
    def oneProcessTask(listOfDoc):
        #print(f'Start: {list[0]}, end: {list[-1]}') # temp
        tempDict = dict()
        for name in listOfDoc:
            with open(name) as f:
                text = f.read()
                li = re.findall(r'\b\w+\b', text)
                for w in li:
                    if tempDict.get(w) is None:
                        tempDict[w] = set()
                    tempDict[w].add(str(name))

    def getListOfDoc(self, keyWord):
        return self.index[keyWord]


if __name__ == '__main__':
    ii = InvertedIndex()
    start_time = time.time()
    ii.createIndex()
    print("--- %s seconds ---" % (time.time() - start_time))
    li = ii.getListOfDoc('rated')
    print(li)