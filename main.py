import time
from invertedIndex import InvertedIndex


if __name__ == '__main__':
    ii = InvertedIndex(treads=4, path='data')
    start_time = time.time()
    ii.createIndex()
    print("--- %s seconds ---" % (time.time() - start_time))
    ii.printListOfDoc(ii.getListOfDoc(str(input("Word to find: "))))
