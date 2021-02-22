from pyspark import SparkContext

sc = SparkContext(appName="Demo Application")


def getInputRdd():
    return sc.textFile("SampleFile.txt")


def filterWordsWithI():
    print("Fitering words with I ")
    inputRDD = getInputRdd()
    iRDD = inputRDD.filter(lambda x: "i" in x)
    l = iRDD.collect()
    print(l)

def filterWordsWithE():
    print("Fitering words with E ")
    inputRDD = getInputRdd()
    eRDD = inputRDD.filter(lambda x: "e" in x)
    l = eRDD.collect()
    print(l)

def filterWordsWithIandE():
    inputRDD = getInputRdd()
    iRDD = inputRDD.filter(lambda x: "i" in x)
    eRDD = inputRDD.filter(lambda x: "e" in x)
    ieRDD=eRDD.union(iRDD).distinct()
    l = ieRDD.collect()
    print(l)


if __name__ == '__main__':
    filterWordsWithI()
    filterWordsWithE()
    filterWordsWithIandE()

