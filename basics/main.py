from pyspark import SparkContext

sc = SparkContext(appName="Demo Application")


def getInputRdd():
    return sc.textFile("SampleFile.txt")


def filterWordsWithI():
    print("Fitering words with I ")
    inputRDD = getInputRdd()
    iRDD = inputRDD.filter(lambda x: "i" in x)  # This is called Transformations: Transformations are operations on RDDs that return a new RDD. e.g filter,union
    l = iRDD.collect() # This is called Actions: Actions are the second type of RDD operation. They are the operations that return a final value to the driver program or write data to an external storage system.
    print(l)

def filterWordsWithE():
    print("Fitering words with E ")
    inputRDD = getInputRdd()
    eRDD = inputRDD.filter(lambda x: "e" in x) # Transformations
    l = eRDD.collect() # Actions
    print(l)

def filterWordsWithIandE():
    inputRDD = getInputRdd()
    iRDD = inputRDD.filter(lambda x: "i" in x) # Transformations
    eRDD = inputRDD.filter(lambda x: "e" in x) # Transformations
    ieRDD=eRDD.union(iRDD).distinct() # Transformations
    l = ieRDD.collect() # Actions
    print(l)

def getFirst2Words():
    inputRDD = getInputRdd()
    twoWordList=inputRDD.take(2)  # Action:
    print(twoWordList)

def ChangeAllWordsToUppercase():
    inputRDD = getInputRdd()
    allWords=inputRDD.map(lambda word: # Transformations
         word.upper()
    ).collect()  # Action:
    print(allWords)


def CreatePairOfLowerAndUppercase(): # Example of key/value pair transforms, there are many of them.
    inputRDD = getInputRdd()
    allWordsRdd=inputRDD.map(lambda word: # Transformations
                          (word,word.upper())
    )
    # Combined keys and values
    print(allWordsRdd.collect())

    # keys only - normal case
    print(allWordsRdd.keys().collect())

    # values only - upper case
    print(allWordsRdd.values().collect())

def groupByKeys(): # Example of key/value pair transforms, there are many of them.
    someRdd=sc.parallelize([1,2,3,4,5,6,2,3,4])
    squareMapped=someRdd.map(lambda x: (x,x*x))
    grouped=squareMapped.groupByKey().collect()
    for group in grouped:
        key = group[0]
        values = group[1].data
        print("For key {} the values are".format(key))
        for value in values:
            print("{} ".format(value))



if __name__ == '__main__':
    filterWordsWithI()
    filterWordsWithE()
    filterWordsWithIandE()
    getFirst2Words()
    ChangeAllWordsToUppercase()
    CreatePairOfLowerAndUppercase()
    groupByKeys()

