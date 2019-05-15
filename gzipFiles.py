import gzip
import re
from os import listdir
import pandas as pd
import gc

source1 = '\\\\vesta.ru\\mfs\\SPECIAL\\common\\vzr_logs\\6site\\'
source2 = '\\\\vesta.ru\\mfs\\SPECIAL\\common\\vzr_logs\\6site2\\'
source3 = '\\\\vesta.ru\\mfs\\SPECIAL\\common\\vzr_logs\\6site3\\'

sources = [source1, source2, source3]

date = '2019-05-10'
number_of_days_to_scan = 10


def parseLogs(date, sources):
    def getChunks(source):
        chunks = []
        chunk = []

        with gzip.open(source + 'ws-vzrsaving-relaunch.log.' + date + '.gz', 'rb') as f:
            for line in f:
                chunk.append(line.decode('UTF-8'))
                if ('</ns2:saveDogResponse>' in line.decode('UTF-8')):
                    chunks.append(chunk.copy())
                    chunk.clear()

        return chunks

    def getSmallChunks(source):
        chunks = getChunks(source)

        contract_numbers = []
        calcIds = []
        contract_numbers_calcIds = {}

        for chunk in chunks:
            for string in chunk:
                if ('<contract_number>' in string):
                    contract_numbers.append(re.search('<contract_number>(.*)</contract_number>', string).group(1))
                if ('<calc_id>' in string):
                    calcIds.append(re.search('<calc_id>(.*)</calc_id>', string).group(1))

        for contract_number, calc_id in zip(contract_numbers, calcIds):
            contract_numbers_calcIds[contract_number] = calc_id[len(calc_id) - len(
                '36626151405F2D128CDC6771EFB8009DDA0B60764C0157'):]

        df = pd.DataFrame(contract_numbers_calcIds.items(),
                          columns=['contract_number', 'calc_id'])

        return df

    def getBigChunks(source):
        serieses = []
        names = ['date', 'INFOTYPE', 'ReqResp', 'code', 'redhat', 'number', 'xmlRequest']

        with gzip.open(source + 'ws-vzr-calc.log.' + date + '.gz', 'rb') as f:
            for line in f:
                serieses.append(line.decode('utf-8'))

        df = pd.DataFrame(columns=names,
                          data=[row.split('|') for row in serieses])
        return df

    def parseRequest(inputData):

        calcIds = []
        automaticalAdded = []
        recommended = []

        for index, value in inputData.iteritems():
            if (re.search('calcId=(.*),totalPremium=', str(value)) is not None
                    and re.search('name=ПАКЕТ РИСКОВ НС ДЛЯ ВЗР', str(value)) is not None):
                calcIds.append(re.search('calcId=(.*),totalPremium=',
                                         str(value)).group(1)[:len('A1D374FBF5A87541BC25202DDAA90A65A079BC96')])
                subStr = str(value).split('name=ПАКЕТ РИСКОВ НС ДЛЯ ВЗР', 1)[1]
                automaticalAdded.append(subStr[subStr.find('automaticalAdded=') + len('automaticalAdded='):
                                               subStr.find(',recomended=')])
                recommended.append(subStr[subStr.find('recomended=') + len('recomended='): subStr.find(',limitEntry=')])

        df = pd.DataFrame({
            'calcIds': calcIds,
            'automaticalAdded': automaticalAdded,
            'recommended': recommended
        })
        return df

    smallChunks = pd.DataFrame()
    bigChunks = pd.DataFrame()
    bigTable = pd.DataFrame()

    try:
        for source in sources:
            smallChunks = pd.concat([smallChunks, getSmallChunks(source)], ignore_index=True)
            bigChunks = pd.concat([bigChunks, getBigChunks(source)], ignore_index=True)
            bigTable = pd.concat([bigTable, parseRequest(bigChunks['xmlRequest'])], ignore_index=True)

            # bigChunks.info(memory_usage='deep')
            del bigChunks
            gc.collect()
            bigChunks = pd.DataFrame()

        smallChunks['calcIds'] = smallChunks['calc_id'] \
            .map(lambda x: x[len('3662417D1EF4FC0249F700D4246EDBF6E6FCEB27CF10A0') -
                             len('17C9E12E72ABE242C4CB5EC0FACDB6C7BDB039C2'):])

        result = pd.merge(smallChunks, bigTable, on='calcIds')
        result = result.drop('calcIds', 1)
        smallChunks = smallChunks.drop('calcIds', 1)

        # with pd.option_context('display.max_columns', 5):
        #     print(result)

        with pd.ExcelWriter(date + '.xlsx') as writer:
            result.to_excel(writer, sheet_name='Sheet_name_1')
            smallChunks.to_excel(writer, sheet_name='Sheet_name_2')
            bigTable.to_excel(writer, sheet_name='Sheet_name_3')
        print('Date ' + date + ' is OK')
    except MemoryError:
        print('Problems with date: ' + date)
        print('Memory Error')
    finally:
        del smallChunks
        del bigTable
        del bigChunks
        gc.collect()


dates = []

for file in sorted(listdir(source1)):
    if len(file) == len('ws-vzr-calc.log.2019-05-06.gz'):
        date = file[len('ws-vzr-calc.log.'): len(file) - len('.gz')]
        dates.append(date)


dates = dates[len(dates)-number_of_days_to_scan:]
for date in dates:
    parseLogs(date, sources)
