# -*- coding: utf-8 -*-
'''
Created on 13 дек. 2017 г.

@author: Администратор
'''


if __name__ == '__main__':
    
    import os, csv, sys
    import datetime
    import pymysql
    
    filepath = 'raw_data.csv'
    
    if os.path.isfile(filepath) == False:
        print('Ошибка чтения файла с данными')
        sys.exit()
    
    dataList = list() #Список кортежей с данными CSV-файла
    
    #===== Чтение данных из файла и сохранение в список кортежей =====
    n = sum(1 for line in open(filepath))
    print('Общее число строк данных:', n)
    
    with open(filepath, newline='') as csvfile:
        print('Чтение данных CSV-файла ...')
        dataReader = csv.reader(csvfile, delimiter=',', quotechar='|')
        isFirstRow = True
        
        for row in dataReader:       
            if isFirstRow == True:
                isFirstRow = False
                continue
            else:                
                dt = str(row[0]) + ',' + str(row[1])          
                dt_obj = datetime.datetime.strptime(dt, '"%Y-%m-%d %H:%M:%S,%f"')
                
                dataList.append((dt_obj,row[2],row[3],row[4]))
    
    #==== Сортировка списка по первому элементу кортежа - по объекту datetime ======            
    print('Сортировка данных по времени ...')
    dataList.sort(key=lambda tup: tup[0])
    #--------------------------------------------------


    time_inteval = datetime.timedelta(minutes=15) 

    startCurrInterval = dataList[0][0]
    endCurrInterval = startCurrInterval + time_inteval
    intervalDict = dict() #словарь метрик (ключ - начало интервала, значение - словарь результатов обработки (ключ - кортеж (api,method), значение - число ответов с 5xx-кодом))
    pairDict = dict() #словарь результатов обработки - метрика для каждого интервала (15-минут)
    
    
    print('Формирование метрики ...')
    for dataElement in dataList:
        currDt = dataElement[0]
        if currDt < endCurrInterval: #если текущее время меньше времени окончания текущего 15-минутного интервала, обрабатываем данные
            key = tuple([dataElement[1],dataElement[2]])
            
            if key in pairDict:
                if int(dataElement[3]) >= 500:
                    pairDict[key] = pairDict.get(key) + 1
            else:
                if int(dataElement[3]) >= 500:
                    pairDict[key] = 1
                else:
                    pairDict[key] = 0
        else: #сохраняем метрику по текущему интервалу и переходим в следующий интервал          
            intervalDict[startCurrInterval] = dict(pairDict)
            startCurrInterval = endCurrInterval
            endCurrInterval = startCurrInterval + time_inteval
        #-- Для последнего интервала сохраняем данные, так как текущее время может не превысить время окончания 15-минутного интервала -- 
        intervalDict[startCurrInterval] = dict(pairDict)
    
    #====== Запись в БД ======
    conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='', db='metrics_db')
        
    try:
        with conn.cursor() as cur:
            cur.execute("truncate table metrics")
            conn.commit()
            print('Таблица metrics очищена ...')
            print('Запись метрики в БД ...')
    
            ex_count = 0
            kkk = list(intervalDict.keys())
            kkk.sort()
            for key in kkk:
                timeframe_start = key
                
                d = intervalDict[key]
                for k in d.keys():
                    sql = "INSERT INTO metrics (timeframe_start, api_name, http_method, count_http_code_5xx, is_anomaly) VALUES (%s, %s, %s, %s, %s)"
                    v = (timeframe_start, str(k[0]), str(k[1]), d[k], 0)
                    cur.execute(sql, v) 
                    ex_count = ex_count + 1  
    finally:
        conn.commit()
        conn.close()
        print('Запись данных в БД окончена (выполнено', ex_count,'записей)')
