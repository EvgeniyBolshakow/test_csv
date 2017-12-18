#Функция вычисления среднего значения
def mean(X):
    c = len(X)
    if c == 0:
        return False
    return sum(X)/c

#Функция вычисления среднеквадратичного отклонения
def sko(X):
    f1 = mean(X)
    XX = [x*x for x in X]
    f2 = mean(XX)
    D = f2 - f1*f1
    return pow(D, 0.5)



if __name__ == '__main__':
    
    import pymysql
        
    #============================================
    methodsList = []
    apiList = []
    
    #======================================
    conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='', db='metrics_db')
            
    try:
        with conn.cursor() as cur:              
            #---- Выборка числа 15-минутных интервалов -----
            sql = "SELECT COUNT(DISTINCT timeframe_start) FROM metrics"          
            cur.execute(sql)
            result = cur.fetchone()
            intervals_count = result[0]
            print(intervals_count)
            
            #---- Выборка всех вариантов http_method с ненулевыми значениями count_http_code_5xx -----
            sql = "SELECT DISTINCT http_method FROM metrics WHERE count_http_code_5xx > 0"   
                   
            cur.execute(sql)
            result = cur.fetchall()
            for row in result:
                methodsList.append(row[0])
            
            #---- Выборка всех вариантов api_name с ненулевыми значениями count_http_code_5xx -----
            sql = "SELECT DISTINCT api_name FROM metrics WHERE count_http_code_5xx > 0"          
            cur.execute(sql)
            result = cur.fetchall()
            for row in result:
                apiList.append(row[0])

    
            for method in methodsList:
                for api_name in apiList:            
                    X = []
                    #---- Выборка значений попарно (api_name,http_method) -----  
                    sql = "SELECT  count_http_code_5xx, timeframe_start  FROM metrics \
                                  WHERE api_name = %s \
                                  AND http_method = %s"  
                    cur.execute(sql, (api_name,method))
                    result = cur.fetchall()
                    for row in result:
                        X.append(row[0])
                    if len(X) > 0:
                        #print(api_name,method)
                        #print(X)
                        #print(len(X))
                        f1 = mean(X)
                        sigma = sko(X)
                        #print('f1 =',f1)
                        #print('sigma =',sigma)
                        a = f1 - 3*sigma
                        b = f1 + 3*sigma
                        #print(a,' : ',b)
                        
                        for x in X:
                            if x < a or x > b:
                                print(api_name,method,x)
                                sql = "SELECT id_metrics FROM metrics \
                                       WHERE api_name = %s \
                                       AND http_method = %s \
                                       AND count_http_code_5xx = %s"  
                                cur.execute(sql, (api_name,method,x))
                                result = cur.fetchone()
                                id_metrics = int(result[0])
                                
                                sql = "UPDATE metrics \
                                       SET is_anomaly = 1 \
                                       WHERE id_metrics = %s" 
                                cur.execute(sql, (id_metrics,))               
    finally:
        conn.close()
