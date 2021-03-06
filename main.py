
import pandas as pd
import psycopg2
from pytrends.request import TrendReq
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
import os


def connect():
    """ Connect to the PostgreSQL database server """
    conn, cur = None, None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host="localhost", port="5432",
            database="GG_Trending",
            user="postgres",
            password="postgres")
        # create a cursor
        cur = conn.cursor()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while excuting SQL" + error)

    return conn, cur

def input_data(file_key,time_frame):
    conn,cur=connect()
    pytrends = TrendReq(hl='en-US', tz=420)
    key = pd.read_excel(file_key)
    columns_name = list(key.columns)
    x = 0
    for col in columns_name:
        keyw = key[col]
        keywords = keyw.dropna().values.tolist()
        for kw in keywords:
            pytrends.build_payload(
                kw_list=[kw],
                cat=0,
                timeframe=time_frame,
                geo='VN',
                gprop=''
            )
            data = pytrends.interest_over_time()
            if not data.empty:
                data = data.drop(labels=['isPartial'], axis='columns')

            dt=[d.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S') for d in data.index]
            vl= [v[0] for v in data.values]

            for l in range(len(data)):
                check="""select keyword from vn_trending
                         where keyword= '{}' and date='{}' and trend_type='{}'""" .format(str(kw).replace("'","''"),dt[l],col)

                cur.execute(check)
                row=cur.rowcount
                if row == 0:
                    query="""INSERT INTO vn_trending (keyword,date, value, trend_type) 
                            VALUES('{}','{}','{}','{}')""".format(str(kw).replace("'","''"),dt[l],vl[l],col)
                    cur.execute(query)
                    conn.commit()
                else:
                    x +=1
    if x!=0:
        print("C?? {} row ???? t???n t???i".format(x))
    conn.close()
    cur.close()

def build_query(year,limit):
    query = """
                    SELECT row_number() over (ORDER BY A.sum_val DESC, A.keyword, B.monthly) as STT , 
                            A.keyword, A.sum_val, B.monthly, B.max_val
                    FROM
                        (	SELECT keyword, sum(VALUE::INT) sum_val
                            FROM vn_trending
                            WHERE EXTRACT(YEAR FROM DATE) = %s
                            GROUP BY keyword
                            ORDER BY sum(VALUE::INT) DESC
                            LIMIT %s
                        ) A
                        JOIN 
                        (
                            SELECT DISTINCT A1.keyword, A2.monthly, A1.max_val
                            FROM
                            (	SELECT keyword, max(sum_val) max_val
                                FROM
                                (
                                    SELECT DISTINCT keyword, sum(VALUE::INT) sum_val, to_char(date::date,'mm-yyyy') monthly
                                    FROM vn_trending
                                    WHERE EXTRACT(YEAR FROM DATE) = %s
                                    GROUP BY keyword,to_char(date::date,'mm-yyyy') 
                                    ORDER BY keyword, sum(VALUE::INT) DESC
                                ) A3 
                                GROUP BY keyword
                            ) A1
                            JOIN
                            (		SELECT DISTINCT keyword, sum(VALUE::INT) sum_val, to_char(date::date,'mm-yyyy') monthly
                                    FROM vn_trending
                                    WHERE EXTRACT(YEAR FROM DATE) = %s
                                    GROUP BY keyword,to_char(date::date,'mm-yyyy') 
                                    ORDER BY keyword, sum(VALUE::INT) DESC
                            ) A2 ON A1.keyword = A2.keyword and A1.max_val = A2.sum_val
                        ) B ON A.keyword = B.keyword
                    ORDER BY A.sum_val DESC, A.keyword, B.monthly;
                    """ % (year, limit, year, year)
    return query

def menu_start():
    x=input("1. L???y d??? li???u trending t??? file\n"
            "2. Xu???t b??o c??o top 10 trending\n"
            "3. Xu???t b??o c??o search keyword in 2020\n"
            "4. V??? bi???u ????? line chart top 5 trending c??c t??? kh??a t??m ki???m nhi???u nh???t 2020\n"
            "5. V??? bi???u ????? bar chart top 5 trending c??c t??? kh??a t??m ki???m nhi???u nh???t 2019\n"
            "6. Th???ng k?? t??m ki???m top trending 5 t??? kh??a trong 2 n??m 2020, 2019\n"
            "...\n"
            "99. Tho??t\n"
            "\nM???i nh???p: ")
    return x

def validate(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
    except Exception as ex:
        print("Datetime kh??ng ????ng ?????nh d???ng")
        print(ex)

class option:
    def switch(self, x):
        method = getattr(self, 'option_'+x, lambda: 'L???a ch???n kh??ng ph?? h???p')
        return method()

    def option_1(self):
        check=False
        while check == False:
            a = input("File name: ")
            if not Path(a).is_file():
                print("File kh??ng t???n t???i, m???i nh???p l???i.")
                continue
            ext=os.path.splitext(a)[-1].lower()
            if ext != '.xlsx' and ext != '.xls':
                print("File kh??ng ????ng ?????nh d???ng")
                continue
            check=True
        td=datetime.today().strftime('%Y-%m-%d')
        temp = False
        while temp == False :
            b = input("From date (Y-M-D): ")
            c = input("To date (Y-M-D): ")
            try:
                datetime.strptime(b, '%Y-%m-%d')
                datetime.strptime(c, '%Y-%m-%d')
                temp = b<c and c<td
                if temp==False:
                    print("Datetime kh??ng h???p l??? m???i nh???p l???i")
            except Exception as ex:
                print("Datetime kh??ng ????ng ?????nh d???ng, m???i nh???p l???i")

        d = b +' '+ c
        input_data(a, d)
        exit_menu()

    def option_2(self):
        year=input("Nh???p n??m: ")
        limit=input("Nh???p limit: ")
        conn,cur=connect()
        query=build_query(year,limit)
        cur.execute(query)
        rf=cur.fetchall()
        conn.close()
        cur.close()
        df=pd.DataFrame(rf,columns=['STT','Keyword','S??? l???n t??m ki???m','Th??ng t??m ki???m nhi???u nh???t', 'max_val'])
        df=df.drop(['max_val'],axis=1)
        writer = pd.ExcelWriter('vn_trending_search_keyword_year.xlsx',engine='xlsxwriter')
        df.to_excel(writer,index=None,startrow=3,sheet_name='Sheet1')
        workbook = writer.book
        merge_format = workbook.add_format({
            'bold': 1,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter'})
        worksheet = writer.sheets['Sheet1']
        worksheet.merge_range('A1:D2','DANH S??CH T??? KH??A T??M KI???M T???I VI???T NAM',merge_format)
        worksheet.merge_range('A3:D3', 'N??m ' + year, merge_format)
        col_format=workbook.add_format({'align': 'center'})
        worksheet.set_column('B:D',23,col_format)
        writer.save()
        exit_menu()

    def option_3(self):

        year=input("Nh???p n??m: ")
        conn,cur = connect()
        query = """ select distinct trend_type from vn_trending """
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=['trend_type'])
        type_trend = [t[0] for t in df.values]

        writer = pd.ExcelWriter('vn_trending_search_keyword_month.xlsx',engine='xlsxwriter')
        for i in range(len(type_trend)):
            queryy = """select keyword, sum(value),Extract(Month from date) monthly
                            from vn_trending
                            WHERE trend_type= '%s' and extract(year from date)= %s
                            group by monthly, keyword
                            order by keyword,monthly""" % (type_trend[i],year)
            cur.execute(queryy)
            b = cur.fetchall()
            result = pd.DataFrame(b)
            result[2] = result[2].astype(int)
            result = pd.pivot_table(result, values=[1], columns=[2], index=[0])
            result.columns = result.columns.droplevel()
            result.columns = ['Th??ng ' + str(k) for k in result.columns]
            result.to_excel(writer, sheet_name=type_trend[i], index_label='Keyword',startrow=3)
            workbook = writer.book
            merge_format = workbook.add_format({
                'bold': 1,
                'border': 1,
                'align': 'center',
                'valign': 'vcenter'})
            worksheet = writer.sheets[type_trend[i]]
            worksheet.merge_range('A1:M2', 'T??? KH??A T??M KI???M NHI???U NH???T T???I VI???T NAM', merge_format)
            worksheet.merge_range('A3:M3', 'N??m ' + year, merge_format)
            worksheet.set_column('A:A', 23)
        writer.save()
        conn.close()
        cur.close()
        exit_menu()

    def option_4(self):
        conn, cur = connect()
        query = """
                    SELECT keyword, SUM(value) as sum_value
                    FROM vn_trending
                    WHERE EXTRACT(YEAR FROM date)=2020
                    GROUP BY keyword
                    ORDER BY SUM(value) DESC
                    LIMIT 5;
                    """
        cur.execute(query)
        rf = pd.DataFrame(cur.fetchall(), columns=['Keyword', 'Sum_value'])
        plt.figure(figsize=(9, 5))
        plt.plot(rf['Keyword'], rf['Sum_value'])
        plt.title('T??? KH??A T??M KI???M NHI???U NH???T T???I VI???T NAM \nN??M 2020')
        plt.grid(axis='y')
        plt.savefig('top_search_key_2020.png', bbox_inches='tight')
        print("???? l??u h??nh ???nh")
        exit_menu()

    def option_5(self):
        conn, cur = connect()
        query = """
                    SELECT keyword, SUM(value) as sum_value
                    FROM vn_trending
                    WHERE EXTRACT(YEAR FROM date)=2019
                    GROUP BY keyword
                    ORDER BY SUM(value) DESC
                    LIMIT 5;
                    """
        cur.execute(query)
        rf = pd.DataFrame(cur.fetchall(), columns=['Keyword', 'Sum_value'])
        plt.figure(figsize=(8, 5))
        plt.bar(rf['Keyword'], rf['Sum_value'])
        plt.title('T??? KH??A T??M KI???M NHI???U NH???T T???I VI???T NAM \nN??M 2019')
        plt.savefig('top_search_key_2019.png', bbox_inches='tight')
        print("???? l??u h??nh ???nh")
        exit_menu()

    def option_6(self):
        year = ['2019','2020']
        limit = input("Nh???p limit: ")
        conn, cur = connect()
        dataset=[]
        for i in range(0,len(year)):
            query = build_query(year[i], limit)
            cur.execute(query)
            rf = cur.fetchall()
            df = pd.DataFrame(rf, columns=['STT', 'Keyword', 'S??? l???n t??m ki???m', 'Th??ng t??m ki???m nhi???u nh???t', 'max_val'])
            if i==0:
                df = df.drop(['max_val'], axis=1)
            else:
                df = df.drop(['max_val','STT'], axis=1)
            dataset.append(df)
        rf=pd.concat(dataset,axis=1)
        writer = pd.ExcelWriter('vn_trending_search_keyword_2_year.xlsx',engine='xlsxwriter')
        rf.to_excel(writer,index=None,sheet_name='Sheet1',startrow=3)
        workbook = writer.book
        merge_format = workbook.add_format({
            'bold': 1,
            'border': 1,
            'align': 'center',
            'valign': 'vcenter'})
        worksheet = writer.sheets['Sheet1']
        worksheet.merge_range('A1:G2', 'TH???NG K?? T??M KI???M NHI???U NH???T TRONG 2 N??M T???I VI???T NAM', merge_format)
        worksheet.merge_range('A3:D3', 'N??m ' + year[0], merge_format)
        worksheet.merge_range('E3:G3', 'N??m ' + year[1], merge_format)
        col_format = workbook.add_format({'align': 'left'})
        worksheet.set_column('D:D', 23, col_format)
        worksheet.set_column('G:G', 23, col_format)
        worksheet.set_column('B:C', 18, col_format)
        worksheet.set_column('E:F', 18, col_format)
        writer.save()
        conn.close()
        cur.close()
        exit_menu()
    def option_99(self):
        exit()

def options():
    x = menu_start()
    while x not in ['1','2','3','4','5','6','99']:
        print("Kh??ng h???p l???, m???i nh???p l???i.")
        x=menu_start()
    opt=option()
    opt.switch(x)

def exit_menu():
    a=input("\n0. Tr??? l???i menu\n"
            "99. Tho??t\n")
    while a not in ['0','99']:
        print("Kh??ng h???p l???, m???i nh???p l???i.")
        a = input("\n0. Tr??? l???i menu\n"
                  "99. Tho??t\n")
    if a == '0':
        options()
    else:
        exit()


if __name__ == '__main__':
    options()


