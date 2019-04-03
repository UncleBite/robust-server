# -*- coding: utf-8 -*-
"""
Created on Sat Mar 23 14:17:29 2019

@author: Xiaodi
"""

import socket
import xml.etree.ElementTree as ET
import psycopg2
import calendar
import time
from threading import Thread, Lock
from socketserver import ThreadingMixIn
import threading

mutex = Lock()
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
db_conn = psycopg2.connect(database="stock_exchange", user="postgres", password="postgres", host="db", port="5432")
print ("Opened database successfully")

#set up isolation level to serialized to gurantee thread safe
db_conn.set_isolation_level(3)
cur = db_conn.cursor()
conn = -1
cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('account',))
if cur.fetchone()[0] == False:
    cur.execute('''CREATE TABLE ACCOUNT
           (ID INT PRIMARY KEY  NOT NULL,
           BALANCE INT);''')
    cur.execute('''CREATE TYPE TYPES_ AS ENUM ('ORDER', 'CANCEL');''')
    cur.execute('''CREATE TYPE STATUS_ AS ENUM ('OPEN', 'EXECUTED','CANCELLED');''')
    cur.execute('''CREATE TABLE TRANSACTION (
                ACCOUNT_ID INT REFERENCES ACCOUNT(ID),
                SYMBOL VARCHAR(50) NOT NULL,
                AMOUNT INT,
                TIME BIGINT,
                PRICE_LIMIT INT,
                TRANS_ID SERIAL,
                TYPE TYPES_ NOT NULL,
                STATUS STATUS_ NOT NULL DEFAULT 'OPEN');''')
    db_conn.commit()


print ("Table created successfully")

HOST = '0.0.0.0'  # Standard loopback interface address (localhost)
PORT = 12345        # Port to listen on (non-privileged ports are > 1023)
PORT_OUT = 23456

threads = []



class ClientThread(Thread):
    def __init__(self,ip,port,conn):
        Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.conn = conn
    def run(self):
        mutex.acquire()
        receive(self.conn)
        mutex.release()







def send_to_client(msg,conn):
        print('sending')
        #s.sendall(b'<?xml version="1.0" encoding="UTF-8"?> <create><account id="22" balance="1000"/><account id="2" balance="500"/><symbol sym="AAA"><account id="2">100</account><account id="1">100</account><account id="1">100</account></symbol> </create>')
        conn.sendall(msg)
        #data = s.recv(1024)

def receive(conn):
        xml = ''
        data = conn.recv(1024)
        xml += str(data,'utf-8')
            #conn.sendall(data)
        xml = xml[xml.find('<'):]
        #test = '<?xml version="1.0"?><header/><body><code><body/>'
        root = ET.fromstring(xml)
        if root.tag == 'create':
            handle_create(root,conn)
        elif root.tag == 'transactions':
            handle_trans(root,conn)
            #process transaction

def handle_trans(root,conn):
    print("thread id is ==>"+ threading.current_thread().name)
    resp_root = ET.Element("results")
    act_id = root.get('id')
    id_exist = "SELECT * FROM ACCOUNT WHERE ID = {}".format(act_id)
    cur.execute(id_exist)
    res = cur.fetchall()
    if len(res) == 0:
        print('Account ID doesn\'t exist!')
        for child in root:
            if child.tag == 'order':
                ET.SubElement(resp_root, "error",sym=child.get('sym'),amount=child.get('amount'),limit= child.get('limit')).text = "ACCOUNT ID Doesn't Exist!"
            if child.tag == 'query':
                ET.SubElement(resp_root, "error",id=child.get('id')).text = "ACCOUNT ID Doesn't Exist!"
            if child.tag == 'cancel':
                ET.SubElement(resp_root, "error",id=child.get('id')).text = "ACCOUNT ID Doesn't Exist!"

    else:
        # check if act_id exist
        balance = "SELECT BALANCE FROM ACCOUNT WHERE ID = {}".format(act_id)
        cur.execute(balance)
        balance = int(cur.fetchone()[0])
        #fetching orders
        for child in root:
            if child.tag == 'order':
                symbol_name = child.get('sym')
                print('symbol == >'+symbol_name)
                amount = int(child.get('amount'))
                limit = int(child.get('limit'))
                symbol_exist = "select exists (select 1 from information_schema.columns where table_name ='account' and column_name='{}');".format(symbol_name.lower())
                cur.execute(symbol_exist)
                symbol_exist_res = cur.fetchall()[0][0]
                if symbol_exist_res== False:
                    print("symbol does not exist")
                    ET.SubElement(resp_root,"error",sym = symbol_name,amount=str(amount),limit=str(limit)).text="Symbol does not exist"
                else:

                    #buy
                    if amount > 0:
                        if amount*limit > balance:
                            print('error: insufficient balance')
                            ET.SubElement(resp_root, "error",sym=child.get('sym'),amount=child.get('amount'),limit= child.get('limit')).text = "insufficient balance"
                        else:
                            add_order = "INSERT INTO TRANSACTION (ACCOUNT_ID,SYMBOL, AMOUNT,PRICE_LIMIT,TYPE) VALUES ({},'{}',{},{},'ORDER');".format(act_id,symbol_name,amount,limit)
                            cur.execute(add_order)
                            db_conn.commit()
                            find_trans_id = "SELECT LAST_VALUE FROM TRANSACTION_TRANS_ID_SEQ"
                            cur.execute(find_trans_id);
                            last_trans_id = str(cur.fetchone()[0]);
                            stmt = "UPDATE ACCOUNT SET BALANCE = BALANCE-{} WHERE ID = {}".format(amount*limit,act_id)
                            cur.execute(stmt)
                            db_conn.commit()
                            ET.SubElement(resp_root, "order",sym=child.get('sym'),amount=child.get('amount'),limit= child.get('limit'), id=last_trans_id)
                            match('buy',limit,symbol_name,act_id,amount,last_trans_id)

                    #sell
                    elif amount < 0:
                        shares = "SELECT {} FROM ACCOUNT WHERE ID = {}".format(symbol_name,act_id)
                        cur.execute(shares)
                        shares = cur.fetchone()[0]
                        if -amount > shares :
                            print('error: insufficient shares')
                            ET.SubElement(resp_root, "error",sym=child.get('sym'),amount=child.get('amount'),limit= child.get('limit')).text = "Insufficient shares"

                        else:
                            add_order = "INSERT INTO TRANSACTION (ACCOUNT_ID,SYMBOL, AMOUNT,PRICE_LIMIT,TYPE) VALUES ({},'{}',{},{},'ORDER');".format(act_id,symbol_name,amount,limit)
                            cur.execute(add_order)
                            db_conn.commit()
                            find_trans_id = "SELECT LAST_VALUE FROM TRANSACTION_TRANS_ID_SEQ"
                            cur.execute(find_trans_id);
                            last_trans_id = str(cur.fetchone()[0]);
                            stmt = "UPDATE ACCOUNT SET {} = {}+{} WHERE ID = {}".format(symbol_name,symbol_name,int(amount),act_id)
                            cur.execute(stmt)
                            db_conn.commit()
                            ET.SubElement(resp_root, "order",sym=child.get('sym'),amount=child.get('amount'),limit= child.get('limit'),id=last_trans_id)
                            match('sell',limit,symbol_name,act_id,-amount,last_trans_id)

            #processing query
            if child.tag == 'query':
                query_id = child.get('id')
                query_stmt = "SELECT * FROM TRANSACTION WHERE TRANS_ID = {}".format(query_id)
                cur.execute(query_stmt)
                res = cur.fetchall()
                if len(res) == 0:
                    #transaction id dose not exist
                    print('Query: transaction id dose not exist')
                    ET.SubElement(resp_root,"error",id = query_id).text = "Query: Transaction id does not exist"
                else:
                    status_root = ET.SubElement(resp_root,"status",id = child.get('id'))
                    print('Query: transaction id dose exist')
                    for item in res:
                        status = item[7]
                        shares = str(item[2])
                        cur_time = str(item[3])
                        price = str(item[4])
                        if status == 'OPEN':
                            ET.SubElement(status_root,"open",shares = shares)
                        elif status == 'CANCELLED':
                            ET.SubElement(status_root,"canceled",shares=shares, time=cur_time)
                        elif status == 'EXECUTED':
                            ET.SubElement(status_root,"executed",shares=shares, price=price, time=cur_time)




            #processing cancel
            if child.tag == 'cancel':
                cancel_id = child.get('id')

                query_stmt = "SELECT * FROM TRANSACTION WHERE TRANS_ID = {} AND STATUS = 'OPEN'".format(cancel_id)
                cur.execute(query_stmt)
                res = cur.fetchall()
                if len(res) == 0:
                    print('Cancel: There is no matching transaction')
                    ET.SubElement(resp_root,"error",id=cancel_id).text = "Cancel: Transaction id does not exist"

                else:
                    cancel_root = ET.SubElement(resp_root,"canceled",id=cancel_id)
                    cancel_time = int(time.time())
                    change_status = "UPDATE TRANSACTION SET STATUS = 'CANCELLED', TIME = {} WHERE TRANS_ID = {} AND STATUS = 'OPEN'".format(cancel_time, cancel_id)
                    cur.execute(change_status)
                    db_conn.commit()

                    get_trans = "SELECT * FROM TRANSACTION WHERE TRANS_ID = {}".format(cancel_id)
                    cur.execute(get_trans)
                    trans_res = cur.fetchall()

                    for item in trans_res:
                        status = item[7]
                        shares = str(item[2])
                        cur_time = str(item[3])
                        price = str(item[4])
                        if status == 'CANCELLED':
                            ET.SubElement(cancel_root,"canceled",shares=shares, time=cur_time)
                        elif status == 'EXECUTED':
                            ET.SubElement(cancel_root,"executed",shares=shares, price=price, time=cur_time)

                    amount = res[0][2]
                    limit = res[0][4]
                    symbol_name = res[0][1]
                    #print(amount,limit,symbol_name)

                    if amount > 0:
                        stmt = "UPDATE ACCOUNT SET BALANCE = BALANCE+{} WHERE ID = {}".format(amount*limit,act_id)
                        cur.execute(stmt)
                        db_conn.commit()
                    #sell
                    else:
                        stmt = "UPDATE ACCOUNT SET {} = {} - '{}' WHERE ID = {}".format(symbol_name,symbol_name,int(amount),act_id)
                        cur.execute(stmt)
                        db_conn.commit()

    tree = ET.ElementTree(resp_root)
    send_to_client(ET.tostring(resp_root, encoding='utf8', method='xml'),conn)




def handle_create(root,conn):
    print("thread id is ==>"+ threading.current_thread().name)
    resp_root = ET.Element("results")
    for child in root:
        if child.tag == 'account':
            account_id = child.get('id')
            balance = child.get('balance')
            #print (account_id,balance)
            #if ID found in Database
            exist = "SELECT * FROM ACCOUNT WHERE ID = {};".format(account_id)
            cur.execute(exist)
            db_conn.commit()
            res = cur.fetchall()
            if len(res) != 0:
                ET.SubElement(resp_root, "error",id =account_id).text = "Account Already Exist!"
            #if ID not found in Database
            else:
                print('not found')
                doc = ET.SubElement(resp_root, "created",id=account_id)
                insert = "INSERT INTO account(ID,BALANCE) VALUES ({},{});".format(account_id, balance)
                cur.execute(insert)
                db_conn.commit()

        if child.tag =='symbol':
            symbol_name = child.get('sym')
            add_column = "ALTER TABLE ACCOUNT ADD COLUMN IF NOT EXISTS {} INT DEFAULT 0".format(symbol_name)

            cur.execute(add_column)
            for act in child.findall('account'):
                sym_act_exist = "SELECT * FROM ACCOUNT WHERE ID = {};".format(act.get('id'))
                cur.execute(sym_act_exist)
                db_conn.commit()
                res = cur.fetchall()
                #print(act.get('id'))
                #print(len(res))
                if len(res) != 0:
                #print(act.get('id'),act.text)
                    if act.text.isdigit() == True:
                        tmp = "UPDATE ACCOUNT SET {} = {}+{} WHERE ID = {}".format(symbol_name, symbol_name,int(act.text), act.get('id'))
                        ET.SubElement(resp_root, "created",sym=symbol_name, id =act.get('id') )
                        cur.execute(tmp)
                        db_conn.commit()
                    else:
                        ET.SubElement(resp_root, "error",sym=symbol_name, id =act.get('id')).text = "Symbol Creation Error!"
                else:
                    ET.SubElement(resp_root, "error",sym=symbol_name, id =act.get('id')).text = "Account Not Exists!"

    tree = ET.ElementTree(resp_root)
    send_to_client(ET.tostring(resp_root, encoding='utf8', method='xml'),conn)

        #    db.account_pool.insert_one({"ID":tmpAccount.getId()})
        #    print(db.account_pool.find({"ID":tmpAccount.getId()}))
        # if child.tag == 'account'
            #create account



def match(operation,limit,symbol,id,amount,last_trans_id):
    if operation == 'buy':
        print('buy')
        buy_stmt = "SELECT * FROM TRANSACTION WHERE price_limit < {} AND symbol = '{}' AND account_id != {} AND AMOUNT < 0 AND STATUS = 'OPEN' ORDER BY price_limit ASC, trans_id ASC".format(int(limit),symbol,int(id))
        cur.execute(buy_stmt)
        res = cur.fetchall()


        if len(res) != 0:
            #matching
            seller_amount = -int(res[0][2])
            print('seller_amount='+ str(seller_amount))
            print('found in buy')
            print(res[0][5])
            seller_trans_id = res[0][5]
            price = res[0][4]
            if seller_amount > amount:
                min_amount = amount
            else:
                min_amount = seller_amount
            execute(last_trans_id,seller_trans_id,min_amount,price)
        #    execute()




    elif operation == 'sell':
        print('sell')
        sell_stmt = "SELECT * FROM TRANSACTION WHERE price_limit > {} AND symbol = '{}' AND account_id != {} AND AMOUNT > 0 AND STATUS = 'OPEN' ORDER BY price_limit DESC, trans_id ASC".format(int(limit),symbol,int(id))
        cur.execute(sell_stmt)
        res = cur.fetchall()
        if len(res) != 0:
            #matching
            buyer_amount = res[0][2]
            print('buyer_amount='+ str(buyer_amount))
            print('found in sell')
            print(res[0][5])
            buyer_trans_id = res[0][5]
            price = res[0][4]
            if buyer_amount > amount:
                min_amount = amount
            else:
                min_amount = buyer_amount
            execute(buyer_trans_id,last_trans_id,min_amount,price)
            #execute()


def execute(buyer_trans_id,seller_trans_id,amount,price):
    print('in execute')
    buyer = "SELECT * FROM TRANSACTION WHERE STATUS = 'OPEN' AND TRANS_ID = {}".format(buyer_trans_id)
    cur.execute(buyer)
    res = cur.fetchone()
    buyer_id = int(res[0])
    buyer_amount = int(res[2])
    seller = "SELECT * FROM TRANSACTION WHERE STATUS = 'OPEN' AND TRANS_ID = {}".format(seller_trans_id)
    cur.execute(seller)
    res = cur.fetchone()
    seller_id = int(res[0])
    symbol = res[1]
    seller_amount = -int(res[2])
    print('in execute, seller_amount='+ str(seller_amount))
    #add symbol amount to buyer account
    update_buyer = "UPDATE ACCOUNT SET {} = {}+{} WHERE ID = {}".format(symbol, symbol, int(amount), buyer_id)
    cur.execute(update_buyer)
    #db_conn.commit()
    #add money to seller account
    update_seller = "UPDATE ACCOUNT SET BALANCE = BALANCE+{} WHERE ID = {}".format(amount*price,seller_id)
    cur.execute(update_seller)
    #db_conn.commit()
    max_amount = None
    max_id = None
    max_trans_id= None
    min_trans_id= None
    print(amount,buyer_amount,seller_amount)
    #update transaction status
    if amount == buyer_amount:
        print('amount=buyer_amount')
        max_amount = seller_amount
        max_id = seller_id
        min_trans_id = buyer_trans_id
        max_trans_id = seller_trans_id


    elif amount == seller_amount:
        print('amount=seller_amount')
        max_amount = buyer_amount
        max_id = buyer_id
        min_trans_id = seller_trans_id
        max_trans_id = buyer_trans_id
    exe_time = int(time.time())
    min_status = "UPDATE TRANSACTION SET STATUS = 'EXECUTED' , TIME = {} WHERE STATUS = 'OPEN' AND TRANS_ID = {}".format(exe_time,min_trans_id)
    cur.execute(min_status)
    if max_amount == amount:
        max_status = "UPDATE TRANSACTION SET STATUS = 'EXECUTED', TIME = {} WHERE STATUS = 'OPEN' AND TRANS_ID = {}".format(exe_time,max_trans_id)
        cur.execute(max_status)
    else:
        max_status = "UPDATE TRANSACTION SET AMOUNT = {} WHERE STATUS = 'OPEN' AND TRANS_ID = {}".format(max_amount-amount, max_trans_id)
        cur.execute(max_status)
        new_trans = "INSERT INTO TRANSACTION (ACCOUNT_ID, SYMBOL, AMOUNT, TIME, PRICE_LIMIT, TRANS_ID, TYPE,STATUS) VALUES ({},'{}',{},{},{},{},'ORDER','EXECUTED')".format(max_id,symbol,amount,exe_time,price,max_trans_id)
        cur.execute(new_trans)
    db_conn.commit()






def main():
    #init_db()

    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen(5)
    while True:

        print ("Multithreaded Python server : Waiting for connections from TCP clients...")
        (conn, (ip,port)) = s.accept()
        newthread = ClientThread(ip,port,conn)
        newthread.start()
        threads.append(newthread)
for t in threads:
    t.join()


if __name__ == "__main__":
    main()
